import pandas as pd
import dash
from dash import html, dcc, Input, Output, State, dash_table
from dash.dash_table.Format import Format, Scheme
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from pathlib import Path
from collections import OrderedDict

EXPENSE_UNIT_STATUS_OPTIONS = ['Onboarded', 'Offboarded', 'Sold']
ONGOING_ACTIVE_STATUSES = {'active', 'ongoing', 'in progress', 'in_progress'}
DEALER_BRAND_MAP = {
    'destination toyota burnaby': {
        'name': 'Destination Toyota Burnaby',
        'short': 'Destination Burnaby',
    },
    'westminster toyota': {
        'name': 'Westminster Toyota',
        'short': 'Westminster',
    },
    'openroad toyota richmond': {
        'name': 'OpenRoad Toyota Richmond',
        'short': 'OpenRoad Richmond',
    },
    'jim pattison toyota surrey': {
        'name': 'Jim Pattison Toyota Surrey',
        'short': 'Pattison Surrey',
    },
}



def _resolve_data_path(env_var_name, default_filename):
    base_dir = Path(__file__).resolve().parent
    configured = os.getenv(env_var_name)
    path = Path(configured) if configured else base_dir / default_filename
    return path


def _find_first_existing_column(dataframe, candidates):
    columns_map = {str(column).strip().lower(): column for column in dataframe.columns}
    for candidate in candidates:
        resolved = columns_map.get(candidate.lower())
        if resolved is not None:
            return resolved
    return None


def _normalize_dealer_name(value):
    if value is None or pd.isna(value):
        return None
    return ' '.join(str(value).strip().lower().split())


def _fallback_dealer_short_name(value):
    label = str(value).strip()
    if len(label) <= 20:
        return label
    words = label.split()
    if len(words) >= 2:
        compact = f"{words[0]} {words[-1]}"
        if len(compact) <= 20:
            return compact
    return f"{label[:17]}..."


def _get_dealer_brand(value):
    normalized = _normalize_dealer_name(value)
    if not normalized:
        return {'name': 'Unknown Dealer', 'short': 'Unknown'}
    brand = DEALER_BRAND_MAP.get(normalized)
    if brand:
        return brand
    label = str(value).strip()
    return {'name': label, 'short': _fallback_dealer_short_name(label)}


def _append_dealer_branding(dataframe, dealer_column, prefix='dealer'):
    frame = dataframe.copy()
    brands = frame[dealer_column].apply(_get_dealer_brand)
    frame[f'{prefix}_name'] = brands.apply(lambda item: item['name'])
    frame[f'{prefix}_short'] = brands.apply(lambda item: item['short'])
    return frame


def _prepare_rental_dataframe(raw_df, fleet_dataframe, now_ts=None):
    prepared_df = raw_df.copy()
    prepared_df = prepared_df[prepared_df['user_groups'] == "Rideshare Drivers"]

    pre_tax_charge_col = _find_first_existing_column(prepared_df, ['Pre-Tax Charge', 'pre_tax_charge', 'pre tax charge'])
    pre_tax_promo_col = _find_first_existing_column(prepared_df, ['pre_tax_promo', 'Pre-Tax Promo', 'pre tax promo'])
    total_to_charge_col = _find_first_existing_column(prepared_df, ['total_to_charge', 'Total to Charge', 'total to charge'])

    if pre_tax_charge_col is not None:
        prepared_df[pre_tax_charge_col] = pd.to_numeric(prepared_df[pre_tax_charge_col], errors='coerce')
    else:
        prepared_df['Pre-Tax Charge'] = 0.0
        pre_tax_charge_col = 'Pre-Tax Charge'

    prepared_df['pre_tax_charge_amount'] = pd.to_numeric(prepared_df[pre_tax_charge_col], errors='coerce')
    prepared_df['pre_tax_promo_amount'] = pd.to_numeric(prepared_df[pre_tax_promo_col], errors='coerce') if pre_tax_promo_col is not None else pd.NA
    prepared_df['legacy_total_to_charge_amount'] = pd.to_numeric(prepared_df[total_to_charge_col], errors='coerce') if total_to_charge_col is not None else pd.NA

    prepared_df['revenue_amount'] = prepared_df['pre_tax_charge_amount']
    if total_to_charge_col is not None:
        prepared_df['revenue_amount'] = prepared_df['revenue_amount'].fillna(prepared_df['legacy_total_to_charge_amount'])
        zero_legacy_charge_mask = prepared_df['legacy_total_to_charge_amount'].fillna(0).eq(0)
        promo_available_mask = prepared_df['pre_tax_promo_amount'].notna()
        prepared_df.loc[zero_legacy_charge_mask & promo_available_mask, 'revenue_amount'] = prepared_df.loc[
            zero_legacy_charge_mask & promo_available_mask, 'pre_tax_promo_amount'
        ]
    prepared_df['revenue_amount'] = pd.to_numeric(prepared_df['revenue_amount'], errors='coerce').fillna(0)

    for col in ['rental_started_at_EST', 'rental_end_datetime_EST']:
        prepared_df[col] = pd.to_datetime(prepared_df[col], errors='coerce')

    prepared_df = prepared_df.dropna(subset=['rental_started_at_EST'])

    status_column = _find_first_existing_column(prepared_df, ['reservation_status', 'reservation status'])
    if status_column:
        status_normalized = prepared_df[status_column].fillna('').astype(str).str.strip().str.lower()
    else:
        status_normalized = pd.Series('', index=prepared_df.index)

    is_invalid_status = status_normalized.str.contains('cancel|void|invalid|rejected|declin|no show', regex=True)
    if status_column:
        is_active_status = status_normalized.isin(ONGOING_ACTIVE_STATUSES)
    else:
        is_active_status = pd.Series(True, index=prepared_df.index)

    prepared_df['is_ongoing_rental'] = (
        prepared_df['rental_started_at_EST'].notna() &
        prepared_df['rental_end_datetime_EST'].isna() &
        is_active_status &
        (~is_invalid_status)
    )

    if now_ts is None:
        now_ts = pd.Timestamp.now()

    prepared_df['effective_rental_end_datetime'] = prepared_df['rental_end_datetime_EST']
    prepared_df.loc[
        prepared_df['effective_rental_end_datetime'].isna() & prepared_df['is_ongoing_rental'],
        'effective_rental_end_datetime'
    ] = now_ts

    duration_hours = (
        prepared_df['effective_rental_end_datetime'] - prepared_df['rental_started_at_EST']
    ).dt.total_seconds() / 3600
    prepared_df['rental_hours'] = duration_hours.clip(lower=0)
    prepared_df.loc[prepared_df['effective_rental_end_datetime'].isna(), 'rental_hours'] = pd.NA
    prepared_df['rental_days'] = prepared_df['rental_hours'] / 24
    prepared_df['rental_status'] = prepared_df['is_ongoing_rental'].map({True: 'Ongoing', False: 'Completed'})

    prepared_df['start_year'] = prepared_df['rental_started_at_EST'].dt.year
    prepared_df['start_month'] = prepared_df['rental_started_at_EST'].dt.month
    prepared_df['start_month_name'] = prepared_df['rental_started_at_EST'].dt.strftime('%B')
    prepared_df['year_month'] = prepared_df['rental_started_at_EST'].dt.strftime('%Y-%m')
    prepared_df['year_month_dt'] = pd.to_datetime(prepared_df['year_month'] + '-01', errors='coerce')
    prepared_df['start_day_of_week'] = prepared_df['rental_started_at_EST'].dt.strftime('%A')
    prepared_df['start_hour'] = prepared_df['rental_started_at_EST'].dt.hour

    prepared_df['license_plate_normalized'] = prepared_df['license_plate_number'].fillna('').str.strip().str.upper().str.replace('-', '', regex=False)
    local_fleet_df = fleet_dataframe.copy()
    local_fleet_df['plate_normalized'] = local_fleet_df['Plate Number'].fillna('').str.strip().str.upper().str.replace('-', '', regex=False)
    plate_to_vin_map = (
        local_fleet_df[['plate_normalized', 'VIN']]
        .dropna(subset=['plate_normalized'])
        .sort_values('VIN')
        .drop_duplicates(subset=['plate_normalized'], keep='first')
    )

    prepared_df = prepared_df.merge(
        plate_to_vin_map,
        left_on='license_plate_normalized',
        right_on='plate_normalized',
        how='left'
    )
    prepared_df['5VIN'] = prepared_df['VIN'].apply(
        lambda value: str(value)[-5:] if pd.notna(value) and len(str(value)) >= 5 else (str(value) if pd.notna(value) else None)
    )
    prepared_df = prepared_df.drop(columns=['license_plate_normalized', 'plate_normalized'])

    fleet_enrichment_lookup = local_fleet_df[['VIN', 'Status', 'Model', 'MY', 'Colour']].dropna(subset=['VIN']).drop_duplicates('VIN')
    prepared_df = prepared_df.merge(fleet_enrichment_lookup, on='VIN', how='left')

    prepared_df['ongoing_risk_bucket'] = 'None'
    prepared_df.loc[prepared_df['is_ongoing_rental'] & (prepared_df['rental_days'] > 7), 'ongoing_risk_bucket'] = '>7 days'
    prepared_df.loc[prepared_df['is_ongoing_rental'] & (prepared_df['rental_days'] > 14), 'ongoing_risk_bucket'] = '>14 days'
    prepared_df.loc[prepared_df['is_ongoing_rental'] & (prepared_df['rental_days'] > 30), 'ongoing_risk_bucket'] = '>30 days'

    # Use categorical dtype for high-repetition dimensions to speed filtering/groupby.
    category_cols = [
        'station_name', 'vehicle_type', 'renter_name', 'start_month_name',
        'year_month', 'rental_status', 'ongoing_risk_bucket', 'Status', 'Model', 'Colour'
    ]
    for category_col in category_cols:
        if category_col in prepared_df.columns:
            prepared_df[category_col] = prepared_df[category_col].astype('category')

    return prepared_df

# Load rental data
rental_file_path = _resolve_data_path('RENTAL_FILE_PATH', 'PastRentalDetails_2026-2-25.xlsx')
df = pd.read_excel(rental_file_path)

# Load fleet data
fleet_file_path = _resolve_data_path('FLEET_FILE_PATH', 'Kinto Fleet_3-19-26.xlsx')
fleet_df = pd.read_excel(fleet_file_path, sheet_name='data', header=0)

# Data cleaning and processing for rental data (includes ongoing rentals)
df = _prepare_rental_dataframe(df, fleet_df)

# Load and normalize invoice/expense data
invoice_file_path = _resolve_data_path('INVOICE_FILE_PATH', 'Invoices consolidated.xlsx')
inv_df = pd.read_excel(invoice_file_path, sheet_name='Invoices')
inv_df = inv_df.drop(columns=['Unnamed: 6'], errors='ignore')

# Parse numeric fields safely
for col in ['Labor', 'Misc', 'Parts', 'Less Insurance', 'Net for SAP', 'GST', 'PST', 'Total Tax', 'total']:
    inv_df[col] = pd.to_numeric(inv_df[col], errors='coerce').fillna(0)

# Parse date column
inv_df['Date of submission'] = pd.to_datetime(inv_df['Date of submission'], errors='coerce')

# Strip trailing spaces from Vehicle name
inv_df['Vehicle'] = inv_df['Vehicle'].str.strip()

# Normalize 5VIN: float64 → zero-padded 5-char string
inv_df['5VIN_key'] = inv_df['5VIN'].apply(
    lambda x: str(int(x)).zfill(5) if pd.notna(x) and str(x) != 'nan' else None
)

# Build vehicle lookup from rental dataset for enrichment (5VIN → VIN, vehicle_type, plate)
vehicle_lookup = (
    df[['5VIN', 'VIN', 'vehicle_type', 'license_plate_number']]
    .dropna(subset=['5VIN'])
    .drop_duplicates('5VIN')
    .rename(columns={'5VIN': '5VIN_key'})
)
inv_df = inv_df.merge(
    vehicle_lookup[['5VIN_key', 'VIN', 'vehicle_type', 'license_plate_number']],
    on='5VIN_key', how='left'
)

# Merge with fleet_df to get Model and MY (model year)
fleet_lookup = fleet_df[['VIN', 'Model', 'MY', 'Status']].dropna(subset=['VIN']).drop_duplicates('VIN')
inv_df = inv_df.merge(fleet_lookup, on='VIN', how='left')
inv_df['Status'] = inv_df['Status'].astype('string').fillna('Unknown').astype(str)

# Add year_month for time series
inv_df['year_month'] = inv_df['Date of submission'].dt.strftime('%Y-%m')
inv_df['year_month_dt'] = pd.to_datetime(inv_df['year_month'] + '-01', errors='coerce')
inv_df['sub_month_name'] = inv_df['Date of submission'].dt.strftime('%B')

for category_col in ['Dealer Name', 'Work Category', 'Vehicle', 'sub_month_name', 'Status', 'Model']:
    if category_col in inv_df.columns:
        inv_df[category_col] = inv_df[category_col].astype('category')

# Validation stats (used in layout options)
inv_total_rows = len(inv_df)
inv_matched = int(inv_df['VIN'].notna().sum())
inv_unmatched = inv_total_rows - inv_matched

# Precomputed options for expense time filters
inv_sub_years = sorted([int(y) for y in inv_df['Date of submission'].dt.year.dropna().unique()])
inv_sub_months = sorted(inv_df['sub_month_name'].dropna().unique(), key=lambda m: datetime.strptime(m, '%B').month)
inv_date_min = inv_df['Date of submission'].dropna().min().date()
inv_date_max = inv_df['Date of submission'].dropna().max().date()
fleet_status_values = sorted([str(s) for s in fleet_df['Status'].dropna().unique()])

# Global lookups and lightweight in-memory cache for expensive callback outputs
vehicle_mileage_lookup = pd.DataFrame(columns=['VIN', 'current_mileage'])
driver_first_rental_lookup = pd.DataFrame(columns=['customer_id', 'first_rental_date'])
UPDATE_ALL_CACHE_MAX = int(os.getenv('UPDATE_ALL_CACHE_MAX', '24'))
_UPDATE_ALL_CACHE = OrderedDict()


def _normalize_cache_key(value):
    if isinstance(value, dict):
        return tuple(sorted((str(k), _normalize_cache_key(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple, set)):
        return tuple(_normalize_cache_key(v) for v in value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, pd.Series):
        return tuple(_normalize_cache_key(v) for v in value.tolist())
    return value


def _cache_get(key):
    value = _UPDATE_ALL_CACHE.get(key)
    if value is not None:
        _UPDATE_ALL_CACHE.move_to_end(key)
    return value


def _cache_set(key, value):
    _UPDATE_ALL_CACHE[key] = value
    _UPDATE_ALL_CACHE.move_to_end(key)
    while len(_UPDATE_ALL_CACHE) > UPDATE_ALL_CACHE_MAX:
        _UPDATE_ALL_CACHE.popitem(last=False)


def _rebuild_reference_lookups():
    global vehicle_mileage_lookup, driver_first_rental_lookup

    mileage_history = df[['VIN', 'mileage_end']].copy() if {'VIN', 'mileage_end'}.issubset(df.columns) else pd.DataFrame(columns=['VIN', 'mileage_end'])
    if not mileage_history.empty:
        mileage_history['mileage_end'] = pd.to_numeric(mileage_history['mileage_end'], errors='coerce')
        vehicle_mileage_lookup = (
            mileage_history.dropna(subset=['VIN', 'mileage_end'])
            .groupby('VIN', as_index=False)['mileage_end']
            .max()
            .rename(columns={'mileage_end': 'current_mileage'})
        )
    else:
        vehicle_mileage_lookup = pd.DataFrame(columns=['VIN', 'current_mileage'])

    full_history_df = df[['customer_id', 'rental_started_at_EST', 'renter_name']].copy()
    if 'customer_id' in full_history_df.columns:
        full_history_df['customer_id'] = full_history_df['customer_id'].astype(str).str.strip()
        full_history_df.loc[full_history_df['customer_id'].isin(['', 'nan', 'None']), 'customer_id'] = pd.NA
    else:
        full_history_df['customer_id'] = pd.NA
    renter_fallback = full_history_df['renter_name'].astype('string').fillna('Unknown').astype(str)
    full_history_df['customer_id'] = full_history_df['customer_id'].fillna('RENTER:' + renter_fallback)

    driver_first_rental_lookup = (
        full_history_df
        .dropna(subset=['rental_started_at_EST'])
        .groupby('customer_id', as_index=False)['rental_started_at_EST']
        .min()
        .rename(columns={'rental_started_at_EST': 'first_rental_date'})
    )


_rebuild_reference_lookups()


def _reload_data():
    """Re-read all source Excel files and recompute all global dataframes."""
    global df, fleet_df, inv_df
    global inv_total_rows, inv_matched, inv_unmatched
    global inv_sub_years, inv_sub_months, inv_date_min, inv_date_max
    global fleet_status_values

    # --- Rental + Fleet ---
    _df = pd.read_excel(rental_file_path)
    _fleet_df = pd.read_excel(fleet_file_path, sheet_name='data', header=0)

    _df = _prepare_rental_dataframe(_df, _fleet_df)

    # --- Invoice / Expense ---
    _inv_df = pd.read_excel(invoice_file_path, sheet_name='Invoices')
    _inv_df = _inv_df.drop(columns=['Unnamed: 6'], errors='ignore')
    for col in ['Labor', 'Misc', 'Parts', 'Less Insurance', 'Net for SAP', 'GST', 'PST', 'Total Tax', 'total']:
        _inv_df[col] = pd.to_numeric(_inv_df[col], errors='coerce').fillna(0)
    _inv_df['Date of submission'] = pd.to_datetime(_inv_df['Date of submission'], errors='coerce')
    _inv_df['Vehicle'] = _inv_df['Vehicle'].str.strip()
    _inv_df['5VIN_key'] = _inv_df['5VIN'].apply(
        lambda x: str(int(x)).zfill(5) if pd.notna(x) and str(x) != 'nan' else None
    )
    _vehicle_lookup = (
        _df[['5VIN', 'VIN', 'vehicle_type', 'license_plate_number']]
        .dropna(subset=['5VIN'])
        .drop_duplicates('5VIN')
        .rename(columns={'5VIN': '5VIN_key'})
    )
    _inv_df = _inv_df.merge(_vehicle_lookup[['5VIN_key', 'VIN', 'vehicle_type', 'license_plate_number']], on='5VIN_key', how='left')
    _fleet_lookup = _fleet_df[['VIN', 'Model', 'MY', 'Status']].dropna(subset=['VIN']).drop_duplicates('VIN')
    _inv_df = _inv_df.merge(_fleet_lookup, on='VIN', how='left')
    _inv_df['Status'] = _inv_df['Status'].astype('string').fillna('Unknown').astype(str)
    _inv_df['year_month'] = _inv_df['Date of submission'].dt.strftime('%Y-%m')
    _inv_df['year_month_dt'] = pd.to_datetime(_inv_df['year_month'] + '-01', errors='coerce')
    _inv_df['sub_month_name'] = _inv_df['Date of submission'].dt.strftime('%B')

    for category_col in ['Dealer Name', 'Work Category', 'Vehicle', 'sub_month_name', 'Status', 'Model']:
        if category_col in _inv_df.columns:
            _inv_df[category_col] = _inv_df[category_col].astype('category')

    # Assign to globals
    df = _df
    fleet_df = _fleet_df
    inv_df = _inv_df
    inv_total_rows = len(inv_df)
    inv_matched = int(inv_df['VIN'].notna().sum())
    inv_unmatched = inv_total_rows - inv_matched
    inv_sub_years = sorted([int(y) for y in inv_df['Date of submission'].dt.year.dropna().unique()])
    inv_sub_months = sorted(inv_df['sub_month_name'].dropna().unique(), key=lambda m: datetime.strptime(m, '%B').month)
    inv_date_min = inv_df['Date of submission'].dropna().min().date()
    inv_date_max = inv_df['Date of submission'].dropna().max().date()
    fleet_status_values = sorted([str(s) for s in fleet_df['Status'].dropna().unique()])

    _rebuild_reference_lookups()
    _UPDATE_ALL_CACHE.clear()


# App
assets_css_path = Path(__file__).resolve().parent / 'assets' / 'custom.css'
cache_bust = int(assets_css_path.stat().st_mtime) if assets_css_path.exists() else 1

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], compress=True)
server = app.server
app.config.suppress_callback_exceptions = True
server.config['SEND_FILE_MAX_AGE_DEFAULT'] = int(os.getenv('STATIC_CACHE_SECONDS', '3600'))

# State store for tracking previous tab selections and reset state
app.layout_children_reference = None

# Layout
app.layout = dbc.Container([
    html.Link(rel='stylesheet', href=f'/assets/custom.css?v={cache_bust}'),
    # Hidden store to track state
    dcc.Store(id='app-state-store', data={'previous_tab': 'overview'}),
    dcc.Store(id='exp-drill-selection', data=None),
    dcc.Store(id='exp-stacked-trace-map', data=[]),
    dcc.Store(id='exp-vehicle-selection', data=None),
    dcc.Store(id='veh-selected-vehicle', data=None),
    dcc.Store(id='veh-selected-mileage-band', data=None),
    dcc.Store(id='data-refresh-counter', data=0),
    dcc.DatePickerRange(
        id='date_range',
        start_date=df['rental_started_at_EST'].min().date(),
        end_date=df['rental_started_at_EST'].max().date(),
        style={'display': 'none'}
    ),
    
    html.Div([
        html.Button(
            html.Div([
                html.Img(src='/assets/KINTO-Logo.png', style={'height': '50px', 'pointer-events': 'none'}),
                html.Img(src='/assets/canada-flag.svg', className='brand-flag', style={'pointer-events': 'none'})
            ], className='brand-lockup'),
            id='logo-reset-button',
            style={
                'background': 'none',
                'border': 'none',
                'padding': '10px',
                'cursor': 'pointer',
                'display': 'inline-block',
                'border-radius': '4px',
                'transition': 'background-color 0.2s ease'
            },
            title='Click to reset all filters and return to overview'
        ),
        html.H1("Rideshare Rental Performance Dashboard", style={'display': 'inline-block', 'margin-left': '20px', 'color': '#2C353B'})
    ], className='dashboard-brandbar', style={'display': 'flex', 'align-items': 'center', 'margin-bottom': '20px'}),
    
    dbc.Row([
        dbc.Col([
            dbc.Button(
                "↻ Refresh Data",
                id='refresh-data-btn',
                color='secondary',
                outline=True,
                size='sm',
                title='Reload all data from source Excel files',
                style={'margin-bottom': '6px'}
            )
        ], width='auto')
    ]),

    dcc.Tabs(id='main-tabs', value='overview', children=[
        dcc.Tab(label='Overview', value='overview'),
        dcc.Tab(label='Monthly Comparison', value='monthly'),
        dcc.Tab(label='Dealer Performance', value='dealer'),
        dcc.Tab(label='Vehicle Performance', value='vehicle'),
        dcc.Tab(label='Rental Details', value='rental'),
        dcc.Tab(label='Driver Analysis', value='driver'),
        dcc.Tab(label='Expenses Analysis', value='expenses'),
    ], className='tab-style', style={'margin': '12px 0'}),

    # Global Filters (always visible)
    html.Div([
        html.Hr(style={'margin': '6px 0 10px 0', 'opacity': '0.2'}),
        html.H3("Filters", className='filters-title'),
        dbc.Row([
            dbc.Col([
                html.Label("Station Name (Dealer)", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '4px'}),
                dcc.Dropdown(
                    id='station_filter',
                    options=[{'label': s, 'value': s} for s in sorted(df['station_name'].unique())],
                    multi=True,
                    placeholder="Select dealers"
                )
            ], xs=12, md=6, lg=3),
            dbc.Col([
                html.Label("Vehicle Type", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '4px'}),
                dcc.Dropdown(
                    id='vehicle_type_filter',
                    options=[{'label': v, 'value': v} for v in sorted(df['vehicle_type'].unique())],
                    multi=True,
                    placeholder="Select vehicle types"
                )
            ], xs=12, md=6, lg=3),
            dbc.Col([
                html.Label("VIN", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '4px'}),
                dcc.Dropdown(
                    id='vin_filter',
                    options=[{'label': v, 'value': v} for v in sorted([x for x in df['VIN'].unique() if pd.notna(x)])],
                    multi=True,
                    placeholder="Select VINs"
                )
            ], xs=12, md=6, lg=3),
            dbc.Col([
                html.Label("License Plate", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '4px'}),
                dcc.Dropdown(
                    id='license_plate_filter',
                    options=[{'label': l, 'value': l} for l in sorted(df['license_plate_number'].unique())],
                    multi=True,
                    placeholder="Select license plates"
                )
            ], xs=12, md=6, lg=3),
        ], className='g-2 mb-2'),

        html.Hr(style={'margin': '6px 0 10px 0', 'opacity': '0.2'}),

        dbc.Row([
            dbc.Col([
                html.Label("Unit Status", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '4px'}),
                dcc.Dropdown(
                    id='fleet_status_filter',
                    options=[{'label': s, 'value': s} for s in fleet_status_values],
                    value=None,
                    multi=True,
                    placeholder="All Unit Statuses"
                )
            ], xs=12, md=12, lg=12),
        ], className='g-2 mb-2'),

        html.Hr(style={'margin': '6px 0 10px 0', 'opacity': '0.2'}),

        dbc.Row([
            dbc.Col([
                html.Label("Year", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '4px'}),
                dcc.Dropdown(
                    id='year_filter',
                    options=[{'label': str(y), 'value': y} for y in sorted(df['start_year'].unique())],
                    multi=True,
                    placeholder="Select years"
                )
            ], xs=12, md=6, lg=6),
            dbc.Col([
                html.Label("Month", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '4px'}),
                dcc.Dropdown(
                    id='month_filter',
                    options=[{'label': m, 'value': m} for m in sorted(df['start_month_name'].unique(), key=lambda m: datetime.strptime(m, '%B').month)],
                    multi=True,
                    placeholder="Select months"
                )
            ], xs=12, md=6, lg=6),
        ], className='g-2 mb-1'),
    ], id='rental-filters-div', style={'margin-bottom': '12px'}),

    # Tab Content Containers
    html.Div(id='overview-content', children=[
        # Executive Overview
        html.Hr(),
        html.H3("Executive Snapshot (Filtered Period)", className='section-title'),
        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Total Revenue", className='kpi-label', style={'textAlign': 'center'}), html.Div(id='kpi_revenue', className='kpi-value', style={'textAlign': 'center', 'width': '100%'})], style={'textAlign': 'center', 'display': 'flex', 'flexDirection': 'column', 'justifyContent': 'center', 'alignItems': 'center'})], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Total Rentals", className='kpi-label', style={'textAlign': 'center'}), html.Div(id='kpi_rentals', className='kpi-value', style={'textAlign': 'center', 'width': '100%'})], style={'textAlign': 'center', 'display': 'flex', 'flexDirection': 'column', 'justifyContent': 'center', 'alignItems': 'center'})], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Total Rental Days", className='kpi-label', style={'textAlign': 'center'}), html.Div(id='kpi_rental_days', className='kpi-value', style={'textAlign': 'center', 'width': '100%'})], style={'textAlign': 'center', 'display': 'flex', 'flexDirection': 'column', 'justifyContent': 'center', 'alignItems': 'center'})], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Avg Revenue/Rental", className='kpi-label', style={'textAlign': 'center'}), html.Div(id='kpi_avg_rev', className='kpi-value', style={'textAlign': 'center', 'width': '100%'})], style={'textAlign': 'center', 'display': 'flex', 'flexDirection': 'column', 'justifyContent': 'center', 'alignItems': 'center'})], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div(html.Span("Total KMs Traveled", id='kpi_total_kms_label'), className='kpi-label', style={'textAlign': 'center'}), html.Div(id='kpi_avg_days', className='kpi-value', style={'textAlign': 'center', 'width': '100%'})], style={'textAlign': 'center', 'display': 'flex', 'flexDirection': 'column', 'justifyContent': 'center', 'alignItems': 'center'})], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Avg KMs Traveled", className='kpi-label', style={'textAlign': 'center'}), html.Div(id='kpi_avg_kms', className='kpi-value', style={'textAlign': 'center', 'width': '100%'})], style={'textAlign': 'center', 'display': 'flex', 'flexDirection': 'column', 'justifyContent': 'center', 'alignItems': 'center'})], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
        ], className='g-3 dashboard-kpi-row overview-kpi-row', style={'marginBottom': '20px'}),
        dbc.Tooltip(
            "Total kilometers driven across all rentals in the selected period",
            target='kpi_total_kms_label',
            placement='top'
        ),
        dbc.Row([
            dbc.Col(dcc.Graph(id='trend_revenue', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=4, className='dashboard-graph-col'),
            dbc.Col(dcc.Graph(id='trend_rentals', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=4, className='dashboard-graph-col'),
            dbc.Col(dcc.Graph(id='trend_rental_days', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=4, className='dashboard-graph-col'),
        ], className='g-3 dashboard-chart-row'),
        html.Hr(style={'margin': '16px 0 10px 0'}),
        html.H5("Current Month Progress & Month-End Forecast", className='section-subtitle'),
        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Month-End Forecast (Capacity-aware) Revenue", className='kpi-label'),
                html.Div(id='cum_proj_rev', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, xl=4, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Month-End Forecast (Capacity-aware) Rentals", className='kpi-label'),
                html.Div(id='cum_proj_rentals', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, xl=4, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Month-End Forecast (Capacity-aware) Rental Days", className='kpi-label'),
                html.Div(id='cum_proj_days', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, xl=4, className='dashboard-kpi-col'),
        ], className='g-3 dashboard-kpi-row'),
        html.Div(id='cum_forecast_confidence', className='cum-summary-text', style={'marginTop': '8px', 'fontWeight': '700'}),
        dbc.Row([
            dbc.Col([
                html.Div(id='cum_revenue_summary', className='cum-summary-text'),
                dcc.Graph(id='cum_revenue_chart', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False})
            ], xs=12, xl=4, className='dashboard-graph-col'),
            dbc.Col([
                html.Div(id='cum_rentals_summary', className='cum-summary-text'),
                dcc.Graph(id='cum_rentals_chart', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False})
            ], xs=12, xl=4, className='dashboard-graph-col'),
            dbc.Col([
                html.Div(id='cum_days_summary', className='cum-summary-text'),
                dcc.Graph(id='cum_days_chart', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False})
            ], xs=12, xl=4, className='dashboard-graph-col'),
        ], className='g-2 mt-2 dashboard-chart-row'),
        dbc.Alert(id='cum_forecast_explanation', color='light', className='mt-2 mb-1', style={'border': '1px solid #e5e7eb'}),
        dbc.Alert(id='cum_reconciliation_warning', color='warning', className='mt-1 mb-1', style={'display': 'none'}),
    ], style={'display': 'block'}),
    
    html.Div(id='monthly-content', children=[
        # Monthly Comparison
        html.Hr(),
        html.H3("Monthly Comparison", className='section-title'),
        html.Div([
            dbc.Row([
                dbc.Col([
                    html.Label("Select Month for Comparison", className='section-subtitle', style={'marginBottom': '0.45rem'}),
                    dcc.Dropdown(
                        id='comparison_month',
                        options=[{'label': ym, 'value': ym} for ym in sorted(df['year_month'].unique())],
                        placeholder="Select year-month"
                    )
                ], xs=12, md=6, xl=4),
            ], className='g-3')
        ], className='monthly-filter-card'),
        html.Div(id='monthly_comparison', className='monthly-comparison-container'),
    ], className='monthly-tab-shell', style={'display': 'none'}),
    
    html.Div(id='dealer-content', children=[
        # Dealer Performance
        html.Hr(),
        html.H3("Dealer Performance"),
        
        html.H5("Fleet Efficiency", className='section-subtitle', style={'marginTop': '12px'}),
        dbc.Row([
            dbc.Col(dcc.Graph(id='dealer_revenue_per_vehicle', className='dashboard-graph'), xs=12, lg=6),
            dbc.Col(dcc.Graph(id='dealer_rentals_per_vehicle', className='dashboard-graph'), xs=12, lg=6),
        ], className='g-3 dashboard-chart-row'),

        html.H5("Fleet Composition", className='section-subtitle', style={'marginTop': '12px'}),
        dbc.Row([
            dbc.Col(dcc.Graph(id='dealer_num_vehicles', className='dashboard-graph'), xs=12, lg=6),
            dbc.Col(dcc.Graph(id='dealer_vehicle_mix', className='dashboard-graph'), xs=12, lg=6),
        ], className='g-3 dashboard-chart-row'),

        html.H5("Model Performance: Mirai", className='section-subtitle', style={'marginTop': '12px'}),
        dbc.Row([
            dbc.Col(dcc.Graph(id='dealer_mirai_performance', className='dashboard-graph'), xs=12),
        ], className='g-3 dashboard-chart-row'),

        html.H5("Driver Quality & Engagement", className='section-subtitle', style={'marginTop': '12px'}),
        dbc.Row([
            dbc.Col(dcc.Graph(id='dealer_repeat_driver_rate', className='dashboard-graph'), xs=12, lg=6),
            dbc.Col(dcc.Graph(id='dealer_rentals_per_driver', className='dashboard-graph'), xs=12, lg=6),
        ], className='g-3 dashboard-chart-row'),

        html.H5("Efficiency Overview", className='section-subtitle', style={'marginTop': '12px'}),
        dbc.Row([
            dbc.Col(dcc.Graph(id='dealer_efficiency_scatter', className='dashboard-graph'), xs=12),
        ], className='g-3 dashboard-chart-row'),

        dash_table.DataTable(
            id='dealer_table',
            columns=[
                {'name': 'Station Name', 'id': 'station_name'},
                {'name': '# Vehicles', 'id': 'vehicles', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'Total Revenue', 'id': 'total_revenue', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Rentals', 'id': 'rentals', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Rental Days', 'id': 'rental_days', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Avg Revenue', 'id': 'avg_revenue', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Avg Rental Days', 'id': 'avg_days', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
            ],
            data=[],
            sort_action='native',
            filter_action='native',
            page_action='none',
            fixed_rows={'headers': True},
            style_table={'overflowX': 'auto', 'overflowY': 'auto', 'maxHeight': '420px', 'marginTop': '12px'},
        ),
    ], style={'display': 'none'}),
    
    html.Div(id='vehicle-content', children=[
        # Vehicle Performance
        html.Hr(),
        html.H3("Vehicle Performance"),

        html.H5("Vehicle Performance Index (Action View)", style={'marginTop': '10px'}),
        dbc.Row([
            dbc.Col(dcc.Graph(id='vehicle_top10_chart'), width=12, lg=6),
            dbc.Col(dcc.Graph(id='vehicle_mileage_chart'), width=12, lg=6),
        ], className='mb-3'),

        html.H5("Mileage Monitoring", style={'marginTop': '6px'}),
        dbc.Row([
            dbc.Col(html.Div(
                dbc.Card(dbc.CardBody([
                    html.Div("⚙ Vehicles > 15,000 km", style={'fontSize': '0.82rem', 'color': '#6b7280', 'fontWeight': '600'}),
                    html.Div(id='veh_mileage_kpi_15000', style={'fontSize': '2rem', 'fontWeight': '700', 'color': '#1f2937', 'lineHeight': '1.1', 'marginTop': '8px'}),
                    html.Div("Click to filter", style={'fontSize': '0.72rem', 'color': '#9ca3af', 'marginTop': '6px'}),
                ], style={'padding': '14px 16px'}), id='veh_kpi_card_15000_card',
                style={'borderRadius': '12px', 'border': '1px solid #e5e7eb', 'boxShadow': '0 2px 10px rgba(17,24,39,0.06)', 'height': '100%', 'transition': 'all 0.2s'}),
                id='veh_kpi_card_15000_btn', n_clicks=0, style={'cursor': 'pointer', 'height': '100%'}
            ), width=12, md=6, lg=3),
            dbc.Col(html.Div(
                dbc.Card(dbc.CardBody([
                    html.Div("⚠ Vehicles 15,000–19,999 km", style={'fontSize': '0.82rem', 'color': '#6b7280', 'fontWeight': '600'}),
                    html.Div(id='veh_mileage_kpi_15_20', style={'fontSize': '2rem', 'fontWeight': '700', 'color': '#b7791f', 'lineHeight': '1.1', 'marginTop': '8px'}),
                    html.Div("Click to filter", style={'fontSize': '0.72rem', 'color': '#9ca3af', 'marginTop': '6px'}),
                ], style={'padding': '14px 16px'}), id='veh_kpi_card_15_20_card',
                style={'borderRadius': '12px', 'border': '1px solid #f6d58b', 'boxShadow': '0 2px 10px rgba(17,24,39,0.06)', 'height': '100%', 'transition': 'all 0.2s'}),
                id='veh_kpi_card_15_20_btn', n_clicks=0, style={'cursor': 'pointer', 'height': '100%'}
            ), width=12, md=6, lg=3),
            dbc.Col(html.Div(
                dbc.Card(dbc.CardBody([
                    html.Div("⛔ Vehicles >= 20,000 km", style={'fontSize': '0.82rem', 'color': '#6b7280', 'fontWeight': '600'}),
                    html.Div(id='veh_mileage_kpi_20', style={'fontSize': '2rem', 'fontWeight': '700', 'color': '#c53030', 'lineHeight': '1.1', 'marginTop': '8px'}),
                    html.Div("Click to filter", style={'fontSize': '0.72rem', 'color': '#9ca3af', 'marginTop': '6px'}),
                ], style={'padding': '14px 16px'}), id='veh_kpi_card_20_card',
                style={'borderRadius': '12px', 'border': '1px solid #f0b2b2', 'boxShadow': '0 2px 10px rgba(17,24,39,0.06)', 'height': '100%', 'transition': 'all 0.2s'}),
                id='veh_kpi_card_20_btn', n_clicks=0, style={'cursor': 'pointer', 'height': '100%'}
            ), width=12, md=6, lg=3),
            dbc.Col(dbc.Card(dbc.CardBody([
                html.Div("🛣 Highest Current Mileage", style={'fontSize': '0.82rem', 'color': '#6b7280', 'fontWeight': '600'}),
                html.Div(id='veh_mileage_kpi_max', style={'fontSize': '2rem', 'fontWeight': '700', 'color': '#1f2937', 'lineHeight': '1.1', 'marginTop': '8px'})
            ], style={'padding': '14px 16px'}), style={'borderRadius': '12px', 'border': '1px solid #e5e7eb', 'boxShadow': '0 2px 10px rgba(17,24,39,0.06)', 'height': '100%'}), width=12, md=6, lg=3),
        ], className='g-3 mb-2'),

        dbc.Row([
            dbc.Col(html.Div(id='veh_selected_summary', style={'margin': '4px 0 8px 0', 'color': '#374151', 'fontWeight': '600'}), width=9),
            dbc.Col(dbc.Button('Clean Filter', id='veh_clean_filter_btn', color='danger', outline=True, size='sm'), width=3, style={'textAlign': 'right'})
        ], className='mb-2'),
        html.Div(
            id='veh_high_mileage_empty_state',
            children='No vehicles match the current Vehicle Performance selections.',
            style={'display': 'none', 'padding': '20px', 'textAlign': 'center', 'color': '#6b7280',
                   'fontStyle': 'italic', 'border': '1px solid #e5e7eb', 'borderRadius': '8px', 'margin': '8px 0'}
        ),

        dash_table.DataTable(
            id='vehicle_high_mileage_table',
            columns=[
                {'name': 'Mileage Status', 'id': 'mileage_status'},
                {'name': 'Model', 'id': 'Model'},
                {'name': 'Color', 'id': 'Colour'},
                {'name': 'Model Year', 'id': 'MY', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'Current Mileage', 'id': 'current_mileage', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'Dealer Name', 'id': 'station_name'},
                {'name': 'VIN', 'id': 'VIN'},
            ],
            data=[],
            sort_action='native',
            filter_action='native',
            page_action='none',
            fixed_rows={'headers': True},
            style_data_conditional=[
                {
                    'if': {'filter_query': '{current_mileage} >= 15000 && {current_mileage} < 20000', 'column_id': 'mileage_status'},
                    'color': '#f5a623',
                    'fontWeight': '700'
                },
                {
                    'if': {'filter_query': '{current_mileage} >= 20000', 'column_id': 'mileage_status'},
                    'color': '#d4420b',
                    'fontWeight': '700'
                },
            ],
            style_cell={'padding': '8px', 'fontSize': '13px', 'textAlign': 'left'},
            style_cell_conditional=[
                {'if': {'column_id': 'mileage_status'}, 'textAlign': 'center', 'width': '80px'},
                {'if': {'column_id': 'current_mileage'}, 'textAlign': 'right'},
                {'if': {'column_id': 'MY'}, 'textAlign': 'right'},
            ],
            style_header={'fontWeight': '700', 'backgroundColor': '#f8f9fa'},
            style_table={'overflowX': 'auto', 'overflowY': 'auto', 'maxHeight': '420px'},
        ),

        html.H5("Vehicle Detail Table", style={'marginTop': '16px'}),
        dash_table.DataTable(
            id='vehicle_table',
            columns=[
                {'name': 'Vehicle ID', 'id': 'vehicle_id'},
                {'name': 'License Plate', 'id': 'license_plate_number'},
                {'name': 'VIN', 'id': 'VIN'},
                {'name': '5VIN', 'id': '5VIN'},
                {'name': 'Vehicle Type', 'id': 'vehicle_type'},
                {'name': 'Rentals', 'id': 'rentals', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Rental Days', 'id': 'rental_days', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Revenue', 'id': 'revenue', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Avg Revenue', 'id': 'avg_revenue', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Avg KMs', 'id': 'avg_kms', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
            ],
            data=[],
            sort_action='native',
            filter_action='native',
            page_action='none',
            fixed_rows={'headers': True},
            style_table={'overflowX': 'auto', 'overflowY': 'auto', 'maxHeight': '420px'},
        ),
    ], style={'display': 'none'}),
    
    html.Div(id='rental-content', children=[
        html.Hr(),
        html.H3("Rental Details", className='section-title'),

        dbc.Row([
            dbc.Col([
                html.Label("Renter Name", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '6px'}),
                dcc.Dropdown(
                    id='renter_filter_rental',
                    options=[{'label': r, 'value': r} for r in sorted(df['renter_name'].unique())],
                    multi=True,
                    placeholder="Filter Rental Details by renter"
                )
            ], xs=12, md=12, lg=12),
        ], className='g-3 mb-3'),

        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Total Rental Days", className='kpi-label'),
                html.Div(id='rental_kpi_total_days', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Completed Days", className='kpi-label'),
                html.Div(id='rental_kpi_completed_days', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Ongoing Days", className='kpi-label'),
                html.Div(id='rental_kpi_ongoing_days', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Ongoing Rentals", className='kpi-label'),
                html.Div(id='rental_kpi_ongoing_count', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Avg Duration", className='kpi-label'),
                html.Div(id='rental_kpi_avg_duration', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div(html.Span("Late Return %", id='rental_kpi_late_return_label'), className='kpi-label'),
                html.Div(id='rental_kpi_long_ongoing', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
        ], className='g-3 dashboard-kpi-row'),

        dbc.Tooltip(
            "Percentage of rentals returned after their scheduled end time (with 2-hour tolerance)",
            target='rental_kpi_late_return_label',
            placement='top'
        ),

        dbc.Alert(id='rental_insight_summary', color='light', className='mt-2 mb-3', style={'border': '1px solid #e5e7eb', 'textAlign': 'center', 'whiteSpace': 'normal', 'lineHeight': '1.55', 'wordBreak': 'break-word'}),

        dbc.Row([
            dbc.Col(dcc.Graph(id='rental_status_breakdown_chart', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=4, className='dashboard-graph-col'),
            dbc.Col(dcc.Graph(id='rental_active_trend_chart', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=4, className='dashboard-graph-col'),
            dbc.Col(dcc.Graph(id='rental_completed_ongoing_chart', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=4, className='dashboard-graph-col'),
        ], className='g-3 dashboard-chart-row'),

        html.H5("Rental Operational Table", className='section-subtitle', style={'marginTop': '10px'}),
        dash_table.DataTable(
            id='rental_table',
            columns=[
                {'name': 'Rental ID', 'id': 'rental_id', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'Rental Start Date', 'id': 'rental_started_at_EST'},
                {'name': 'Rental End Date', 'id': 'rental_end_datetime_EST'},
                {'name': 'Rental Status', 'id': 'rental_status'},
                {'name': 'Ongoing', 'id': 'ongoing_risk_bucket'},
                {'name': 'Renter Name', 'id': 'renter_name'},
                {'name': 'Station Name', 'id': 'station_name'},
                {'name': 'Model', 'id': 'Model'},
                {'name': 'License Plate', 'id': 'license_plate_number'},
                {'name': '5VIN', 'id': '5VIN'},
                {'name': 'Rental Days', 'id': 'rental_days', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'KMs Traveled', 'id': 'kms_traveled', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Total to Charge', 'id': 'total_to_charge', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
            ],
            data=[],
            sort_action='native',
            filter_action='native',
            page_action='none',
            fixed_rows={'headers': True},
            style_table={'overflowX': 'auto', 'overflowY': 'auto', 'maxHeight': '460px'},
            style_cell={
                'padding': '8px',
                'fontSize': '13px',
                'textAlign': 'left',
                'whiteSpace': 'normal',
                'height': 'auto',
                'minWidth': '90px',
            },
            style_header={
                'fontWeight': '700',
                'whiteSpace': 'normal',
                'height': 'auto',
                'lineHeight': '1.25',
                'textAlign': 'center',
            },
            style_cell_conditional=[
                {'if': {'column_id': 'rental_id'}, 'minWidth': '72px', 'width': '72px', 'maxWidth': '72px', 'textAlign': 'right'},
                {'if': {'column_id': 'rental_started_at_EST'}, 'minWidth': '175px', 'width': '175px', 'maxWidth': '175px'},
                {'if': {'column_id': 'rental_end_datetime_EST'}, 'minWidth': '175px', 'width': '175px', 'maxWidth': '175px'},
                {'if': {'column_id': 'rental_status'}, 'minWidth': '110px', 'width': '110px', 'maxWidth': '110px', 'textAlign': 'center'},
                {'if': {'column_id': 'ongoing_risk_bucket'}, 'minWidth': '110px', 'width': '110px', 'maxWidth': '110px', 'textAlign': 'center'},
                {'if': {'column_id': 'renter_name'}, 'minWidth': '220px', 'width': '220px', 'maxWidth': '260px'},
                {'if': {'column_id': 'station_name'}, 'minWidth': '220px', 'width': '220px', 'maxWidth': '260px'},
                {'if': {'column_id': 'Model'}, 'minWidth': '60px', 'width': '60px', 'maxWidth': '70px', 'textAlign': 'center'},
                {'if': {'column_id': 'license_plate_number'}, 'minWidth': '110px', 'width': '110px', 'maxWidth': '120px', 'textAlign': 'center'},
                {'if': {'column_id': '5VIN'}, 'minWidth': '70px', 'width': '70px', 'maxWidth': '80px', 'textAlign': 'center'},
                {'if': {'column_id': 'rental_days'}, 'minWidth': '95px', 'width': '95px', 'maxWidth': '105px', 'textAlign': 'right'},
                {'if': {'column_id': 'kms_traveled'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '120px', 'textAlign': 'right'},
                {'if': {'column_id': 'total_to_charge'}, 'minWidth': '105px', 'width': '105px', 'maxWidth': '130px', 'textAlign': 'right'},
            ],
            style_data_conditional=[
                {
                    'if': {'filter_query': '{rental_status} = "Ongoing"', 'column_id': 'rental_status'},
                    'color': '#00708D',
                    'fontWeight': '700'
                },
                {
                    'if': {'filter_query': '{ongoing_risk_bucket} = ">14 days" || {ongoing_risk_bucket} = ">30 days"', 'column_id': 'ongoing_risk_bucket'},
                    'color': '#00708D',
                    'fontWeight': '700'
                },
            ],
        ),
    ], style={'display': 'none'}),
    
    html.Div(id='driver-content', children=[
        # Driver Analysis
        html.Hr(),
        html.H3("Driver Analysis", className='section-title'),

        dbc.Row([
            dbc.Col([
                html.Label("Renter Name / ID", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '6px'}),
                dcc.Dropdown(
                    id='renter_filter_driver',
                    options=[
                        {'label': f"{row['renter_name']} (ID: {row['customer_id']})", 'value': row['renter_name']}
                        for _, row in df[['customer_id', 'renter_name']].drop_duplicates('customer_id').sort_values('renter_name').iterrows()
                    ],
                    multi=True,
                    placeholder="Filter Driver Analysis by renter name or ID"
                )
            ], xs=12, md=12, lg=12),
        ], className='g-3 mb-3'),
        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Active Drivers %", className='kpi-label'),
                html.Div(id='driver_kpi_total', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Dormant Drivers %", className='kpi-label'),
                html.Div(id='driver_kpi_new', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Rentals / Active Driver", className='kpi-label'),
                html.Div(id='driver_kpi_new_pct', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Revenue / Active Driver", className='kpi-label'),
                html.Div(id='driver_kpi_avg_tenure', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("NEW DRIVERS (PERIOD)", className='kpi-label'),
                html.Div(id='driver_kpi_overall_tenure', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Top 10 Driver Share", className='kpi-label'),
                html.Div(id='driver_kpi_inactive_pct', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
        ], className='g-3 dashboard-kpi-row driver-kpi-row'),

        dbc.Alert(id='driver_insight_summary', color='light', className='mt-2 mb-3', style={'border': '1px solid #e5e7eb'}),

        dbc.Row([
            dbc.Col(dcc.Graph(id='driver_new_over_time_chart', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=6, className='dashboard-graph-col'),
            dbc.Col(dcc.Graph(id='driver_active_vs_new_chart', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=6, className='dashboard-graph-col'),
        ], className='g-3 dashboard-chart-row'),

        dbc.Row([
            dbc.Col(dcc.Graph(id='driver_tenure_bucket_chart', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=4, className='dashboard-graph-col'),
            dbc.Col(dcc.Graph(id='driver_segment_chart', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=4, className='dashboard-graph-col'),
            dbc.Col(dcc.Graph(id='driver_gap_chart', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=4, className='dashboard-graph-col'),
        ], className='g-3 dashboard-chart-row'),

        dbc.Row([
            dbc.Col(dcc.Graph(id='driver_cohort_heatmap', className='dashboard-graph dashboard-graph-tall', config={'responsive': True, 'displayModeBar': False}), width=12, className='dashboard-graph-col'),
        ], className='g-3 dashboard-chart-row'),

        html.H5("Top Drivers", className='section-subtitle', style={'marginTop': '12px'}),
        dash_table.DataTable(
            id='driver_top_table',
            columns=[
                {'name': 'Cust. ID',          'id': 'customer_id'},
                {'name': 'Renter Name',        'id': 'renter_name'},
                {'name': 'First Rental',       'id': 'first_rental_date'},
                {'name': 'Tenure\n(Days)',      'id': 'driver_tenure_days', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'First Dealer',       'id': 'first_dealer'},
                {'name': 'Multi-\nDealer?',    'id': 'multi_dealer'},
                {'name': 'Rentals',            'id': 'rentals', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'Revenue ($)',        'id': 'revenue', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Rental\nDays',       'id': 'rental_days', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
            ],
            data=[],
            sort_action='native',
            filter_action='native',
            page_action='none',
            fixed_rows={'headers': True},
            style_table={'overflowX': 'auto', 'overflowY': 'auto', 'maxHeight': '420px'},
            style_cell={'textAlign': 'left', 'padding': '6px 10px', 'fontSize': '0.82rem',
                        'whiteSpace': 'normal', 'height': 'auto', 'overflow': 'hidden'},
            style_header={'whiteSpace': 'pre-line', 'textAlign': 'center',
                          'fontWeight': '600', 'fontSize': '0.80rem', 'height': 'auto'},
            style_cell_conditional=[
                {'if': {'column_id': 'customer_id'},       'width': '70px',  'minWidth': '60px',  'maxWidth': '80px'},
                {'if': {'column_id': 'renter_name'},       'width': '180px', 'minWidth': '140px', 'maxWidth': '220px'},
                {'if': {'column_id': 'first_rental_date'}, 'width': '100px', 'minWidth': '90px',  'maxWidth': '110px'},
                {'if': {'column_id': 'driver_tenure_days'},'width': '75px',  'minWidth': '65px',  'maxWidth': '90px',  'textAlign': 'right'},
                {'if': {'column_id': 'first_dealer'},      'width': '200px', 'minWidth': '160px', 'maxWidth': '250px'},
                {'if': {'column_id': 'multi_dealer'},      'width': '75px',  'minWidth': '65px',  'maxWidth': '90px',  'textAlign': 'center'},
                {'if': {'column_id': 'rentals'},           'width': '70px',  'minWidth': '60px',  'maxWidth': '85px',  'textAlign': 'right'},
                {'if': {'column_id': 'revenue'},           'width': '110px', 'minWidth': '90px',  'maxWidth': '130px', 'textAlign': 'right'},
                {'if': {'column_id': 'rental_days'},       'width': '80px',  'minWidth': '70px',  'maxWidth': '100px', 'textAlign': 'right'},
            ],
        ),

        html.H5("Driver Detail Table", className='section-subtitle', style={'marginTop': '16px'}),
        dash_table.DataTable(
            id='driver_table',
            columns=[
                {'name': 'Customer ID', 'id': 'customer_id'},
                {'name': 'Renter Name', 'id': 'renter_name'},
                {'name': 'First Rental Date', 'id': 'first_rental_date'},
                {'name': 'Tenure Bucket', 'id': 'tenure_bucket'},
                {'name': 'Tenure (Days)', 'id': 'driver_tenure_days', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'Rentals', 'id': 'rentals', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'Active Months', 'id': 'active_months', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'Avg Days Between Rentals', 'id': 'avg_days_between_rentals', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Rental Days', 'id': 'rental_days', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Revenue', 'id': 'revenue', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Avg Duration', 'id': 'avg_duration', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Avg Revenue', 'id': 'avg_revenue', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Avg KMs', 'id': 'avg_kms', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
            ],
            data=[],
            sort_action='native',
            filter_action='native',
            page_action='none',
            fixed_rows={'headers': True},
            style_table={'overflowX': 'auto', 'overflowY': 'auto', 'maxHeight': '460px'},
        ),
    ], style={'display': 'none'}),
    


    html.Div(id='expenses-content', children=[
        html.Hr(),
        html.H3("Expenses Analysis"),

        # Validation summary
        dbc.Alert(id='exp_validation_summary', color='info', className='mb-3',
                  children=f"{inv_total_rows} invoices loaded | {inv_matched} matched to fleet ({inv_matched/inv_total_rows*100:.1f}%) | {inv_unmatched} unmatched"),

        # Expense-specific filters
        html.Div([
            html.H5("Expense Filters", style={'marginBottom': '10px'}),
            dbc.Row([
                dbc.Col([
                    html.Label("Dealer"),
                    dcc.Dropdown(
                        id='exp_dealer_filter',
                        options=[{'label': d, 'value': d} for d in sorted(inv_df['Dealer Name'].dropna().unique(), key=lambda x: str(x))],
                        multi=True,
                        placeholder="All Dealers"
                    )
                ], width=3),
                dbc.Col([
                    html.Label("Work Category"),
                    dcc.Dropdown(
                        id='exp_category_filter',
                        options=[{'label': c, 'value': c} for c in sorted(inv_df['Work Category'].dropna().unique(), key=lambda x: str(x))],
                        multi=True,
                        placeholder="All Categories"
                    )
                ], width=3),
                dbc.Col([
                    html.Label("Vehicle Model"),
                    dcc.Dropdown(
                        id='exp_vehicle_filter',
                        options=[{'label': v, 'value': v} for v in sorted(inv_df['Vehicle'].dropna().unique(), key=lambda x: str(x))],
                        multi=True,
                        placeholder="All Models"
                    )
                ], width=3),
                dbc.Col([
                    html.Label("Model Year"),
                    dcc.Dropdown(
                        id='exp_year_filter',
                        options=[{'label': str(int(y)), 'value': int(y)} for y in sorted(inv_df['MY'].dropna().unique())],
                        multi=True,
                        placeholder="All Years"
                    )
                ], width=3),
                dbc.Col([
                    html.Label("Unit Status"),
                    dcc.Dropdown(
                        id='exp_unit_status_filter',
                        options=[{'label': s, 'value': s} for s in EXPENSE_UNIT_STATUS_OPTIONS],
                        multi=True,
                        value=EXPENSE_UNIT_STATUS_OPTIONS,
                        placeholder="All Unit Status"
                    )
                ], width=3),
            ], className='mb-3'),
            dbc.Row([
                dbc.Col([
                    html.Label("Expense Year"),
                    dcc.Dropdown(
                        id='exp_time_year_filter',
                        options=[{'label': str(y), 'value': y} for y in inv_sub_years],
                        multi=True,
                        placeholder="All Years"
                    )
                ], width=2),
                dbc.Col([
                    html.Label("Expense Month"),
                    dcc.Dropdown(
                        id='exp_time_month_filter',
                        options=[{'label': m, 'value': m} for m in inv_sub_months],
                        multi=True,
                        placeholder="All Months"
                    )
                ], width=3),
                dbc.Col([
                    html.Label("Expense Date Range"),
                    dcc.DatePickerRange(
                        id='exp_date_range',
                        start_date=inv_date_min,
                        end_date=inv_date_max
                    )
                ], width=5),
            ])
        ], style={'marginBottom': '20px', 'padding': '12px', 'background': '#f8f9fa', 'borderRadius': '8px'}),

        # KPI cards
        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Total Invoiced", className='kpi-label'), html.Div(id='exp_kpi_total', className='kpi-value')])], className='kpi-card'), width=2),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Invoice Count", className='kpi-label'), html.Div(id='exp_kpi_count', className='kpi-value')])], className='kpi-card'), width=2),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Avg per Invoice", className='kpi-label'), html.Div(id='exp_kpi_avg', className='kpi-value')])], className='kpi-card'), width=2),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Unique Vehicles", className='kpi-label'), html.Div(id='exp_kpi_vehicles', className='kpi-value')])], className='kpi-card'), width=2),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Avg Cost / Vehicle", className='kpi-label'), html.Div(id='exp_kpi_avg_vehicle', className='kpi-value')])], className='kpi-card'), width=2),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Fleet Match Rate", className='kpi-label'), html.Div(id='exp_kpi_match_rate', className='kpi-value')])], className='kpi-card'), width=2),
        ], className='mb-3'),

        # Charts row 1: trend (wide) + category pie (narrow)
        dbc.Row([
            dbc.Col(dcc.Graph(id='exp_trend_chart'), width=8),
            dbc.Col(dcc.Graph(id='exp_category_chart'), width=4),
        ]),

        # Charts row 2: dealer + model/year
        dbc.Row([
            dbc.Col(dcc.Graph(id='exp_dealer_chart'), width=6),
            dbc.Col(dcc.Graph(id='exp_model_chart'), width=6),
        ]),

        # Charts row 3: stacked dealer × category
        dbc.Row([
            dbc.Col(dcc.Graph(id='exp_stacked_chart'), width=12),
        ]),

        dbc.Row([
            dbc.Col([
                dbc.Button("Clear selection", id='exp_clear_selection_btn', color='secondary', outline=True, size='sm')
            ], width=12)
        ], className='mb-2'),

        html.Div(
            id='exp_drilldown_empty',
            children="Select a dealer and category from the chart to see detailed expenses",
            style={'display': 'block', 'marginBottom': '10px', 'color': '#67707d'}
        ),

        html.Div(id='exp_drilldown_container', children=[
            html.Div(id='exp_selected_slice', style={'marginBottom': '10px', 'color': '#2C353B', 'fontWeight': '600'}),

            dbc.Row([
                dbc.Col(dbc.Card([dbc.CardBody([
                    html.Div("Total Expense", className='kpi-label'),
                    html.Div(id='exp_detail_total', className='kpi-value')
                ])], className='kpi-card'), width=2),
                dbc.Col(dbc.Card([dbc.CardBody([
                    html.Div("Total Labor", className='kpi-label'),
                    html.Div(id='exp_detail_labor', className='kpi-value')
                ])], className='kpi-card'), width=2),
                dbc.Col(dbc.Card([dbc.CardBody([
                    html.Div("Total Parts", className='kpi-label'),
                    html.Div(id='exp_detail_parts', className='kpi-value')
                ])], className='kpi-card'), width=2),
                dbc.Col(dbc.Card([dbc.CardBody([
                    html.Div("Total Misc", className='kpi-label'),
                    html.Div(id='exp_detail_misc', className='kpi-value')
                ])], className='kpi-card'), width=2),
                dbc.Col(dbc.Card([dbc.CardBody([
                    html.Div("Invoices", className='kpi-label'),
                    html.Div(id='exp_detail_count', className='kpi-value')
                ])], className='kpi-card'), width=2),
            ], className='mb-3'),

            dash_table.DataTable(
                id='exp_drilldown_table',
                columns=[
                    {'name': 'Date of submission', 'id': 'Date of submission'},
                    {'name': 'Dealer Name', 'id': 'Dealer Name'},
                    {'name': 'Work Category', 'id': 'Work Category'},
                    {'name': 'Work description', 'id': 'Work description'},
                    {'name': '#SAP', 'id': 'SAP#'},
                    {'name': 'Labor', 'id': 'Labor', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                    {'name': 'Parts', 'id': 'Parts', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                    {'name': 'Misc', 'id': 'Misc', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                    {'name': 'Total (Tax inc.)', 'id': 'total', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                    {'name': 'VIN', 'id': 'VIN'},
                    {'name': 'Vehicle Model', 'id': 'Model'},
                    {'name': 'Model Year', 'id': 'MY'},
                ],
                data=[],
                sort_action='native',
                filter_action='native',
                page_action='none',
                fixed_rows={'headers': True},
                style_cell={'padding': '8px', 'fontSize': '13px', 'whiteSpace': 'normal', 'height': 'auto', 'textAlign': 'left'},
                style_table={'overflowX': 'auto', 'overflowY': 'auto', 'maxHeight': '460px'},
                style_cell_conditional=[
                    {'if': {'column_id': 'Labor'}, 'textAlign': 'right'},
                    {'if': {'column_id': 'Parts'}, 'textAlign': 'right'},
                    {'if': {'column_id': 'Misc'}, 'textAlign': 'right'},
                    {'if': {'column_id': 'total'}, 'textAlign': 'right'},
                ],
            ),
        ], style={'display': 'none'}),

        # Vehicle detail table
        html.Hr(),
        html.H5("Vehicle Expense Detail"),
        html.Div(
            'Tip: Click any row in this table to display its invoice-level detail below.',
            style={'marginBottom': '8px', 'color': '#67707d', 'fontSize': '0.92rem', 'fontWeight': '600'}
        ),
        dash_table.DataTable(
            id='exp_vehicle_table',
            columns=[
                {'name': 'VIN', 'id': 'VIN'},
                {'name': 'Vehicle Type', 'id': 'vehicle_type'},
                {'name': 'Model', 'id': 'Model'},
                {'name': 'Model Year', 'id': 'MY'},
                {'name': 'Dealer Name', 'id': 'Dealer Name'},
                {'name': 'Status', 'id': 'Status'},
                {'name': 'Invoice Count', 'id': 'invoice_count', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'Total Cost', 'id': 'total_cost', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
            ],
            data=[],
            sort_action='native',
            filter_action='native',
            page_action='none',
            fixed_rows={'headers': True},
            style_cell={
                'padding': '8px',
                'fontSize': '13px',
                'textAlign': 'center',
                'whiteSpace': 'normal',
                'height': 'auto',
                'width': '12.5%',
                'minWidth': '12.5%',
                'maxWidth': '12.5%'
            },
            style_header={
                'whiteSpace': 'normal',
                'height': 'auto',
                'textAlign': 'center',
                'fontWeight': '700',
            },
            style_table={'overflowX': 'auto', 'overflowY': 'auto', 'maxHeight': '420px'},
            style_data={'cursor': 'pointer'},
            style_data_conditional=[
                {'if': {'state': 'active'}, 'backgroundColor': '#eef6fb', 'border': '1px solid #00708D'},
            ],
            style_cell_conditional=[
                {'if': {'column_id': 'invoice_count'}, 'textAlign': 'right'},
                {'if': {'column_id': 'total_cost'}, 'textAlign': 'right'},
            ],
        ),

        dbc.Row([
            dbc.Col([
                dbc.Button("Clear vehicle selection", id='exp_clear_vehicle_selection_btn', color='secondary', outline=True, size='sm')
            ], width=12)
        ], className='mt-2 mb-2'),

        html.Div(
            id='exp_vehicle_drilldown_empty',
            children='Click a vehicle row to view the invoice details included in that summary',
            style={'display': 'block', 'marginBottom': '10px', 'color': '#67707d'}
        ),

        html.Div(id='exp_vehicle_drilldown_container', children=[
            dbc.Row([
                dbc.Col(dbc.Card([dbc.CardBody([
                    html.Div('Selected VIN', className='kpi-label'),
                    html.Div(id='exp_vehicle_sel_vin', className='kpi-value')
                ])], className='kpi-card'), width=3),
                dbc.Col(dbc.Card([dbc.CardBody([
                    html.Div('Selected 5VIN', className='kpi-label'),
                    html.Div(id='exp_vehicle_sel_5vin', className='kpi-value')
                ])], className='kpi-card'), width=3),
                dbc.Col(dbc.Card([dbc.CardBody([
                    html.Div('Invoice Count', className='kpi-label'),
                    html.Div(id='exp_vehicle_inv_count', className='kpi-value')
                ])], className='kpi-card'), width=3),
                dbc.Col(dbc.Card([dbc.CardBody([
                    html.Div('Total Cost', className='kpi-label'),
                    html.Div(id='exp_vehicle_total_cost', className='kpi-value')
                ])], className='kpi-card'), width=3),
            ], className='mb-3'),

            dash_table.DataTable(
                id='exp_vehicle_invoice_table',
                columns=[
                    {'name': 'Date of submission', 'id': 'Date of submission'},
                    {'name': 'Dealer Name', 'id': 'Dealer Name'},
                    {'name': 'Invoice #', 'id': 'Invoice #'},
                    {'name': 'SAP#', 'id': 'SAP#'},
                    {'name': 'Work Category', 'id': 'Work Category'},
                    {'name': 'Work description', 'id': 'Work description'},
                    {'name': 'total', 'id': 'total', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                    {'name': '5VIN', 'id': '5VIN_key'},
                ],
                data=[],
                sort_action='native',
                filter_action='native',
                page_action='none',
                fixed_rows={'headers': True},
                style_cell={'padding': '8px', 'fontSize': '13px', 'whiteSpace': 'normal', 'height': 'auto', 'textAlign': 'left'},
                style_table={'overflowX': 'auto', 'overflowY': 'auto', 'maxHeight': '460px'},
                style_cell_conditional=[
                    {'if': {'column_id': 'total'}, 'textAlign': 'right'},
                ],
            ),
        ], style={'display': 'none'}),
    ], style={'display': 'none'}),
], fluid=True)

# Callback 1: Unified handler for logo reset, filter changes, and tab navigation
@app.callback(
    [Output('station_filter', 'value'),
     Output('vehicle_type_filter', 'value'),
     Output('license_plate_filter', 'value'),
    Output('renter_filter_rental', 'value'),
    Output('renter_filter_driver', 'value'),
     Output('year_filter', 'value'),
     Output('month_filter', 'value'),
     Output('fleet_status_filter', 'value'),
     Output('date_range', 'start_date'),
     Output('date_range', 'end_date'),
     Output('main-tabs', 'value'),
     Output('comparison_month', 'value'),
     Output('app-state-store', 'data')],
    [Input('logo-reset-button', 'n_clicks'),
     Input('main-tabs', 'value')],
    State('app-state-store', 'data'),
    prevent_initial_call=False
)
def unified_state_handler(logo_clicks, current_tab, stored_data):
    """Handle both logo reset and tab toggle behavior."""
    if stored_data is None:
        stored_data = {'previous_tab': 'overview', 'last_logo_clicks': 0}

    reset_start = df['rental_started_at_EST'].min().date()
    reset_end = df['rental_started_at_EST'].max().date()

    last_logo_clicks = stored_data.get('last_logo_clicks', 0)
    logo_was_clicked = logo_clicks and logo_clicks > last_logo_clicks

    if logo_was_clicked:
        updated_store = {'previous_tab': 'overview', 'last_logo_clicks': logo_clicks}
        return (
            None, None, None, None, None, None, None, None,
            reset_start, reset_end, 'overview', None, updated_store
        )

    previous_tab = stored_data.get('previous_tab', 'overview')

    if current_tab == previous_tab and current_tab != 'overview':
        updated_store = {'previous_tab': 'overview', 'last_logo_clicks': logo_clicks or 0}
        return (
            None, None, None, None, None, None, None, None,
            reset_start, reset_end, 'overview', None, updated_store
        )

    updated_store = {'previous_tab': current_tab, 'last_logo_clicks': logo_clicks or 0}
    from dash import no_update
    return (
        no_update, no_update, no_update, no_update, no_update, no_update,
        no_update, no_update, no_update, no_update, no_update, no_update,
        updated_store
    )

# Helper function to create complete monthly date series with zero-fill
def build_complete_monthly_series(filtered_df, value_col):
    """Build a complete monthly series with zero values for missing months"""
    if filtered_df.empty:
        return pd.DataFrame({value_col: [], 'year_month_dt': []})
    
    monthly_data = filtered_df.groupby('year_month_dt')[value_col].sum().reset_index()
    
    # Get date range from filtered data
    min_date = monthly_data['year_month_dt'].min()
    max_date = monthly_data['year_month_dt'].max()
    
    # Create complete monthly range
    if pd.notna(min_date) and pd.notna(max_date):
        date_range = pd.date_range(start=min_date, end=max_date, freq='MS')
        complete_df = pd.DataFrame({'year_month_dt': date_range})
        complete_df = complete_df.merge(monthly_data, on='year_month_dt', how='left')
        complete_df[value_col] = complete_df[value_col].fillna(0.0)
        return complete_df.sort_values('year_month_dt')
    
    return monthly_data.sort_values('year_month_dt')


def _monthly_time_axis(point_count):
    if point_count <= 6:
        dtick = 'M1'
        tickangle = 0
    elif point_count <= 12:
        dtick = 'M2'
        tickangle = -20
    else:
        dtick = 'M3'
        tickangle = -25

    return dict(
        type='date',
        tickformat='%b %Y',
        tickmode='linear',
        dtick=dtick,
        showgrid=False,
        title='',
        tickangle=tickangle,
        automargin=True,
        nticks=8,
    )


def _apply_standard_figure_layout(fig, title, *, xaxis=None, yaxis=None, height=360, hovermode='x unified', show_legend=False, legend_y=1.10, bottom_margin=58):
    layout_kwargs = dict(
        template='plotly_white',
        title=dict(text=title, x=0, xanchor='left', y=0.98, yanchor='top', font=dict(size=15, color='#1f2937')),
        autosize=True,
        height=height,
        hovermode=hovermode,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(t=108 if show_legend else 74, r=20, b=bottom_margin, l=20),
    )

    if show_legend:
        layout_kwargs['legend'] = dict(
            orientation='h',
            yanchor='bottom',
            y=legend_y,
            xanchor='left',
            x=0,
            bgcolor='rgba(0,0,0,0)',
            borderwidth=0,
            font=dict(size=11, color='#4b5563'),
            title=dict(text='')
        )

    fig.update_layout(**layout_kwargs)

    if xaxis:
        fig.update_xaxes(**xaxis)
    else:
        fig.update_xaxes(automargin=True)

    if yaxis:
        fig.update_yaxes(**yaxis)
    else:
        fig.update_yaxes(automargin=True)

    return fig


def get_filtered_expense_df(dealers, categories, vehicles, model_years, exp_years, exp_months, exp_start, exp_end, fleet_statuses=None, exp_unit_statuses=None):
    filt = inv_df.copy()

    if dealers:
        filt = filt[filt['Dealer Name'].isin(dealers)]
    if categories:
        filt = filt[filt['Work Category'].isin(categories)]
    if vehicles:
        filt = filt[filt['Vehicle'].isin(vehicles)]
    if model_years:
        filt = filt[filt['MY'].isin([float(y) for y in model_years])]
    if exp_years:
        filt = filt[filt['Date of submission'].dt.year.isin([int(y) for y in exp_years])]
    if exp_months:
        filt = filt[filt['sub_month_name'].isin(exp_months)]
    if exp_start and exp_end:
        filt = filt[
            (filt['Date of submission'].dt.date >= pd.to_datetime(exp_start).date()) &
            (filt['Date of submission'].dt.date <= pd.to_datetime(exp_end).date())
        ]
    status_series = filt['Status'].astype('string').fillna('Unknown').astype(str)
    if exp_unit_statuses:
        filt = filt[status_series.isin(exp_unit_statuses)]
    elif fleet_statuses:
        filt = filt[status_series.isin(fleet_statuses)]

    return filt

# Callback 2: Control tab visibility and global filter visibility
@app.callback(
    [Output('overview-content', 'style'),
     Output('monthly-content', 'style'),
     Output('dealer-content', 'style'),
     Output('vehicle-content', 'style'),
     Output('rental-content', 'style'),
     Output('driver-content', 'style'),
     Output('expenses-content', 'style'),
     Output('rental-filters-div', 'style')],
    [Input('main-tabs', 'value')]
)
def update_tab_visibility(selected_tab):
    tabs = ['overview', 'monthly', 'dealer', 'vehicle', 'rental', 'driver', 'expenses']
    styles = [
        {'display': 'block'} if tab == selected_tab else {'display': 'none'}
        for tab in tabs
    ]
    # Collapse global rental filters entirely when Expenses tab is active
    rental_filters_style = {'display': 'none'} if selected_tab == 'expenses' else {'margin-bottom': '20px'}
    return styles + [rental_filters_style]

# Callback to update comparison month dropdown options
@app.callback(
    Output('comparison_month', 'options'),
    [Input('station_filter', 'value'),
     Input('vehicle_type_filter', 'value'),
     Input('license_plate_filter', 'value'),
     Input('year_filter', 'value'),
     Input('month_filter', 'value'),
     Input('date_range', 'start_date'),
     Input('date_range', 'end_date'),
    Input('vin_filter', 'value'),
    Input('fleet_status_filter', 'value'),
    Input('data-refresh-counter', 'data')]
)
def update_comparison_month_options(stations, vehicle_types, plates, years, months, start_date, end_date, vins, fleet_statuses=None, _refresh=None):
    filtered_df = df.copy()
    
    if stations:
        filtered_df = filtered_df[filtered_df['station_name'].isin(stations)]
    if vehicle_types:
        filtered_df = filtered_df[filtered_df['vehicle_type'].isin(vehicle_types)]
    if plates:
        filtered_df = filtered_df[filtered_df['license_plate_number'].isin(plates)]
    if vins:
        filtered_df = filtered_df[filtered_df['VIN'].isin(vins)]
    if years:
        filtered_df = filtered_df[filtered_df['start_year'].isin(years)]
    if months:
        filtered_df = filtered_df[filtered_df['start_month_name'].isin(months)]
    if fleet_statuses:
        filtered_df = filtered_df[filtered_df['Status'].isin(fleet_statuses)]
    if start_date and end_date:
        selected_start = pd.to_datetime(start_date)
        selected_end = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        effective_end_for_filter = filtered_df['effective_rental_end_datetime'].fillna(filtered_df['rental_started_at_EST'])
        filtered_df = filtered_df[
            (filtered_df['rental_started_at_EST'] <= selected_end) &
            (effective_end_for_filter >= selected_start)
        ]
    
    available_months = sorted(filtered_df['year_month'].unique())
    return [{'label': ym, 'value': ym} for ym in available_months]

# Callbacks
@app.callback(
    [Output('kpi_revenue', 'children'),
     Output('kpi_rentals', 'children'),
     Output('kpi_rental_days', 'children'),
     Output('kpi_avg_rev', 'children'),
     Output('kpi_avg_days', 'children'),
     Output('kpi_avg_kms', 'children'),
     Output('trend_revenue', 'figure'),
     Output('trend_rentals', 'figure'),
     Output('trend_rental_days', 'figure'),
    Output('cum_proj_rev', 'children'),
    Output('cum_proj_rentals', 'children'),
    Output('cum_proj_days', 'children'),
    Output('cum_revenue_summary', 'children'),
    Output('cum_rentals_summary', 'children'),
    Output('cum_days_summary', 'children'),
    Output('cum_revenue_chart', 'figure'),
    Output('cum_rentals_chart', 'figure'),
    Output('cum_days_chart', 'figure'),
    Output('cum_forecast_confidence', 'children'),
    Output('cum_forecast_explanation', 'children'),
    Output('cum_reconciliation_warning', 'children'),
    Output('cum_reconciliation_warning', 'style'),
     Output('dealer_table', 'data'),
     Output('vehicle_table', 'data'),
    Output('vehicle_top10_chart', 'figure'),
    Output('vehicle_mileage_chart', 'figure'),
    Output('veh_mileage_kpi_15000', 'children'),
    Output('veh_mileage_kpi_15_20', 'children'),
    Output('veh_mileage_kpi_20', 'children'),
    Output('veh_mileage_kpi_max', 'children'),
    Output('veh_selected_summary', 'children'),
    Output('veh_selected_summary', 'style'),
    Output('veh_high_mileage_empty_state', 'style'),
    Output('veh_kpi_card_15000_card', 'style'),
    Output('veh_kpi_card_15_20_card', 'style'),
    Output('veh_kpi_card_20_card', 'style'),
    Output('vehicle_high_mileage_table', 'data'),
     Output('rental_table', 'data'),
    Output('rental_kpi_total_days', 'children'),
    Output('rental_kpi_completed_days', 'children'),
    Output('rental_kpi_ongoing_days', 'children'),
    Output('rental_kpi_ongoing_count', 'children'),
    Output('rental_kpi_avg_duration', 'children'),
    Output('rental_kpi_long_ongoing', 'children'),
    Output('rental_insight_summary', 'children'),
    Output('rental_status_breakdown_chart', 'figure'),
    Output('rental_active_trend_chart', 'figure'),
    Output('rental_completed_ongoing_chart', 'figure'),
     Output('driver_table', 'data'),
    Output('driver_kpi_total', 'children'),
    Output('driver_kpi_new', 'children'),
    Output('driver_kpi_new_pct', 'children'),
    Output('driver_kpi_avg_tenure', 'children'),
    Output('driver_kpi_overall_tenure', 'children'),
    Output('driver_kpi_inactive_pct', 'children'),
    Output('driver_insight_summary', 'children'),
    Output('driver_new_over_time_chart', 'figure'),
    Output('driver_active_vs_new_chart', 'figure'),
    Output('driver_tenure_bucket_chart', 'figure'),
    Output('driver_segment_chart', 'figure'),
    Output('driver_cohort_heatmap', 'figure'),
    Output('driver_gap_chart', 'figure'),
    Output('driver_top_table', 'data'),
    Output('monthly_comparison', 'children'),
    Output('dealer_revenue_per_vehicle', 'figure'),
    Output('dealer_rentals_per_vehicle', 'figure'),
    Output('dealer_num_vehicles', 'figure'),
    Output('dealer_vehicle_mix', 'figure'),
    Output('dealer_mirai_performance', 'figure'),
    Output('dealer_repeat_driver_rate', 'figure'),
    Output('dealer_rentals_per_driver', 'figure'),
    Output('dealer_efficiency_scatter', 'figure')],
    [Input('station_filter', 'value'),
     Input('vehicle_type_filter', 'value'),
     Input('license_plate_filter', 'value'),
        Input('renter_filter_rental', 'value'),
        Input('renter_filter_driver', 'value'),
     Input('year_filter', 'value'),
     Input('month_filter', 'value'),
     Input('date_range', 'start_date'),
     Input('date_range', 'end_date'),
    Input('comparison_month', 'value'),
    Input('main-tabs', 'value'),
    Input('vin_filter', 'value'),
    Input('fleet_status_filter', 'value'),
    Input('veh-selected-vehicle', 'data'),
    Input('veh-selected-mileage-band', 'data'),
    Input('data-refresh-counter', 'data')]
)
def update_all(stations, vehicle_types, plates, rental_renters, driver_renters, years, months, start_date, end_date, comparison_month, active_tab, vins, fleet_statuses=None, selected_vehicle=None, selected_band=None, _refresh=None):
    cache_key = (
        _normalize_cache_key(stations),
        _normalize_cache_key(vehicle_types),
        _normalize_cache_key(plates),
        _normalize_cache_key(rental_renters),
        _normalize_cache_key(driver_renters),
        _normalize_cache_key(years),
        _normalize_cache_key(months),
        _normalize_cache_key(start_date),
        _normalize_cache_key(end_date),
        _normalize_cache_key(comparison_month),
        _normalize_cache_key(active_tab),
        _normalize_cache_key(vins),
        _normalize_cache_key(fleet_statuses),
        _normalize_cache_key(selected_vehicle),
        _normalize_cache_key(selected_band),
        _normalize_cache_key(_refresh),
    )
    cached_result = _cache_get(cache_key)
    if cached_result is not None:
        return cached_result

    def _build_monthly_metrics_map(source_df):
        if source_df.empty:
            return {}, None

        monthly_base = source_df.copy()
        monthly_base['metric_date'] = pd.to_datetime(monthly_base['rental_started_at_EST']).dt.floor('D')
        monthly_base = monthly_base.dropna(subset=['metric_date'])
        if monthly_base.empty:
            return {}, None

        monthly_base['month_start'] = monthly_base['metric_date'].dt.to_period('M').dt.to_timestamp()
        monthly_base['year_month'] = monthly_base['month_start'].dt.strftime('%Y-%m')

        grouped = monthly_base.groupby(['month_start', 'year_month'], as_index=False).agg(
            revenue=('revenue_amount', 'sum'),
            rental_days=('rental_days', 'sum'),
            rentals=('rental_id', 'nunique'),
            distinct_rental_id=('rental_id', 'nunique'),
            row_count=('rental_id', 'size'),
        )
        grouped = grouped.sort_values('month_start')
        latest_month = grouped['month_start'].max()
        return grouped.set_index('year_month').to_dict('index'), latest_month

    filtered_df = df.copy()
    
    if stations:
        filtered_df = filtered_df[filtered_df['station_name'].isin(stations)]
    if vehicle_types:
        filtered_df = filtered_df[filtered_df['vehicle_type'].isin(vehicle_types)]
    if plates:
        filtered_df = filtered_df[filtered_df['license_plate_number'].isin(plates)]
    if vins:
        filtered_df = filtered_df[filtered_df['VIN'].isin(vins)]
    active_renter_filter = None
    if active_tab == 'rental':
        active_renter_filter = rental_renters
    elif active_tab == 'driver':
        active_renter_filter = driver_renters
    if active_renter_filter:
        filtered_df = filtered_df[filtered_df['renter_name'].isin(active_renter_filter)]
    if years:
        filtered_df = filtered_df[filtered_df['start_year'].isin(years)]

    monthly_scope_ignore_global_month_df = filtered_df.copy()

    if months:
        filtered_df = filtered_df[filtered_df['start_month_name'].isin(months)]
    cumulative_filtered_df = filtered_df.copy()

    if fleet_statuses:
        filtered_df = filtered_df[filtered_df['Status'].isin(fleet_statuses)]
        cumulative_filtered_df = cumulative_filtered_df[cumulative_filtered_df['Status'].isin(fleet_statuses)]
        monthly_scope_ignore_global_month_df = monthly_scope_ignore_global_month_df[
            monthly_scope_ignore_global_month_df['Status'].isin(fleet_statuses)
        ]
    if start_date and end_date:
        selected_start = pd.to_datetime(start_date)
        selected_end = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        effective_end_for_filter = filtered_df['effective_rental_end_datetime'].fillna(filtered_df['rental_started_at_EST'])
        filtered_df = filtered_df[
            (filtered_df['rental_started_at_EST'] <= selected_end) &
            (effective_end_for_filter >= selected_start)
        ]
        cumulative_effective_end_for_filter = cumulative_filtered_df['effective_rental_end_datetime'].fillna(cumulative_filtered_df['rental_started_at_EST'])
        cumulative_filtered_df = cumulative_filtered_df[
            (cumulative_filtered_df['rental_started_at_EST'] <= selected_end) &
            (cumulative_effective_end_for_filter >= selected_start)
        ]
        monthly_scope_effective_end_for_filter = monthly_scope_ignore_global_month_df['effective_rental_end_datetime'].fillna(monthly_scope_ignore_global_month_df['rental_started_at_EST'])
        monthly_scope_ignore_global_month_df = monthly_scope_ignore_global_month_df[
            (monthly_scope_ignore_global_month_df['rental_started_at_EST'] <= selected_end) &
            (monthly_scope_effective_end_for_filter >= selected_start)
        ]

    # Keep one unified callback, but prune heavy per-tab computations by feeding
    # empty frames when that tab is not active. This reduces CPU time on Render.
    tab_filtered_dealer_df = filtered_df if active_tab == 'dealer' else filtered_df.iloc[0:0]
    tab_filtered_vehicle_df = filtered_df if active_tab == 'vehicle' else filtered_df.iloc[0:0]
    tab_filtered_rental_df = filtered_df if active_tab == 'rental' else filtered_df.iloc[0:0]
    tab_filtered_driver_df = filtered_df if active_tab == 'driver' else filtered_df.iloc[0:0]

    shared_monthly_metrics_map, shared_target_month = _build_monthly_metrics_map(cumulative_filtered_df)
    shared_target_month_str = pd.Timestamp(shared_target_month).strftime('%Y-%m') if shared_target_month is not None else None
    
    # KPIs
    total_rev = filtered_df['revenue_amount'].sum()
    total_rentals = len(filtered_df)
    total_days = filtered_df['rental_days'].sum()
    avg_rev = total_rev / total_rentals if total_rentals > 0 else 0
    total_kms = filtered_df['kms_traveled'].sum()
    avg_kms = filtered_df['kms_traveled'].mean()
    
    # Trends with complete monthly series and data labels
    trend_data_rev = build_complete_monthly_series(filtered_df, 'revenue_amount')
    trend_rev = px.line(trend_data_rev, x='year_month_dt', y='revenue_amount', 
                        title='Revenue Over Time', markers=True, color_discrete_sequence=['#00708D'])
    trend_rev.update_traces(
        line=dict(width=3, shape='spline'), 
        marker=dict(size=6),
        hovertemplate='<b>%{x|%b %Y}</b><br>Revenue: $%{y:,.2f}<extra></extra>'
    )
    _apply_standard_figure_layout(
        trend_rev,
        'Revenue Over Time',
        xaxis=_monthly_time_axis(len(trend_data_rev)),
        yaxis=dict(tickformat='$,.0f', title='Revenue', automargin=True),
        height=380,
    )

    trend_data_rentals = build_complete_monthly_series(filtered_df, 'rental_id')
    trend_rentals = px.line(trend_data_rentals, x='year_month_dt', y='rental_id', 
                            title='Rentals Over Time', markers=True, color_discrete_sequence=['#2C353B'])
    trend_rentals.update_traces(
        line=dict(width=3, shape='spline'),
        marker=dict(size=6),
        hovertemplate='<b>%{x|%b %Y}</b><br>Rentals: %{y}<extra></extra>'
    )
    _apply_standard_figure_layout(
        trend_rentals,
        'Rentals Over Time',
        xaxis=_monthly_time_axis(len(trend_data_rentals)),
        yaxis=dict(tickformat=',.0f', title='Rentals', automargin=True),
        height=380,
    )

    trend_data_days = build_complete_monthly_series(filtered_df, 'rental_days')
    trend_days = px.line(trend_data_days, x='year_month_dt', y='rental_days',
                         title='Rental Days Over Time', markers=True, color_discrete_sequence=['#00708D'])
    trend_days.update_traces(
        line=dict(width=3, shape='spline'),
        marker=dict(size=6),
        hovertemplate='<b>%{x|%b %Y}</b><br>Rental Days: %{y:.2f}<extra></extra>'
    )
    _apply_standard_figure_layout(
        trend_days,
        'Rental Days Over Time',
        xaxis=_monthly_time_axis(len(trend_data_days)),
        yaxis=dict(tickformat='.2f', title='Days', automargin=True),
        height=380,
    )

    # Cumulative Performance (Month-to-Date Comparison)
    def _empty_cum_figure(title, y_title):
        fig = go.Figure()
        _apply_standard_figure_layout(
            fig,
            title,
            xaxis=dict(title='Day of Month', tickmode='array', tickvals=[1, 5, 10, 15, 20, 25, 30], tickangle=0, showgrid=False, automargin=True),
            yaxis=dict(title=y_title, showgrid=True, gridcolor='rgba(156,163,175,0.20)', zeroline=False, automargin=True),
            height=420,
            bottom_margin=64,
        )
        fig.update_layout(
            annotations=[dict(text='No data available for selected filters', x=0.5, y=0.5, xref='paper', yref='paper', showarrow=False, font=dict(color='#6b7280'))]
        )
        return fig

    def _build_cumulative_figure(cum_df, metric_col, title, y_title, value_format, projection_info=None, target_month=None, target_day=None):
        fig = go.Figure()
        if cum_df.empty:
            return _empty_cum_figure(title, y_title)

        if target_month is None:
            current_month = pd.to_datetime(cum_df['month_start']).max().to_period('M').to_timestamp()
        else:
            current_month = pd.Timestamp(target_month).to_period('M').to_timestamp()

        current_month_rows_all = cum_df[cum_df['month_start'] == current_month]
        if current_month_rows_all.empty:
            return _empty_cum_figure(title, y_title)

        if target_day is None:
            latest_day = int(current_month_rows_all['day_of_month'].max())
        else:
            latest_day = int(target_day)

        prev_month = (pd.Timestamp(current_month) - pd.DateOffset(months=1)).to_period('M').to_timestamp()
        same_month_last_year = (pd.Timestamp(current_month) - pd.DateOffset(years=1)).to_period('M').to_timestamp()

        comparison_months = [
            (current_month, 'Current Month', '#00708D', 4),
            (prev_month, 'Previous Month', '#94a3b8', 2.5),
            (same_month_last_year, 'Same Month LY', '#cbd5e1', 2.5),
        ]

        for month_start, label, color, width in comparison_months:
            month_slice = cum_df[(cum_df['month_start'] == month_start) & (cum_df['day_of_month'] <= latest_day)].copy()
            if month_slice.empty:
                continue

            if label == 'Current Month':
                month_slice = month_slice.sort_values('day_of_month')
                full_days = pd.DataFrame({'day_of_month': list(range(1, latest_day + 1))})
                month_slice = full_days.merge(
                    month_slice[['day_of_month', f'cum_{metric_col}']],
                    on='day_of_month',
                    how='left'
                )
                month_slice[f'cum_{metric_col}'] = month_slice[f'cum_{metric_col}'].ffill().fillna(0)

            month_name = pd.Timestamp(month_start).strftime('%b %Y')
            fig.add_trace(go.Scatter(
                x=month_slice['day_of_month'],
                y=month_slice[f'cum_{metric_col}'],
                customdata=[['Actual'] for _ in range(len(month_slice))],
                mode='lines+markers',
                name=label,
                line=dict(color=color, width=width),
                marker=dict(size=6 if label == 'Current Month' else 4),
                hovertemplate=f'<b>{label} ({month_name})</b><br>Day: %{{x}}<br>Value: {value_format}<extra></extra>'
            ))

        # Optional benchmark: average of last 6 months (excluding current month)
        prior_months = sorted([m for m in cum_df['month_start'].unique() if pd.Timestamp(m) < pd.Timestamp(current_month)])
        benchmark_months = prior_months[-6:]
        if benchmark_months:
            benchmark_slice = cum_df[(cum_df['month_start'].isin(benchmark_months)) & (cum_df['day_of_month'] <= latest_day)]
            if not benchmark_slice.empty:
                benchmark_line = benchmark_slice.groupby('day_of_month', as_index=False)[f'cum_{metric_col}'].mean()
                fig.add_trace(go.Scatter(
                    x=benchmark_line['day_of_month'],
                    y=benchmark_line[f'cum_{metric_col}'],
                    customdata=[['Benchmark'] for _ in range(len(benchmark_line))],
                    mode='lines',
                    name='Avg 6 Months',
                    line=dict(color='#d1d5db', width=2, dash='dot'),
                    hovertemplate=f'<b>Avg 6 Months</b><br>Day: %{{x}}<br>Value: {value_format}<extra></extra>'
                ))

        current_month_df = cum_df[(cum_df['month_start'] == current_month) & (cum_df['day_of_month'] <= latest_day)].copy()
        if (
            projection_info
            and projection_info.get('available')
            and not current_month_df.empty
            and projection_info.get('month_days', 0) > projection_info.get('current_day', 0)
        ):
            current_actual = float(projection_info['current_value'])
            current_day = int(projection_info['current_day'])
            total_days_current_month = int(projection_info['month_days'])
            base_projection = float(projection_info['base_projection'])
            adjusted_projection = float(projection_info['adjusted_projection'])

            fig.add_trace(go.Scatter(
                x=[current_day, total_days_current_month],
                y=[current_actual, adjusted_projection],
                mode='lines+markers',
                name='Adjusted Forecast',
                line=dict(color='#00708D', width=3.5, dash='dash'),
                marker=dict(size=7, symbol='diamond'),
                hovertemplate=(
                    f'<b>Adjusted Forecast</b><br>Day: %{{x}}<br>Value: {value_format}<extra></extra>'
                )
            ))

        prev_month_df = cum_df[cum_df['month_start'] == prev_month]
        if not prev_month_df.empty:
            prev_final = float(prev_month_df[f'cum_{metric_col}'].max())
            fig.add_hline(
                y=prev_final,
                line_dash='dot',
                line_color='#9ca3af',
                annotation_text=f"Last Month Final ({pd.Timestamp(prev_month).strftime('%b %Y')})",
                annotation_position='top left'
            )

        # Today marker
        fig.add_vline(
            x=latest_day,
            line_dash='dash',
            line_color='rgba(55,65,81,0.45)',
            annotation_text='Today',
            annotation_position='top'
        )

        _apply_standard_figure_layout(
            fig,
            title,
            xaxis=dict(
                title='Day of Month',
                tickmode='array',
                tickvals=[1, 5, 10, 15, 20, 25, 30],
                tickangle=0,
                showgrid=False,
                range=[1, 31],
                automargin=True,
            ),
            yaxis=dict(title=y_title, showgrid=True, gridcolor='rgba(156,163,175,0.20)', zeroline=False, automargin=True),
            height=420,
            hovermode='x',
            show_legend=True,
            legend_y=1.08,
            bottom_margin=64,
        )
        return fig

    def _build_mtd_summary(cum_df, metric_col, target_month=None, target_day=None):
        if cum_df.empty:
            return html.Span('MTD vs Prev: N/A', style={'color': '#6b7280'})

        if target_month is None:
            current_month = pd.to_datetime(cum_df['month_start']).max().to_period('M').to_timestamp()
        else:
            current_month = pd.Timestamp(target_month).to_period('M').to_timestamp()

        current_month_rows_all = cum_df[cum_df['month_start'] == current_month]
        if current_month_rows_all.empty:
            return html.Span('MTD vs Prev: N/A', style={'color': '#6b7280'})

        if target_day is None:
            latest_day = int(current_month_rows_all['day_of_month'].max())
        else:
            latest_day = int(target_day)

        prev_month = (pd.Timestamp(current_month) - pd.DateOffset(months=1)).to_period('M').to_timestamp()

        cur = cum_df[(cum_df['month_start'] == current_month) & (cum_df['day_of_month'] <= latest_day)]
        prv = cum_df[(cum_df['month_start'] == prev_month) & (cum_df['day_of_month'] <= latest_day)]
        if cur.empty or prv.empty:
            return html.Span('MTD vs Prev: N/A', style={'color': '#6b7280'})

        current_val = float(cur.sort_values('day_of_month')[f'cum_{metric_col}'].iloc[-1])
        prev_val = float(prv.sort_values('day_of_month')[f'cum_{metric_col}'].iloc[-1])
        if prev_val == 0:
            return html.Span('MTD vs Prev: N/A', style={'color': '#6b7280'})

        pct = ((current_val - prev_val) / prev_val) * 100
        if pct > 0:
            return html.Span(f'MTD vs Prev: ↑ {abs(pct):.1f}%', style={'color': '#198754'})
        if pct < 0:
            return html.Span(f'MTD vs Prev: ↓ {abs(pct):.1f}%', style={'color': '#dc3545'})
        return html.Span('MTD vs Prev: → 0.0%', style={'color': '#6b7280'})

    def _confidence_view(score):
        if score is None:
            return html.Span('Forecast Confidence: N/A', style={'color': '#6b7280'})
        if score >= 75:
            color = '#198754'
        elif score >= 55:
            color = '#f59e0b'
        else:
            color = '#dc3545'
        return html.Div([
            html.Span('Forecast Confidence: ', style={'color': '#374151', 'fontWeight': '600'}),
            html.Span(f'{score}%', style={'color': color, 'fontWeight': '700'})
        ])

    if cumulative_filtered_df.empty:
        projected_month_end_revenue = html.Span('Projection not available', style={'color': '#6b7280'})
        projected_month_end_rentals = html.Span('Projection not available', style={'color': '#6b7280'})
        projected_month_end_days = html.Span('Projection not available', style={'color': '#6b7280'})
        cum_revenue_summary = html.Span('MTD vs Prev: N/A', style={'color': '#6b7280'})
        cum_rentals_summary = html.Span('MTD vs Prev: N/A', style={'color': '#6b7280'})
        cum_days_summary = html.Span('MTD vs Prev: N/A', style={'color': '#6b7280'})
        cum_revenue_fig = _empty_cum_figure('Revenue (Cumulative by Month)', 'Revenue')
        cum_rentals_fig = _empty_cum_figure('Rentals (Cumulative by Month)', 'Rentals')
        cum_days_fig = _empty_cum_figure('Rental Days (Cumulative by Month)', 'Rental Days')
        cum_forecast_confidence = _confidence_view(None)
        cum_forecast_explanation = html.Span('Forecast explanation unavailable: no records match the selected filters.', style={'color': '#6b7280'})
        cum_reconciliation_warning = ''
        cum_reconciliation_warning_style = {'display': 'none'}
    else:
        daily_df = cumulative_filtered_df[['rental_id', 'rental_started_at_EST', 'revenue_amount', 'rental_days']].copy()
        daily_df['date'] = pd.to_datetime(daily_df['rental_started_at_EST']).dt.floor('D')
        daily_df = daily_df.dropna(subset=['date'])
        daily_df['month_start'] = daily_df['date'].dt.to_period('M').dt.to_timestamp()
        daily_df['day_of_month'] = daily_df['date'].dt.day

        daily_agg = daily_df.groupby(['month_start', 'date', 'day_of_month'], as_index=False).agg(
            revenue=('revenue_amount', 'sum'),
            rental_days=('rental_days', 'sum')
        ).sort_values(['month_start', 'date'])

        rental_first_occurrence = daily_df.dropna(subset=['rental_id']).copy()
        rental_first_occurrence = rental_first_occurrence.sort_values(['month_start', 'rental_id', 'date'])
        rental_first_occurrence = rental_first_occurrence.drop_duplicates(subset=['month_start', 'rental_id'], keep='first')
        rental_daily = rental_first_occurrence.groupby(['month_start', 'date', 'day_of_month'], as_index=False).agg(
            rentals=('rental_id', 'nunique')
        )
        daily_agg = daily_agg.merge(
            rental_daily,
            on=['month_start', 'date', 'day_of_month'],
            how='left'
        )
        daily_agg['rentals'] = daily_agg['rentals'].fillna(0)

        for metric in ['revenue', 'rentals', 'rental_days']:
            daily_agg[f'cum_{metric}'] = daily_agg.groupby('month_start')[metric].cumsum()

        current_month = pd.to_datetime(daily_agg['month_start']).max().to_period('M').to_timestamp()
        current_month_rows = daily_agg[daily_agg['month_start'] == current_month]
        latest_available_day = int(current_month_rows['day_of_month'].max()) if not current_month_rows.empty else 0
        now_ts = pd.Timestamp.now()
        now_month = now_ts.to_period('M').to_timestamp()
        current_month_days = int(pd.Timestamp(current_month).days_in_month)
        is_current_system_month = pd.Timestamp(current_month) == pd.Timestamp(now_month)
        if is_current_system_month:
            latest_day = min(int(now_ts.day), current_month_days)
        else:
            latest_day = latest_available_day

        if latest_day <= 0:
            latest_day = latest_available_day

        day_fraction = (
            now_ts.hour / 24
            + now_ts.minute / 1440
            + now_ts.second / 86400
        ) if is_current_system_month else 0.0

        if is_current_system_month:
            elapsed_days_effective = (latest_day - 1) + day_fraction
            elapsed_days_effective = max(max(latest_day - 1, 1), min(current_month_days, elapsed_days_effective))
        else:
            elapsed_days_effective = float(max(latest_day, 1))

        remaining_days_effective = max(current_month_days - elapsed_days_effective, 0.0)

        shared_current_metrics = shared_monthly_metrics_map.get(pd.Timestamp(current_month).strftime('%Y-%m'), {})
        shared_current_rentals = float(shared_current_metrics.get('rentals', 0)) if shared_current_metrics else 0.0
        shared_current_revenue = float(shared_current_metrics.get('revenue', 0)) if shared_current_metrics else 0.0
        shared_current_days = float(shared_current_metrics.get('rental_days', 0)) if shared_current_metrics else 0.0
        shared_current_distinct_rentals = int(shared_current_metrics.get('distinct_rental_id', 0)) if shared_current_metrics else 0
        shared_current_rows = int(shared_current_metrics.get('row_count', 0)) if shared_current_metrics else 0

        def _estimate_active_units(month_df):
            if month_df.empty or 'VIN' not in month_df.columns:
                return None
            vin_rows = month_df[['VIN', 'Status']].copy()
            vin_rows = vin_rows.dropna(subset=['VIN'])
            if vin_rows.empty:
                return None
            vin_rows['status_norm'] = vin_rows['Status'].fillna('').astype(str).str.strip().str.lower()
            status_filter = vin_rows['status_norm'].str.contains('onboard|available|active|in service', regex=True)
            filtered_vins = vin_rows.loc[status_filter, 'VIN'].astype(str).str.strip()
            if filtered_vins.empty:
                filtered_vins = vin_rows['VIN'].astype(str).str.strip()
            filtered_vins = filtered_vins[filtered_vins != '']
            if filtered_vins.empty:
                return None
            return int(filtered_vins.nunique())

        current_month_scope_df = cumulative_filtered_df[
            pd.to_datetime(cumulative_filtered_df['rental_started_at_EST']).dt.to_period('M').dt.to_timestamp() == current_month
        ]
        active_units = _estimate_active_units(current_month_scope_df)

        def _clip(value, lower_bound, upper_bound):
            return max(lower_bound, min(upper_bound, value))

        def _month_snapshot(metric_col, month_start, day_cutoff):
            rows = daily_agg[daily_agg['month_start'] == month_start].sort_values('day_of_month')
            if rows.empty:
                return None
            observed_rows = rows[rows['day_of_month'] <= day_cutoff]
            if observed_rows.empty:
                return None
            final_val = float(rows[f'cum_{metric_col}'].max())
            observed_val = float(observed_rows[f'cum_{metric_col}'].iloc[-1])
            days_in_month = int(pd.Timestamp(month_start).days_in_month)
            return {
                'final': final_val,
                'observed': observed_val,
                'days_in_month': days_in_month,
            }

        def _build_forecast(metric_col):
            current_snapshot = _month_snapshot(metric_col, current_month, latest_day)
            if not current_snapshot:
                return {'available': False}

            current_day = min(latest_day, current_month_days)
            current_value = current_snapshot['observed']
            if current_day <= 1 or current_value <= 0:
                return {'available': False}

            prior_months = sorted([m for m in daily_agg['month_start'].unique() if pd.Timestamp(m) < pd.Timestamp(current_month)])
            recent_months = prior_months[-6:]
            completion_ratios = []

            for month_start in recent_months:
                snapshot = _month_snapshot(metric_col, month_start, current_day)
                if not snapshot:
                    continue
                final_val = snapshot['final']
                observed_val = snapshot['observed']
                if final_val <= 0:
                    continue
                ratio = observed_val / final_val
                if ratio <= 0 or ratio >= 1.2:
                    continue
                completion_ratios.append(ratio)

            run_rate = current_value / max(elapsed_days_effective, 1)
            progress_ratio = current_day / max(current_month_days, 1)
            if progress_ratio > 0.75:
                damping_factor = 0.75
            elif progress_ratio > 0.45:
                damping_factor = 0.82
            else:
                damping_factor = 0.90

            base_projection = current_value + (run_rate * remaining_days_effective)
            adjusted_projection = current_value + (run_rate * remaining_days_effective * damping_factor)

            if active_units and active_units > 0 and remaining_days_effective > 0:
                current_per_unit_daily = run_rate / active_units
                metric_caps = {
                    'rentals': (0.03, 1.00),
                    'rental_days': (0.10, 4.50),
                    'revenue': (15.0, 600.0),
                }
                min_cap, max_cap = metric_caps.get(metric_col, (0.01, 1000.0))
                per_unit_daily_limit = _clip(current_per_unit_daily * 1.05, min_cap, max_cap)
                capacity_cap = current_value + (active_units * remaining_days_effective * per_unit_daily_limit)
            else:
                capacity_cap = adjusted_projection

            adjusted_projection = min(adjusted_projection, capacity_cap)
            adjusted_projection = max(adjusted_projection, current_value)

            history_score = float(_clip(len(completion_ratios) / 6.0, 0, 1))
            coverage_score = float(_clip(current_day / max(current_month_days, 1), 0, 1))
            if len(completion_ratios) >= 2:
                ratio_mean = float(pd.Series(completion_ratios).mean())
                ratio_std = float(pd.Series(completion_ratios).std(ddof=0))
                variability = (ratio_std / ratio_mean) if ratio_mean > 0 else 1.0
                stability_score = float(_clip(1 - (variability * 1.5), 0, 1))
            else:
                stability_score = 0.45
            if capacity_cap > 0:
                cap_pressure = float(_clip(1 - max((adjusted_projection - capacity_cap), 0) / capacity_cap, 0, 1))
            else:
                cap_pressure = 0.0

            confidence_score = int(round(100 * (
                0.35 * history_score +
                0.25 * coverage_score +
                0.25 * stability_score +
                0.15 * cap_pressure
            )))

            return {
                'available': True,
                'current_day': current_day,
                'month_days': current_month_days,
                'current_value': current_value,
                'base_projection': float(base_projection),
                'capacity_cap': float(capacity_cap),
                'damping_factor': float(damping_factor),
                'adjusted_projection': float(adjusted_projection),
                'confidence': int(_clip(confidence_score, 0, 100)),
                'history_months_used': len(completion_ratios),
            }

        def _previous_month_final(metric_col):
            prev_month = (pd.Timestamp(current_month) - pd.DateOffset(months=1)).to_period('M').to_timestamp()
            prev_rows = daily_agg[daily_agg['month_start'] == prev_month]
            if prev_rows.empty:
                return None
            return float(prev_rows[f'cum_{metric_col}'].max())

        def _format_projected_kpi(forecast_info, metric_col, is_currency=False):
            if not forecast_info.get('available'):
                return html.Span('Projection not available', style={'color': '#6b7280'})

            projected_value = float(forecast_info['adjusted_projection'])
            value_text = f"${projected_value:,.2f}" if is_currency else f"{projected_value:,.2f}"
            prev_final = _previous_month_final(metric_col)
            if prev_final is None or prev_final == 0:
                return html.Div([
                    html.Div(value_text),
                    html.Div('vs last month: N/A', style={'fontSize': '0.78rem', 'fontWeight': '600', 'color': '#6b7280', 'marginTop': '2px'})
                ], style={'lineHeight': '1.1'})

            delta_pct = ((projected_value - prev_final) / prev_final) * 100
            if delta_pct > 0:
                delta_text = f"↑ {abs(delta_pct):.1f}% vs last month"
                delta_color = '#198754'
            elif delta_pct < 0:
                delta_text = f"↓ {abs(delta_pct):.1f}% vs last month"
                delta_color = '#dc3545'
            else:
                delta_text = '→ 0.0% vs last month'
                delta_color = '#6b7280'

            return html.Div([
                html.Div(value_text),
                html.Div(delta_text, style={'fontSize': '0.78rem', 'fontWeight': '600', 'color': delta_color, 'marginTop': '2px'})
            ], style={'lineHeight': '1.1'})

        forecast_revenue = _build_forecast('revenue')
        forecast_rentals = _build_forecast('rentals')
        forecast_days = _build_forecast('rental_days')

        cumulative_current_rows = daily_agg[
            (daily_agg['month_start'] == current_month) & (daily_agg['day_of_month'] <= latest_day)
        ]
        cumulative_current_rentals = float(cumulative_current_rows['cum_rentals'].iloc[-1]) if not cumulative_current_rows.empty else 0.0

        projected_month_end_revenue = _format_projected_kpi(forecast_revenue, 'revenue', is_currency=True)
        projected_month_end_rentals = _format_projected_kpi(forecast_rentals, 'rentals', is_currency=False)
        projected_month_end_days = _format_projected_kpi(forecast_days, 'rental_days', is_currency=False)

        confidence_values = [
            metric_forecast['confidence']
            for metric_forecast in [forecast_revenue, forecast_rentals, forecast_days]
            if metric_forecast.get('available')
        ]
        overall_confidence = int(round(float(pd.Series(confidence_values).mean()))) if confidence_values else None
        cum_forecast_confidence = _confidence_view(overall_confidence)

        current_month_label = pd.Timestamp(current_month).strftime('%b %Y')
        if overall_confidence is None:
            cum_forecast_explanation = html.Span('Forecast explanation unavailable: insufficient month-to-date signal for current filters.', style={'color': '#6b7280'})
        else:
            cum_forecast_explanation = html.Div([
                html.Div(
                    'This forecast is based on current month run-rate (as of system today), remaining days, and capacity constraints. It applies a conservative adjustment to avoid overestimating end-of-month performance.',
                    style={'fontWeight': '600'}
                ),
                html.Div(
                    f"Confidence score reflects history depth, month progress, pattern stability, and capacity pressure (current: {overall_confidence}%).",
                    style={'marginTop': '4px'}
                )
            ])

        monthly_current_rentals_for_check = shared_current_rentals
        if abs(monthly_current_rentals_for_check - cumulative_current_rentals) > 0.01:
            cum_reconciliation_warning = 'Current-month metric mismatch detected between Monthly Comparison and Cumulative Performance'
            cum_reconciliation_warning_style = {'display': 'block'}
        else:
            cum_reconciliation_warning = ''
            cum_reconciliation_warning_style = {'display': 'none'}

        cum_revenue_summary = _build_mtd_summary(daily_agg, 'revenue', target_month=current_month, target_day=latest_day)
        cum_rentals_summary = _build_mtd_summary(daily_agg, 'rentals', target_month=current_month, target_day=latest_day)
        cum_days_summary = _build_mtd_summary(daily_agg, 'rental_days', target_month=current_month, target_day=latest_day)

        cum_revenue_fig = _build_cumulative_figure(
            daily_agg,
            metric_col='revenue',
            title='Revenue (Cumulative by Month)',
            y_title='Revenue',
            value_format='$%{y:,.2f}',
            projection_info=forecast_revenue,
            target_month=current_month,
            target_day=latest_day
        )
        cum_rentals_fig = _build_cumulative_figure(
            daily_agg,
            metric_col='rentals',
            title='Rentals (Cumulative by Month)',
            y_title='Rentals',
            value_format='%{y:,.2f}',
            projection_info=forecast_rentals,
            target_month=current_month,
            target_day=latest_day
        )
        cum_days_fig = _build_cumulative_figure(
            daily_agg,
            metric_col='rental_days',
            title='Rental Days (Cumulative by Month)',
            y_title='Rental Days',
            value_format='%{y:,.2f}',
            projection_info=forecast_days,
            target_month=current_month,
            target_day=latest_day
        )
    
    # Dealer table
    dealer_agg = tab_filtered_dealer_df.groupby('station_name').agg(
        total_revenue=('revenue_amount', 'sum'),
        rentals=('rental_id', 'count'),
        rental_days=('rental_days', 'sum'),
        vehicles=('VIN', lambda values: values.dropna().astype(str).str.strip().replace('', pd.NA).dropna().nunique()),
    ).reset_index()
    dealer_agg['vehicles'] = dealer_agg['vehicles'].fillna(0).astype(int)
    dealer_agg['avg_revenue'] = dealer_agg['total_revenue'] / dealer_agg['rentals']
    dealer_agg['avg_days'] = dealer_agg['rental_days'] / dealer_agg['rentals']
    dealer_agg = _append_dealer_branding(dealer_agg, 'station_name', prefix='dealer')
    
    # Vehicle performance (vehicle-level view using VIN as primary key)
    def _first_valid(series):
        valid = series.dropna()
        return valid.iloc[0] if not valid.empty else None

    vehicle_view_df = tab_filtered_vehicle_df.copy()
    vehicle_view_df['vehicle_key'] = vehicle_view_df['VIN'].astype(str)
    vehicle_view_df.loc[vehicle_view_df['VIN'].isna(), 'vehicle_key'] = '5VIN:' + vehicle_view_df['5VIN'].fillna('UNKNOWN').astype(str)

    vehicle_perf_df = vehicle_view_df.groupby('vehicle_key').agg(
        vehicle_id=('vehicle_id', _first_valid),
        license_plate_number=('license_plate_number', _first_valid),
        VIN=('VIN', _first_valid),
        **{'5VIN': ('5VIN', _first_valid)},
        vehicle_type=('vehicle_type', _first_valid),
        station_name=('station_name', _first_valid),
        Model=('Model', _first_valid),
        MY=('MY', _first_valid),
        Colour=('Colour', _first_valid),
        rentals=('rental_id', 'count'),
        rental_days=('rental_days', 'sum'),
        revenue=('revenue_amount', 'sum'),
        avg_kms=('kms_traveled', 'mean')
    ).reset_index(drop=True)

    vehicle_perf_df['avg_revenue'] = vehicle_perf_df['revenue'] / vehicle_perf_df['rentals']

    # Current mileage lookup is precomputed globally and refreshed on data reload.
    vehicle_perf_df = vehicle_perf_df.merge(vehicle_mileage_lookup, on='VIN', how='left')
    vehicle_perf_df['current_mileage'] = pd.to_numeric(vehicle_perf_df['current_mileage'], errors='coerce')
    vehicle_perf_df['MY'] = pd.to_numeric(vehicle_perf_df['MY'], errors='coerce')

    # Vehicle Performance tab only: exclude vehicles without valid 5VIN
    vehicle_perf_tab_df = vehicle_perf_df[
        vehicle_perf_df['5VIN'].notna() &
        (vehicle_perf_df['5VIN'].astype(str).str.strip() != '') &
        (vehicle_perf_df['5VIN'].astype(str).str.lower() != 'nan')
    ].copy()

    # Existing detailed vehicle table (kept as bottom section)
    vehicle_agg = vehicle_perf_tab_df[['vehicle_id', 'license_plate_number', 'VIN', '5VIN', 'vehicle_type', 'rentals', 'rental_days', 'revenue', 'avg_kms', 'avg_revenue']].copy()

    # Performance Index = vehicle utilization proxy / fleet average utilization proxy
    performance_df = vehicle_perf_tab_df.copy()
    if start_date and end_date:
        period_days = max((pd.to_datetime(end_date).normalize() - pd.to_datetime(start_date).normalize()).days + 1, 1)
    else:
        _min_dt = pd.to_datetime(filtered_df['rental_started_at_EST']).min()
        _max_dt = pd.to_datetime(filtered_df['rental_started_at_EST']).max()
        period_days = max((_max_dt.normalize() - _min_dt.normalize()).days + 1, 1) if pd.notna(_min_dt) and pd.notna(_max_dt) else 1

    performance_df['utilization_proxy'] = pd.to_numeric(performance_df['rental_days'], errors='coerce').fillna(0) / period_days
    if performance_df['utilization_proxy'].sum() <= 0:
        performance_df['utilization_proxy'] = pd.to_numeric(performance_df['rentals'], errors='coerce').fillna(0) / period_days
    if performance_df['utilization_proxy'].sum() <= 0:
        performance_df['utilization_proxy'] = pd.to_numeric(performance_df['revenue'], errors='coerce').fillna(0) / period_days

    fleet_avg_utilization = float(performance_df['utilization_proxy'].mean()) if not performance_df.empty else 0.0
    if fleet_avg_utilization > 0:
        performance_df['performance_index'] = performance_df['utilization_proxy'] / fleet_avg_utilization
    else:
        performance_df['performance_index'] = 1.0

    performance_df['performance_category'] = 'Normal'
    performance_df.loc[performance_df['performance_index'] > 1.2, 'performance_category'] = 'Overperforming'
    performance_df.loc[performance_df['performance_index'] < 0.8, 'performance_category'] = 'Underperforming'
    performance_df['display_5vin'] = performance_df['5VIN'].fillna(performance_df['VIN'].astype(str).str[-5:]).fillna('N/A')
    performance_df = _append_dealer_branding(performance_df, 'station_name', prefix='dealer')
    performance_df['label'] = performance_df['display_5vin'].astype(str) + ' - ' + performance_df['dealer_short'].astype(str)

    # Chart 1: Underperforming vehicles (Top 10 worst index)
    underperforming_df = performance_df[performance_df['performance_index'] < 0.8].copy()
    underperforming_df = underperforming_df.sort_values('performance_index', ascending=True).head(10)
    underperforming_df = underperforming_df.sort_values('performance_index', ascending=False)
    if not underperforming_df.empty:
        top10_fig = go.Figure(go.Bar(
            x=underperforming_df['performance_index'],
            y=underperforming_df['label'],
            orientation='h',
            marker=dict(color='#dc3545', opacity=0.88),
            text=[f"{v:.2f}x" for v in underperforming_df['performance_index']],
            textposition='outside',
            customdata=underperforming_df[['VIN', '5VIN', 'station_name', 'label', 'utilization_proxy']].values,
            hovertemplate='<b>%{y}</b><br>Performance Index: %{x:.2f}x<br>Dealer: %{customdata[2]}<br>Utilization proxy/day: %{customdata[4]:.3f}<br>VIN: %{customdata[0]}<extra></extra>'
        ))
        top10_fig.update_layout(
            title='Underperforming Vehicles (Top 10 Worst)',
            template='plotly_white',
            xaxis=dict(title='Performance Index (Fleet Avg = 1.0)', tickformat='.2f'),
            yaxis=dict(title='5VIN - Dealer'),
            margin=dict(l=10, r=10, t=45, b=10)
        )
    else:
        top10_fig = go.Figure()
        top10_fig.update_layout(
            template='plotly_white',
            title='Underperforming Vehicles (Top 10 Worst)',
            annotations=[dict(text='No underperforming vehicles (index < 0.8) for current filters.', x=0.5, y=0.5, showarrow=False)],
            xaxis={'visible': False}, yaxis={'visible': False}
        )

    # Chart 2: Top performing vehicles (Top 5 best index)
    top_performing_df = performance_df[performance_df['performance_index'] > 1.2].copy()
    top_performing_df = top_performing_df.sort_values('performance_index', ascending=False).head(5)
    top_performing_df = top_performing_df.sort_values('performance_index', ascending=True)
    if not top_performing_df.empty:
        mileage_scatter_fig = go.Figure(go.Bar(
            x=top_performing_df['performance_index'],
            y=top_performing_df['label'],
            orientation='h',
            marker=dict(color='#198754', opacity=0.88),
            text=[f"{v:.2f}x" for v in top_performing_df['performance_index']],
            textposition='outside',
            customdata=top_performing_df[['VIN', '5VIN', 'station_name', 'label', 'utilization_proxy']].values,
            hovertemplate='<b>%{y}</b><br>Performance Index: %{x:.2f}x<br>Dealer: %{customdata[2]}<br>Utilization proxy/day: %{customdata[4]:.3f}<br>VIN: %{customdata[0]}<extra></extra>'
        ))
        mileage_scatter_fig.update_layout(
            title='Top Performing Vehicles (Top 5)',
            template='plotly_white',
            xaxis=dict(title='Performance Index (Fleet Avg = 1.0)', tickformat='.2f'),
            yaxis=dict(title='5VIN - Dealer'),
            margin=dict(l=10, r=10, t=45, b=10)
        )
    else:
        mileage_scatter_fig = go.Figure()
        mileage_scatter_fig.update_layout(
            template='plotly_white',
            title='Top Performing Vehicles (Top 5)',
            annotations=[dict(text='No overperforming vehicles (index > 1.2) for current filters.', x=0.5, y=0.5, showarrow=False)],
            xaxis={'visible': False}, yaxis={'visible': False}
        )

    # High-mileage monitoring table and KPIs
    high_mileage_df = vehicle_perf_tab_df[vehicle_perf_tab_df['current_mileage'] > 15000].copy()
    high_mileage_df['mileage_status'] = high_mileage_df['current_mileage'].apply(lambda x: '⚠' if pd.notna(x) and x < 20000 else '⛔')
    high_mileage_df['MY'] = high_mileage_df['MY'].round(0)
    high_mileage_df['current_mileage'] = high_mileage_df['current_mileage'].round(0)
    high_mileage_df = high_mileage_df[['mileage_status', 'Model', 'Colour', 'MY', 'current_mileage', 'station_name', 'VIN']].sort_values('current_mileage', ascending=False)

    mileage_count_15000 = int(len(high_mileage_df))
    mileage_count_15_20 = int(len(high_mileage_df[(high_mileage_df['current_mileage'] >= 15000) & (high_mileage_df['current_mileage'] < 20000)]))
    mileage_count_20 = int(len(high_mileage_df[high_mileage_df['current_mileage'] >= 20000]))
    highest_mileage = int(round(float(vehicle_perf_tab_df['current_mileage'].max()))) if vehicle_perf_tab_df['current_mileage'].notna().any() else 0

    # Local Vehicle Performance tab filters (applied after global filters)
    filtered_vehicle_agg = vehicle_agg.copy()
    filtered_high_mileage_df = high_mileage_df.copy()

    _card_base = {'borderRadius': '12px', 'boxShadow': '0 2px 10px rgba(17,24,39,0.06)', 'height': '100%', 'transition': 'all 0.2s'}
    card_15000_style = {**_card_base, 'border': '1px solid #e5e7eb'}
    card_15_20_style = {**_card_base, 'border': '1px solid #f6d58b'}
    card_20_style    = {**_card_base, 'border': '1px solid #f0b2b2'}

    summary_parts = []
    any_local_filter = False

    # Step 1: mileage-band filter from KPI card clicks
    if selected_band == 'gt15000':
        # all vehicles >15000 already in filtered_high_mileage_df
        card_15000_style = {**_card_base, 'border': '2px solid #00708D',
                            'boxShadow': '0 4px 16px rgba(0,112,141,0.25)', 'backgroundColor': '#eaf6fa'}
        summary_parts.append('Mileage Filter: Vehicles > 15,000 km')
        any_local_filter = True
    elif selected_band == 'band15_20':
        filtered_high_mileage_df = filtered_high_mileage_df[
            (filtered_high_mileage_df['current_mileage'] >= 15000) &
            (filtered_high_mileage_df['current_mileage'] < 20000)
        ]
        card_15_20_style = {**_card_base, 'border': '2px solid #b7791f',
                            'boxShadow': '0 4px 16px rgba(183,121,31,0.25)', 'backgroundColor': '#fffbeb'}
        summary_parts.append('Mileage Filter: Vehicles 15,000–19,999 km')
        any_local_filter = True
    elif selected_band == 'ge20000':
        filtered_high_mileage_df = filtered_high_mileage_df[
            filtered_high_mileage_df['current_mileage'] >= 20000
        ]
        card_20_style = {**_card_base, 'border': '2px solid #c53030',
                         'boxShadow': '0 4px 16px rgba(197,48,48,0.25)', 'backgroundColor': '#fff5f5'}
        summary_parts.append('Mileage Filter: Vehicles >= 20,000 km')
        any_local_filter = True

    # Step 2: vehicle selection filter from chart bar clicks
    if selected_vehicle:
        sel_vin = selected_vehicle.get('VIN')
        sel_5vin = selected_vehicle.get('5VIN')
        sel_label = selected_vehicle.get('label', '') or ''
        sel_dealer = selected_vehicle.get('dealer', '') or ''
        veh_display = sel_label if sel_label else (sel_5vin or sel_vin or 'Unknown')
        if sel_dealer:
            summary_parts.append(f'Selected Vehicle: {veh_display} | {sel_dealer}')
        else:
            summary_parts.append(f'Selected Vehicle: {veh_display}')
        any_local_filter = True

        if sel_vin:
            filtered_vehicle_agg = filtered_vehicle_agg[
                filtered_vehicle_agg['VIN'].astype(str).str.strip() == str(sel_vin).strip()
            ]
            filtered_high_mileage_df = filtered_high_mileage_df[
                filtered_high_mileage_df['VIN'].astype(str).str.strip() == str(sel_vin).strip()
            ]
        elif sel_5vin:
            filtered_vehicle_agg = filtered_vehicle_agg[
                filtered_vehicle_agg['5VIN'].astype(str).str.strip() == str(sel_5vin).strip()
            ]
            matching_vins = vehicle_perf_tab_df[
                vehicle_perf_tab_df['5VIN'].astype(str).str.strip() == str(sel_5vin).strip()
            ]['VIN'].dropna().astype(str).str.strip().unique().tolist()
            if matching_vins:
                filtered_high_mileage_df = filtered_high_mileage_df[
                    filtered_high_mileage_df['VIN'].astype(str).str.strip().isin(matching_vins)
                ]
            else:
                filtered_high_mileage_df = filtered_high_mileage_df.iloc[0:0]

    # Build summary display
    if any_local_filter and summary_parts:
        from dash import html as _html
        selected_summary_children = [_html.Div(part, style={'lineHeight': '1.6'}) for part in summary_parts]
        selected_summary_style = {'margin': '4px 0 8px 0', 'color': '#1f2937', 'fontWeight': '700'}
    else:
        selected_summary_children = 'Click a KPI card or chart bar to filter.'
        selected_summary_style = {'margin': '4px 0 8px 0', 'color': '#6b7280', 'fontWeight': '500'}

    # Empty-state handling
    high_mileage_df = filtered_high_mileage_df
    vehicle_agg = filtered_vehicle_agg
    no_results = any_local_filter and len(high_mileage_df) == 0
    empty_state_style = (
        {'display': 'block', 'padding': '20px', 'textAlign': 'center', 'color': '#6b7280',
         'fontStyle': 'italic', 'border': '1px solid #e5e7eb', 'borderRadius': '8px', 'margin': '8px 0'}
        if no_results else {'display': 'none'}
    )

    high_mileage_data = high_mileage_df.to_dict('records')
    
    # Rental table (includes ongoing rentals)
    rental_table_df = tab_filtered_rental_df[[
        'rental_id', 'rental_started_at_EST', 'rental_end_datetime_EST', 'rental_status',
        'is_ongoing_rental', 'ongoing_risk_bucket', 'renter_name', 'station_name', 'Model',
        'license_plate_number', '5VIN', 'rental_days', 'kms_traveled', 'total_to_charge'
    ]].copy()
    rental_table_df['rental_started_at_EST'] = rental_table_df['rental_started_at_EST'].dt.strftime('%Y-%m-%d %H:%M')
    rental_table_df['rental_end_datetime_EST'] = rental_table_df['rental_end_datetime_EST'].dt.strftime('%Y-%m-%d %H:%M')
    rental_table_df['rental_end_datetime_EST'] = rental_table_df['rental_end_datetime_EST'].fillna('Ongoing')
    rental_table_df['ongoing_risk_bucket'] = rental_table_df['ongoing_risk_bucket'].replace({'None': ''})
    rental_table_df = rental_table_df.sort_values(by=['is_ongoing_rental', 'rental_days'], ascending=[False, False])
    rental_table_df = rental_table_df.drop(columns=['is_ongoing_rental'])
    rental_data = rental_table_df.to_dict('records')

    # Rental Details tab operational KPIs + charts
    rental_ops_df = tab_filtered_rental_df.dropna(subset=['rental_started_at_EST']).copy()
    rental_ops_df = rental_ops_df[rental_ops_df['rental_hours'].fillna(0) >= 0]

    if rental_ops_df.empty:
        rental_kpi_total_days = "0.00"
        rental_kpi_completed_days = "0.00"
        rental_kpi_ongoing_days = "0.00"
        rental_kpi_ongoing_count = "0"
        rental_kpi_avg_duration = "0.00 days"
        rental_kpi_long_ongoing = "0.0%"
        rental_insight = "No rentals available for the selected filters."

        rental_status_breakdown_fig = go.Figure()
        _apply_standard_figure_layout(
            rental_status_breakdown_fig,
            'Rentals Started by Day of Week',
            xaxis=dict(title=''),
            yaxis=dict(title='Rentals', automargin=True),
            height=360,
        )
        rental_active_trend_fig = go.Figure()
        _apply_standard_figure_layout(
            rental_active_trend_fig,
            'Rentals Started by Hour',
            xaxis=dict(title=''),
            yaxis=dict(title='Rentals', automargin=True),
            height=360,
        )
        rental_completed_ongoing_fig = go.Figure()
        _apply_standard_figure_layout(
            rental_completed_ongoing_fig,
            'Completed vs Ongoing Rentals',
            xaxis=dict(title=''),
            yaxis=dict(title='Rentals', automargin=True),
            height=360,
        )
    else:
        now_est = pd.Timestamp.now()
        completed_days_val = float(rental_ops_df.loc[~rental_ops_df['is_ongoing_rental'], 'rental_days'].sum())
        ongoing_days_val = float(rental_ops_df.loc[rental_ops_df['is_ongoing_rental'], 'rental_days'].sum())
        total_days_val = completed_days_val + ongoing_days_val
        ongoing_count_val = int(rental_ops_df['is_ongoing_rental'].sum())
        avg_duration_val = float(rental_ops_df['rental_days'].mean()) if len(rental_ops_df) else 0.0

        completed_rentals_df = rental_ops_df[
            ~rental_ops_df['is_ongoing_rental']
        ].copy()
        completed_rentals_df['scheduled_end_datetime_EST'] = pd.to_datetime(
            completed_rentals_df.get('scheduled_end_datetime_EST'),
            errors='coerce'
        )
        completed_rentals_df['rental_end_datetime_EST'] = pd.to_datetime(
            completed_rentals_df.get('rental_end_datetime_EST'),
            errors='coerce'
        )
        completed_rentals_df = completed_rentals_df.dropna(subset=['scheduled_end_datetime_EST', 'rental_end_datetime_EST'])

        late_tolerance = pd.Timedelta(hours=2)
        completed_rentals_df['return_delay_minutes'] = (
            (completed_rentals_df['rental_end_datetime_EST'] - completed_rentals_df['scheduled_end_datetime_EST'])
            .dt.total_seconds() / 60
        )
        late_returns_df = completed_rentals_df[
            completed_rentals_df['return_delay_minutes'] > (late_tolerance.total_seconds() / 60)
        ]
        late_return_pct = (
            (len(late_returns_df) / len(completed_rentals_df)) * 100
            if len(completed_rentals_df) else 0.0
        )
        avg_late_delay_days = float(late_returns_df['return_delay_minutes'].mean() / 1440) if not late_returns_df.empty else 0.0
        max_late_delay_days = float(late_returns_df['return_delay_minutes'].max() / 1440) if not late_returns_df.empty else 0.0

        rental_kpi_total_days = f"{total_days_val:,.2f}"
        rental_kpi_completed_days = f"{completed_days_val:,.2f}"
        rental_kpi_ongoing_days = f"{ongoing_days_val:,.2f}"
        rental_kpi_ongoing_count = f"{ongoing_count_val:,}"
        rental_kpi_avg_duration = f"{avg_duration_val:,.2f} days"
        rental_kpi_long_ongoing = f"{late_return_pct:.1f}%"

        rental_ops_df['rental_start_month'] = rental_ops_df['rental_started_at_EST'].dt.to_period('M').dt.to_timestamp()
        rental_ops_df = rental_ops_df.dropna(subset=['rental_start_month'])

        # Chart 1: Rentals Started by Day of Week (Mon -> Sun)
        weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        weekday_counts = (
            rental_ops_df.assign(day_of_week=rental_ops_df['rental_started_at_EST'].dt.day_name())
            .groupby('day_of_week', as_index=False)['rental_id']
            .nunique()
            .rename(columns={'rental_id': 'rentals'})
        )
        weekday_counts['day_of_week'] = pd.Categorical(weekday_counts['day_of_week'], categories=weekday_order, ordered=True)
        weekday_counts = weekday_counts.sort_values('day_of_week')

        rental_status_breakdown_fig = px.bar(
            weekday_counts,
            x='day_of_week',
            y='rentals',
            title='Rentals Started by Day of Week',
            color_discrete_sequence=['#00708D']
        ) if not weekday_counts.empty else go.Figure()
        if not weekday_counts.empty:
            rental_status_breakdown_fig.update_traces(
                hovertemplate='<b>%{x}</b><br>Rentals: %{y:,.0f}<extra></extra>'
            )
            _apply_standard_figure_layout(
                rental_status_breakdown_fig,
                'Rentals Started by Day of Week',
                xaxis=dict(title='', showgrid=False, tickangle=0, automargin=True),
                yaxis=dict(title='Rentals', tickformat=',.0f', automargin=True),
                height=360,
            )

        # Chart 2: Rentals Started by Hour of Day (0-23)
        hourly_counts = (
            rental_ops_df.assign(start_hour=rental_ops_df['rental_started_at_EST'].dt.hour)
            .groupby('start_hour', as_index=False)['rental_id']
            .nunique()
            .rename(columns={'rental_id': 'rentals'})
        )
        full_hours = pd.DataFrame({'start_hour': list(range(24))})
        hourly_counts = full_hours.merge(hourly_counts, on='start_hour', how='left').fillna({'rentals': 0})

        rental_active_trend_fig = px.bar(
            hourly_counts,
            x='start_hour',
            y='rentals',
            title='Rentals Started by Hour',
            color_discrete_sequence=['#2C353B']
        )
        rental_active_trend_fig.update_traces(
            hovertemplate='<b>Hour %{x:,.0f}</b><br>Rentals: %{y:,.0f}<extra></extra>'
        )
        _apply_standard_figure_layout(
            rental_active_trend_fig,
            'Rentals Started by Hour',
            xaxis=dict(title='Hour of Day', tickmode='linear', dtick=1, showgrid=False, automargin=True),
            yaxis=dict(title='Rentals', tickformat=',.0f', automargin=True),
            height=360,
        )

        # Chart 3: Completed vs Ongoing Rentals (single kept mix chart)
        monthly_status_counts = (
            rental_ops_df.groupby(['rental_start_month', 'rental_status'])['rental_id']
            .nunique()
            .reset_index(name='rentals')
        )
        if monthly_status_counts.empty:
            rental_completed_ongoing_fig = go.Figure()
            _apply_standard_figure_layout(
                rental_completed_ongoing_fig,
                'Completed vs Ongoing Rentals',
                xaxis=dict(title=''),
                yaxis=dict(title='Rentals', automargin=True),
                height=360,
            )
        else:
            monthly_wide = (
                monthly_status_counts
                .pivot(index='rental_start_month', columns='rental_status', values='rentals')
                .fillna(0)
                .reset_index()
            )
            if 'Completed' not in monthly_wide.columns:
                monthly_wide['Completed'] = 0.0
            if 'Ongoing' not in monthly_wide.columns:
                monthly_wide['Ongoing'] = 0.0

            rental_completed_ongoing_fig = go.Figure()
            rental_completed_ongoing_fig.add_trace(go.Bar(
                x=monthly_wide['rental_start_month'],
                y=monthly_wide['Completed'],
                name='Completed',
                marker=dict(color='#00708D'),
                hovertemplate='<b>%{x|%b %Y}</b><br>Completed: %{y:,.0f}<extra></extra>'
            ))
            rental_completed_ongoing_fig.add_trace(go.Bar(
                x=monthly_wide['rental_start_month'],
                y=monthly_wide['Ongoing'],
                name='Ongoing',
                marker=dict(color='#d4420b'),
                hovertemplate='<b>%{x|%b %Y}</b><br>Ongoing: %{y:,.0f}<extra></extra>'
            ))
            _apply_standard_figure_layout(
                rental_completed_ongoing_fig,
                'Completed vs Ongoing Rentals',
                xaxis=_monthly_time_axis(len(monthly_wide)),
                yaxis=dict(title='Rentals', tickformat=',.0f', automargin=True),
                height=360,
                show_legend=True,
                legend_y=1.08,
            )
            rental_completed_ongoing_fig.update_layout(barmode='stack')

        month_start = now_est.to_period('M').to_timestamp()
        month_end = month_start + pd.offsets.MonthEnd(1)
        month_mask = (
            (rental_ops_df['rental_started_at_EST'] <= month_end) &
            (rental_ops_df['effective_rental_end_datetime'] >= month_start)
        )
        month_days_total = float(rental_ops_df.loc[month_mask, 'rental_days'].sum())
        started_this_month_ongoing = int(
            (
                rental_ops_df['is_ongoing_rental'] &
                (rental_ops_df['rental_started_at_EST'] >= month_start) &
                (rental_ops_df['rental_started_at_EST'] <= month_end)
            ).sum()
        )
        carry_over_now = int(
            (
                rental_ops_df['is_ongoing_rental'] &
                (rental_ops_df['rental_started_at_EST'] < month_start) &
                (rental_ops_df['effective_rental_end_datetime'] >= month_start)
            ).sum()
        )
        rental_insight = (
            f"Current-month rental days (including ongoing): {month_days_total:,.2f}"
            f" | Started this month and still ongoing: {started_this_month_ongoing:,}"
            f" | Ongoing from previous months: {carry_over_now:,}"
            f" | Late return rate (>2h): {late_return_pct:.1f}%"
            f" | Avg late delay: {avg_late_delay_days:,.2f} days"
            f" | Max late delay: {max_late_delay_days:,.2f} days"
        )
    
    # Driver lifecycle & behavior analytics (customer_id-based)
    def _empty_driver_figure(title, y_title=''):
        fig = go.Figure()
        _apply_standard_figure_layout(
            fig,
            title,
            xaxis=dict(showgrid=False, title='', automargin=True),
            yaxis=dict(title=y_title, showgrid=True, gridcolor='rgba(156,163,175,0.20)', zeroline=False, automargin=True),
            height=360,
        )
        fig.update_layout(
            annotations=[dict(text='No data available for selected filters', x=0.5, y=0.5, xref='paper', yref='paper', showarrow=False, font=dict(color='#6b7280'))]
        )
        return fig

    filtered_driver_df = tab_filtered_driver_df.copy()

    if 'customer_id' in filtered_driver_df.columns:
        filtered_driver_df['customer_id'] = filtered_driver_df['customer_id'].astype(str).str.strip()
        filtered_driver_df.loc[filtered_driver_df['customer_id'].isin(['', 'nan', 'None']), 'customer_id'] = pd.NA
    else:
        filtered_driver_df['customer_id'] = pd.NA

    driver_renter_fallback = filtered_driver_df['renter_name'].astype('string').fillna('Unknown').astype(str)
    filtered_driver_df['customer_id'] = filtered_driver_df['customer_id'].fillna('RENTER:' + driver_renter_fallback)
    filtered_driver_df = filtered_driver_df.merge(driver_first_rental_lookup, on='customer_id', how='left')

    if filtered_driver_df.empty:
        driver_agg = pd.DataFrame(columns=['customer_id', 'renter_name', 'first_rental_date', 'tenure_bucket', 'driver_tenure_days', 'rentals', 'active_months', 'avg_days_between_rentals', 'rental_days', 'revenue', 'avg_duration', 'avg_revenue', 'avg_kms'])
        driver_top_table_data = []
        driver_kpi_total = '0.0%'
        driver_kpi_new = '0.0%'
        driver_kpi_new_pct = '0.0'
        driver_kpi_avg_tenure = '$0'
        driver_kpi_overall_tenure = '0'
        driver_kpi_inactive_pct = '0.0%'
        driver_insight = 'No driver activity for the selected filters and period.'
        driver_new_over_time_fig = _empty_driver_figure('New Drivers Over Time', 'Drivers')
        driver_active_vs_new_fig = _empty_driver_figure('Active vs New Drivers', 'Drivers')
        driver_tenure_bucket_fig = _empty_driver_figure('Driver Tenure Buckets', 'Drivers')
        driver_segment_fig = _empty_driver_figure('Driver Value Segmentation', 'Drivers')
        driver_cohort_fig = _empty_driver_figure('Driver Cohort Retention (%)', 'Retention %')
        driver_gap_fig = _empty_driver_figure('Avg Days Between Rentals', 'Days')
    else:
        latest_reference_date = pd.to_datetime(filtered_driver_df['rental_started_at_EST']).max().normalize()
        filtered_driver_df['driver_tenure_days'] = (latest_reference_date - pd.to_datetime(filtered_driver_df['first_rental_date']).dt.normalize()).dt.days.clip(lower=0)
        filtered_driver_df['tenure_bucket'] = pd.cut(
            filtered_driver_df['driver_tenure_days'],
            bins=[-1, 30, 90, 180, 10**9],
            labels=['New (0-30)', 'Early (31-90)', 'Mid (91-180)', 'Mature (180+)']
        )

        filtered_driver_df['rental_month'] = filtered_driver_df['rental_started_at_EST'].dt.to_period('M').dt.to_timestamp()
        filtered_driver_df = filtered_driver_df.sort_values(['customer_id', 'rental_started_at_EST'])
        filtered_driver_df['days_since_prev_rental'] = filtered_driver_df.groupby('customer_id')['rental_started_at_EST'].diff().dt.days

        active_months = filtered_driver_df.groupby('customer_id')['rental_month'].nunique().rename('active_months')
        avg_days_between = filtered_driver_df.groupby('customer_id')['days_since_prev_rental'].mean().rename('avg_days_between_rentals')

        driver_agg = filtered_driver_df.groupby('customer_id').agg(
            renter_name=('renter_name', 'first'),
            first_rental_date=('first_rental_date', 'first'),
            tenure_bucket=('tenure_bucket', 'first'),
            driver_tenure_days=('driver_tenure_days', 'first'),
            rentals=('rental_id', 'count'),
            rental_days=('rental_days', 'sum'),
            avg_duration=('rental_days', 'mean'),
            revenue=('revenue_amount', 'sum'),
            avg_revenue=('revenue_amount', 'mean'),
            avg_kms=('kms_traveled', 'mean')
        ).reset_index()

        driver_agg = driver_agg.merge(active_months, on='customer_id', how='left')
        driver_agg = driver_agg.merge(avg_days_between, on='customer_id', how='left')
        driver_agg['active_months'] = driver_agg['active_months'].fillna(1)
        driver_agg['avg_days_between_rentals'] = driver_agg['avg_days_between_rentals'].fillna(0)
        driver_agg['first_rental_date'] = pd.to_datetime(driver_agg['first_rental_date']).dt.strftime('%Y-%m-%d')
        driver_agg['tenure_bucket'] = driver_agg['tenure_bucket'].astype(str).replace('nan', 'Unknown')

        # First dealer and multi-dealer flag
        _sorted_rentals = filtered_driver_df.sort_values('rental_started_at_EST')
        _first_dealer = _sorted_rentals.groupby('customer_id')['station_name'].first().rename('first_dealer')
        _unique_dealers = _sorted_rentals.groupby('customer_id')['station_name'].nunique().rename('_n_dealers')
        driver_agg = driver_agg.merge(_first_dealer, on='customer_id', how='left')
        driver_agg = driver_agg.merge(_unique_dealers, on='customer_id', how='left')
        driver_agg['multi_dealer'] = driver_agg['_n_dealers'].apply(lambda x: 'Yes' if x > 1 else 'No')
        driver_agg.drop(columns=['_n_dealers'], inplace=True)

        period_start = pd.to_datetime(filtered_driver_df['rental_started_at_EST']).min().normalize()
        period_end = pd.to_datetime(filtered_driver_df['rental_started_at_EST']).max().normalize()

        first_dates_by_driver = filtered_driver_df[['customer_id', 'first_rental_date']].drop_duplicates('customer_id')
        new_drivers_period = first_dates_by_driver[
            (pd.to_datetime(first_dates_by_driver['first_rental_date']).dt.normalize() >= period_start) &
            (pd.to_datetime(first_dates_by_driver['first_rental_date']).dt.normalize() <= period_end)
        ]['customer_id'].nunique()

        active_window_days = 60
        active_window_cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=active_window_days)

        lifecycle_df = filtered_driver_df.dropna(subset=['rental_started_at_EST']).copy()
        driver_lifecycle = lifecycle_df.groupby('customer_id', as_index=False).agg(
            first_activity_date=('rental_started_at_EST', 'min'),
            last_activity_date=('rental_started_at_EST', 'max')
        )
        driver_lifecycle['is_active_window'] = pd.to_datetime(driver_lifecycle['last_activity_date']).dt.normalize() >= active_window_cutoff

        all_drivers_df = driver_lifecycle.copy()
        active_drivers_df = all_drivers_df[all_drivers_df['is_active_window']].copy()

        total_drivers = int(all_drivers_df['customer_id'].nunique()) if not all_drivers_df.empty else 0
        active_segment_count = int(active_drivers_df['customer_id'].nunique()) if not active_drivers_df.empty else 0
        dormant_segment_count = max(total_drivers - active_segment_count, 0)
        active_segment_pct = (active_segment_count / total_drivers * 100) if total_drivers else 0.0
        dormant_segment_pct = (dormant_segment_count / total_drivers * 100) if total_drivers else 0.0

        total_driver_rentals = float(pd.to_numeric(driver_agg['rentals'], errors='coerce').fillna(0).sum()) if not driver_agg.empty else 0.0
        total_driver_revenue = float(pd.to_numeric(driver_agg['revenue'], errors='coerce').fillna(0).sum()) if not driver_agg.empty else 0.0
        rentals_per_active_driver = (total_driver_rentals / active_segment_count) if active_segment_count else 0.0
        revenue_per_active_driver = (total_driver_revenue / active_segment_count) if active_segment_count else 0.0

        period_start_ts = pd.to_datetime(period_start).normalize()
        period_end_ts = pd.to_datetime(period_end).normalize()
        first_activity_in_filtered = filtered_driver_df.groupby('customer_id', as_index=False)['rental_started_at_EST'].min()
        new_driver_ids = first_activity_in_filtered[
            (pd.to_datetime(first_activity_in_filtered['rental_started_at_EST']).dt.normalize() >= period_start_ts) &
            (pd.to_datetime(first_activity_in_filtered['rental_started_at_EST']).dt.normalize() <= period_end_ts)
        ]['customer_id']

        new_segment_count = int(new_driver_ids.nunique())
        new_segment_pct = (new_segment_count / total_drivers * 100) if total_drivers else 0.0

        top_share = 0.0
        if total_driver_revenue > 0 and not driver_agg.empty:
            top_share = float(driver_agg.sort_values('revenue', ascending=False).head(10)['revenue'].sum() / total_driver_revenue * 100)

        def _kpi_color(value, healthy_threshold, warning_threshold, higher_is_better=True):
            if higher_is_better:
                if value >= healthy_threshold:
                    return '#198754'
                if value >= warning_threshold:
                    return '#f59e0b'
                return '#dc3545'
            if value >= healthy_threshold:
                return '#dc3545'
            if value >= warning_threshold:
                return '#f59e0b'
            return '#198754'

        driver_kpi_total = html.Span(f"{active_segment_pct:.1f}%", style={'color': _kpi_color(active_segment_pct, 65, 45, True), 'fontWeight': '700'})
        driver_kpi_new = html.Span(f"{dormant_segment_pct:.1f}%", style={'color': _kpi_color(dormant_segment_pct, 40, 25, False), 'fontWeight': '700'})
        driver_kpi_new_pct = f"{rentals_per_active_driver:,.1f}"
        driver_kpi_avg_tenure = f"${revenue_per_active_driver:,.0f}"
        driver_kpi_overall_tenure = f"{new_segment_count:,}"
        driver_kpi_inactive_pct = html.Span(f"{top_share:.1f}%", style={'color': _kpi_color(top_share, 45, 30, False), 'fontWeight': '700'})

        # New drivers over time
        new_monthly = (
            first_dates_by_driver.assign(first_rental_month=pd.to_datetime(first_dates_by_driver['first_rental_date']).dt.to_period('M').dt.to_timestamp())
            .groupby('first_rental_month', as_index=False)['customer_id']
            .nunique()
            .rename(columns={'customer_id': 'new_drivers'})
            .sort_values('first_rental_month')
        )

        driver_new_over_time_fig = px.bar(
            new_monthly,
            x='first_rental_month',
            y='new_drivers',
            title='New Drivers Over Time',
            color_discrete_sequence=['#00708D']
        ) if not new_monthly.empty else _empty_driver_figure('New Drivers Over Time', 'Drivers')

        if not new_monthly.empty:
            _apply_standard_figure_layout(
                driver_new_over_time_fig,
                'New Drivers Over Time',
                xaxis=_monthly_time_axis(len(new_monthly)),
                yaxis=dict(title='Drivers', tickformat=',.0f', automargin=True),
                height=360,
            )

        # Active vs new drivers
        active_monthly = (
            filtered_driver_df.groupby('rental_month', as_index=False)['customer_id']
            .nunique()
            .rename(columns={'customer_id': 'active_drivers'})
            .sort_values('rental_month')
        )
        active_new_monthly = active_monthly.merge(
            new_monthly.rename(columns={'first_rental_month': 'rental_month'}),
            on='rental_month', how='left'
        ) if not active_monthly.empty else pd.DataFrame(columns=['rental_month', 'active_drivers', 'new_drivers'])
        active_new_monthly['new_drivers'] = active_new_monthly.get('new_drivers', 0).fillna(0)

        if active_new_monthly.empty:
            driver_active_vs_new_fig = _empty_driver_figure('Active vs New Drivers', 'Drivers')
        else:
            driver_active_vs_new_fig = go.Figure()
            driver_active_vs_new_fig.add_trace(go.Scatter(
                x=active_new_monthly['rental_month'], y=active_new_monthly['active_drivers'],
                mode='lines+markers', name='Active Drivers', line=dict(color='#2C353B', width=3)
            ))
            driver_active_vs_new_fig.add_trace(go.Scatter(
                x=active_new_monthly['rental_month'], y=active_new_monthly['new_drivers'],
                mode='lines+markers', name='New Drivers', line=dict(color='#00708D', width=3, dash='dash')
            ))
            _apply_standard_figure_layout(
                driver_active_vs_new_fig,
                'Active vs New Drivers',
                xaxis=_monthly_time_axis(len(active_new_monthly)),
                yaxis=dict(title='Drivers', tickformat=',.0f', automargin=True),
                height=360,
                show_legend=True,
                legend_y=1.08,
            )

        # Tenure bucket distribution
        tenure_bucket_df = (
            driver_agg.groupby('tenure_bucket', as_index=False)['customer_id']
            .count()
            .rename(columns={'customer_id': 'drivers'})
        )
        tenure_order = ['New (0-30)', 'Early (31-90)', 'Mid (91-180)', 'Mature (180+)']
        tenure_bucket_df['tenure_bucket'] = pd.Categorical(tenure_bucket_df['tenure_bucket'], categories=tenure_order, ordered=True)
        tenure_bucket_df = tenure_bucket_df.sort_values('tenure_bucket')
        driver_tenure_bucket_fig = px.bar(
            tenure_bucket_df,
            x='tenure_bucket', y='drivers',
            title='Driver Tenure Buckets',
            color_discrete_sequence=['#00708D']
        ) if not tenure_bucket_df.empty else _empty_driver_figure('Driver Tenure Buckets', 'Drivers')
        if not tenure_bucket_df.empty:
            _apply_standard_figure_layout(
                driver_tenure_bucket_fig,
                'Driver Tenure Buckets',
                xaxis=dict(showgrid=False, title='', tickangle=0, automargin=True),
                yaxis=dict(title='Drivers', tickformat=',.0f', automargin=True),
                height=360,
            )

        # Driver frequency mix insight
        total_segment_revenue = float(driver_agg['revenue'].sum()) if not driver_agg.empty else 0.0
        frequency_df = driver_agg[['customer_id', 'rentals', 'revenue']].copy()
        frequency_df['rentals'] = pd.to_numeric(frequency_df['rentals'], errors='coerce').fillna(0)
        frequency_df['bucket'] = pd.cut(
            frequency_df['rentals'],
            bins=[-0.01, 1, 3, 6, 10**9],
            labels=['1 rental', '2-3 rentals', '4-6 rentals', '7+ rentals']
        )

        frequency_summary = (
            frequency_df.groupby('bucket', as_index=False)
            .agg(drivers=('customer_id', 'count'), revenue=('revenue', 'sum'))
        )
        frequency_order = ['1 rental', '2-3 rentals', '4-6 rentals', '7+ rentals']
        frequency_summary['bucket'] = pd.Categorical(frequency_summary['bucket'], categories=frequency_order, ordered=True)
        frequency_summary = frequency_summary.sort_values('bucket')
        frequency_summary['revenue_share'] = ((frequency_summary['revenue'] / total_segment_revenue) * 100).fillna(0) if total_segment_revenue else 0

        driver_segment_fig = go.Figure()
        if not frequency_summary.empty:
            driver_segment_fig.add_trace(go.Bar(
                x=frequency_summary['bucket'],
                y=frequency_summary['drivers'],
                name='Drivers',
                marker_color='#00708D',
                hovertemplate='<b>%{x}</b><br>Drivers: %{y:,.0f}<extra></extra>'
            ))
            driver_segment_fig.add_trace(go.Scatter(
                x=frequency_summary['bucket'],
                y=frequency_summary['revenue_share'],
                name='Revenue Share %',
                mode='lines+markers',
                yaxis='y2',
                line=dict(color='#2C353B', width=2.5),
                marker=dict(size=7),
                hovertemplate='<b>%{x}</b><br>Revenue Share: %{y:.1f}%<extra></extra>'
            ))
            _apply_standard_figure_layout(
                driver_segment_fig,
                'Driver Frequency Mix (Rentals per Driver)',
                xaxis=dict(showgrid=False, title='Rental Frequency Bucket', tickangle=0, automargin=True),
                yaxis=dict(title='Drivers', tickformat=',.0f', automargin=True),
                height=360,
                show_legend=True,
                legend_y=1.08,
            )
            driver_segment_fig.update_layout(
                yaxis2=dict(title='Revenue Share %', overlaying='y', side='right', tickformat='.1f', range=[0, 100], automargin=True)
            )
        else:
            driver_segment_fig = _empty_driver_figure('Driver Frequency Mix (Rentals per Driver)', 'Drivers')

        # Cohort heatmap
        first_month_per_driver = filtered_driver_df.groupby('customer_id')['first_rental_date'].first().dt.to_period('M').dt.to_timestamp()
        cohort_activity = filtered_driver_df[['customer_id', 'rental_month']].drop_duplicates().copy()
        cohort_activity['first_rental_month'] = cohort_activity['customer_id'].map(first_month_per_driver)
        cohort_activity['cohort_index'] = (
            (cohort_activity['rental_month'].dt.year - cohort_activity['first_rental_month'].dt.year) * 12 +
            (cohort_activity['rental_month'].dt.month - cohort_activity['first_rental_month'].dt.month)
        )
        cohort_activity = cohort_activity[cohort_activity['cohort_index'] >= 0]

        cohort_sizes = cohort_activity[cohort_activity['cohort_index'] == 0].groupby('first_rental_month')['customer_id'].nunique()
        cohort_counts = cohort_activity.groupby(['first_rental_month', 'cohort_index'])['customer_id'].nunique().reset_index(name='drivers')
        cohort_counts['cohort_size'] = cohort_counts['first_rental_month'].map(cohort_sizes)
        cohort_counts['retention_pct'] = (cohort_counts['drivers'] / cohort_counts['cohort_size'] * 100).fillna(0)
        cohort_counts = cohort_counts[cohort_counts['cohort_index'] <= 12]

        if cohort_counts.empty:
            driver_cohort_fig = _empty_driver_figure('Driver Cohort Retention (%)', 'Retention %')
        else:
            cohort_pivot = cohort_counts.pivot(index='first_rental_month', columns='cohort_index', values='retention_pct').fillna(0)
            cohort_y = [pd.Timestamp(idx).strftime('%Y-%m') for idx in cohort_pivot.index]
            driver_cohort_fig = go.Figure(data=go.Heatmap(
                z=cohort_pivot.values,
                x=[f'M+{int(c)}' for c in cohort_pivot.columns],
                y=cohort_y,
                colorscale='Blues',
                colorbar=dict(title='Retention %'),
                hovertemplate='Cohort: %{y}<br>Offset: %{x}<br>Retention: %{z:.1f}%<extra></extra>'
            ))
            _apply_standard_figure_layout(
                driver_cohort_fig,
                'Driver Cohort Retention (%)',
                xaxis=dict(showgrid=False, title='Months Since First Rental', automargin=True),
                yaxis=dict(showgrid=False, title='Cohort Month', automargin=True),
                height=430,
                hovermode='closest',
            )

        # Engagement recency distribution
        gap_source = pd.to_numeric(driver_agg['avg_days_between_rentals'], errors='coerce').dropna()
        if gap_source.empty:
            driver_gap_fig = _empty_driver_figure('Rental Recency Distribution', 'Drivers')
        else:
            recency_df = pd.DataFrame({'days_between': gap_source})
            recency_df['bucket'] = pd.cut(
                recency_df['days_between'],
                bins=[-0.01, 7, 14, 30, 60, 10**9],
                labels=['0-7 days', '8-14 days', '15-30 days', '31-60 days', '60+ days']
            )
            recency_summary = (
                recency_df.groupby('bucket', as_index=False)
                .size()
                .rename(columns={'size': 'drivers'})
            )
            recency_summary['bucket'] = pd.Categorical(
                recency_summary['bucket'],
                categories=['0-7 days', '8-14 days', '15-30 days', '31-60 days', '60+ days'],
                ordered=True
            )
            recency_summary = recency_summary.sort_values('bucket')

            median_gap = float(gap_source.median())
            p75_gap = float(gap_source.quantile(0.75))
            p90_gap = float(gap_source.quantile(0.90))

            driver_gap_fig = px.bar(
                recency_summary,
                x='bucket',
                y='drivers',
                title='Rental Recency Distribution',
                color_discrete_sequence=['#00708D']
            )
            driver_gap_fig.update_traces(
                hovertemplate='<b>%{x}</b><br>Drivers: %{y:,.0f}<extra></extra>'
            )
            _apply_standard_figure_layout(
                driver_gap_fig,
                'Rental Recency Distribution',
                xaxis=dict(title='Avg Days Between Rentals (Bucket)', tickangle=0, showgrid=False, automargin=True),
                yaxis=dict(title='Drivers', tickformat=',.0f', automargin=True),
                height=360,
            )
            driver_gap_fig.add_annotation(
                text=f"Median: {median_gap:.1f}d | P75: {p75_gap:.1f}d | P90: {p90_gap:.1f}d",
                xref='paper', yref='paper', x=0.99, y=1.18,
                xanchor='right', yanchor='bottom',
                showarrow=False,
                font=dict(size=11, color='#4b5563')
            )

        # Top drivers table
        top_driver_df = driver_agg.sort_values(['rentals', 'revenue'], ascending=[False, False]).head(20).copy()
        driver_top_table_data = top_driver_df[
            ['customer_id', 'renter_name', 'first_rental_date', 'driver_tenure_days',
             'first_dealer', 'multi_dealer', 'rentals', 'revenue', 'rental_days']
        ].to_dict('records')

        if active_segment_pct >= 65:
            engagement_level = 'High engagement'
        elif active_segment_pct >= 45:
            engagement_level = 'Moderate engagement'
        else:
            engagement_level = 'Low engagement'

        if top_share >= 45:
            risk_line = 'Key risk: high concentration in top drivers; diversify usage across the base.'
        elif dormant_segment_pct >= 50:
            risk_line = 'Main opportunity: reactivate dormant drivers to unlock growth.'
        else:
            risk_line = 'Risk/opportunity is balanced; focus on steady reactivation and concentration control.'

        line_1 = f"{engagement_level}: {active_segment_pct:.1f}% active vs {dormant_segment_pct:.1f}% dormant (last 60 days)."
        line_2 = f"Productivity is {rentals_per_active_driver:,.1f} rentals per active driver; new drivers (period): {new_segment_count:,}."
        line_3 = risk_line

        driver_insight = html.Div([
            html.Div(line_1),
            html.Div(line_2),
            html.Div(line_3),
        ])
    

    # Monthly Comparison (Executive view)
    if active_tab != 'monthly':
        monthly_content = dash.no_update
    elif cumulative_filtered_df.empty:
        monthly_content = dbc.Alert("No data available for the selected filters", color="info")
    else:
        monthly_scope_ignore_global_month = monthly_scope_ignore_global_month_df.copy()
        monthly_scope_with_global_month = filtered_df.copy()

        if start_date and end_date:
            scope_start_label = pd.to_datetime(start_date).strftime('%Y-%m-%d')
            scope_end_label = pd.to_datetime(end_date).strftime('%Y-%m-%d')
        else:
            scope_dates = pd.to_datetime(
                monthly_scope_ignore_global_month['effective_rental_end_datetime'].fillna(
                    monthly_scope_ignore_global_month['rental_started_at_EST']
                )
            ).dropna()
            if scope_dates.empty:
                scope_start_label = 'N/A'
                scope_end_label = 'N/A'
            else:
                scope_start_label = scope_dates.min().strftime('%Y-%m-%d')
                scope_end_label = scope_dates.max().strftime('%Y-%m-%d')

        target_month = None
        selected_month_message = None
        selected_comparison_month = comparison_month.strip() if isinstance(comparison_month, str) else comparison_month

        if selected_comparison_month:
            available_target_months = set(monthly_scope_ignore_global_month['year_month'].dropna().unique())
            if selected_comparison_month not in available_target_months:
                monthly_content = html.Div([
                    dbc.Alert("Selected comparison month is outside the current global date filter", color="warning"),
                    html.Div(f"Target comparison month: {selected_comparison_month}", className='monthly-context-line'),
                    html.Div(f"Global date scope: [{scope_start_label}] to [{scope_end_label}]", className='monthly-context-line')
                ])
            else:
                target_month = selected_comparison_month
                selected_month_message = f"Using locally selected comparison month: {target_month}"
        else:
            if monthly_scope_with_global_month.empty:
                monthly_content = dbc.Alert("No valid month data available after applying global time filters", color="info")
            else:
                latest_month = monthly_scope_with_global_month['year_month_dt'].max()
                if pd.isna(latest_month):
                    monthly_content = dbc.Alert("No valid month data available after applying global time filters", color="info")
                else:
                    target_month = latest_month.strftime('%Y-%m')
                    selected_month_message = f"Using latest available month within current global filters: {target_month}"

        if 'monthly_content' not in locals():
            comparison_month = target_month
            current_dt = pd.to_datetime(f"{comparison_month}-01", errors='coerce')
            if pd.isna(current_dt):
                monthly_content = dbc.Alert("Invalid month selected for comparison", color="warning")
            else:
                prev_dt = current_dt - pd.DateOffset(months=1)
                prev_month_str = prev_dt.strftime('%Y-%m')
                same_month_last_year_str = (current_dt - pd.DateOffset(years=1)).strftime('%Y-%m')

                if selected_comparison_month:
                    monthly_scope_for_metrics = monthly_scope_ignore_global_month.copy()
                else:
                    monthly_scope_for_metrics = monthly_scope_with_global_month.copy()

                comparison_df = monthly_scope_for_metrics.copy()

                monthly_metrics_map_filtered, _ = _build_monthly_metrics_map(monthly_scope_for_metrics)
                monthly_metrics_map_comparison, _ = _build_monthly_metrics_map(comparison_df)

                filtered_driver_base = monthly_scope_for_metrics[['customer_id', 'rental_started_at_EST']].copy()
                filtered_driver_base = filtered_driver_base.dropna(subset=['customer_id', 'rental_started_at_EST'])
                filtered_driver_base['first_month'] = (
                    pd.to_datetime(filtered_driver_base['rental_started_at_EST'])
                    .dt.to_period('M')
                    .dt.to_timestamp()
                )
                first_month_per_driver = (
                    filtered_driver_base
                    .groupby('customer_id', as_index=True)['first_month']
                    .min()
                )
                new_drivers_monthly = (
                    first_month_per_driver
                    .dt.strftime('%Y-%m')
                    .value_counts()
                    .rename_axis('year_month')
                    .reset_index(name='new_drivers')
                )
                new_drivers_map = dict(zip(new_drivers_monthly['year_month'], new_drivers_monthly['new_drivers']))
                
                # Also build full-history new drivers map for YoY comparison
                comparison_driver_base = comparison_df[['customer_id', 'rental_started_at_EST']].copy()
                comparison_driver_base = comparison_driver_base.dropna(subset=['customer_id', 'rental_started_at_EST'])
                comparison_driver_base['first_month'] = (
                    pd.to_datetime(comparison_driver_base['rental_started_at_EST'])
                    .dt.to_period('M')
                    .dt.to_timestamp()
                )
                comparison_first_month_per_driver = (
                    comparison_driver_base
                    .groupby('customer_id', as_index=True)['first_month']
                    .min()
                )
                comparison_new_drivers_monthly = (
                    comparison_first_month_per_driver
                    .dt.strftime('%Y-%m')
                    .value_counts()
                    .rename_axis('year_month')
                    .reset_index(name='new_drivers')
                )
                comparison_new_drivers_map = dict(zip(comparison_new_drivers_monthly['year_month'], comparison_new_drivers_monthly['new_drivers']))

                def get_metric(as_month, metric, use_comparison_df=False):
                    if as_month is None:
                        return None
                    search_df = comparison_df if use_comparison_df else monthly_scope_for_metrics
                    search_new_drivers_map = comparison_new_drivers_map if use_comparison_df else new_drivers_map
                    search_metrics_map = monthly_metrics_map_comparison if use_comparison_df else monthly_metrics_map_filtered
                    metric_row = search_metrics_map.get(as_month)
                    if metric == 'rental_days':
                        return float(metric_row['rental_days']) if metric_row else None
                    if metric == 'rentals':
                        return float(metric_row['rentals']) if metric_row else None
                    if metric == 'revenue':
                        return float(metric_row['revenue']) if metric_row else None
                    if metric == 'new_drivers':
                        subset = search_df[search_df['year_month'] == as_month]
                        if subset.empty:
                            return None
                        return float(search_new_drivers_map.get(as_month, 0))
                    return None

                def format_value(value, is_currency=False, is_count=False):
                    if value is None or pd.isna(value):
                        return 'N/A'
                    if is_count:
                        return f"{value:,.0f}"
                    return f"${value:,.2f}" if is_currency else f"{value:,.2f}"

                def compute_change(curr, base):
                    if curr is None or base is None:
                        return None, None, 'neutral', 'N/A', '#6c757d', 'N/A'
                    diff = curr - base
                    if base == 0:
                        pct = None
                    else:
                        pct = (diff / base) * 100

                    if diff > 0:
                        direction = 'up'
                        arrow = '↑'
                        color = '#198754'
                    elif diff < 0:
                        direction = 'down'
                        arrow = '↓'
                        color = '#dc3545'
                    else:
                        direction = 'flat'
                        arrow = '→'
                        color = '#6c757d'

                    pct_text = 'N/A' if pct is None else f"{pct:+.2f}%"
                    return diff, pct, direction, arrow, color, pct_text

                metric_specs = [
                    ('rental_days', 'Rental Days', False),
                    ('revenue', 'Revenue', True),
                    ('rentals', 'Rentals', False),
                    ('new_drivers', 'New Drivers', False),
                ]

                metric_cards = []
                mom_candidates = []

                current_label = current_dt.strftime('%B %Y')
                prev_label = prev_dt.strftime('%B %Y')
                yoy_label = (current_dt - pd.DateOffset(years=1)).strftime('%B %Y')

                metric_results = {}

                for metric_key, metric_label, is_currency in metric_specs:
                    current_val = get_metric(comparison_month, metric_key)
                    prev_val = get_metric(prev_month_str, metric_key)
                    yoy_val = get_metric(same_month_last_year_str, metric_key, use_comparison_df=True)
                    is_count_metric = metric_key in {'rentals', 'new_drivers'}

                    mom_diff, mom_pct, _, mom_arrow, mom_color, mom_pct_text = compute_change(current_val, prev_val)
                    yoy_diff, yoy_pct, _, yoy_arrow, yoy_color, yoy_pct_text = compute_change(current_val, yoy_val)

                    metric_results[metric_key] = {
                        'label': metric_label,
                        'current': current_val,
                        'mom_diff': mom_diff,
                        'mom_pct': mom_pct,
                        'is_currency': is_currency,
                        'is_count': is_count_metric,
                    }

                    if mom_pct is not None:
                        mom_candidates.append((metric_key, abs(mom_pct)))

                    if mom_diff is None:
                        mom_diff_text = 'N/A'
                    elif is_currency:
                        mom_diff_text = f"${mom_diff:+,.2f}"
                    elif is_count_metric:
                        mom_diff_text = f"{mom_diff:+,.0f}"
                    else:
                        mom_diff_text = f"{mom_diff:+,.2f}"

                    if yoy_diff is None:
                        yoy_diff_text = 'N/A'
                    elif is_currency:
                        yoy_diff_text = f"${yoy_diff:+,.2f}"
                    elif is_count_metric:
                        yoy_diff_text = f"{yoy_diff:+,.0f}"
                    else:
                        yoy_diff_text = f"{yoy_diff:+,.2f}"

                    metric_cards.append((metric_key, dbc.Card(
                        dbc.CardBody([
                            html.Div(metric_label, className='kpi-label'),
                            html.Div(format_value(current_val, is_currency, is_count_metric), className='kpi-value'),
                            html.Div(f"{mom_arrow} {mom_pct_text} vs last month", style={'color': mom_color, 'fontWeight': '600', 'fontSize': '0.92rem', 'marginTop': '8px', 'minHeight': '22px'}),
                            html.Div(f"Diff: {mom_diff_text}", style={'color': '#6b7280', 'fontSize': '0.85rem', 'minHeight': '20px'}),
                            html.Div(f"{yoy_arrow} {yoy_pct_text} vs last year", style={'color': yoy_color, 'fontWeight': '600', 'fontSize': '0.92rem', 'marginTop': '10px', 'minHeight': '22px'}),
                            html.Div(f"Diff: {yoy_diff_text}", style={'color': '#6b7280', 'fontSize': '0.85rem', 'minHeight': '20px'}),
                        ], style={'textAlign': 'center', 'display': 'flex', 'flexDirection': 'column', 'justifyContent': 'center', 'alignItems': 'center'}),
                        className='kpi-card dashboard-kpi-card'
                    )))

                critical_metric_key = None
                if mom_candidates:
                    critical_metric_key = sorted(mom_candidates, key=lambda x: x[1], reverse=True)[0][0]

                metric_card_map = {metric_key: card for metric_key, card in metric_cards}
                ordered_metric_keys = [metric_key for metric_key, _, _ in metric_specs]

                critical_title = 'Most Critical Change'
                if critical_metric_key:
                    critical_title = f"Most Critical Change: {metric_results[critical_metric_key]['label']}"

                styled_cards = []
                for metric_key in ordered_metric_keys:
                    card = metric_card_map[metric_key]
                    is_critical = metric_key == critical_metric_key
                    card_node = card
                    if is_critical:
                        card_node = html.Div(
                            card,
                            className='monthly-critical-wrapper'
                        )
                    styled_cards.append(
                        dbc.Col([
                            card_node
                        ], xs=12, md=6, xl=3, className=f"dashboard-kpi-col monthly-kpi-col {'monthly-kpi-critical-col' if is_critical else ''}")
                    )

                def _comparison_series_specs(primary_color):
                    if primary_color == '#2C353B':
                        return [
                            ('Current Month', '#2C353B', 'solid', 3.2, 8),
                            ('Previous Month', '#6e7680', 'solid', 2.4, 6),
                            ('Last Year', '#c7cdd3', 'dash', 2.2, 5),
                        ]
                    return [
                        ('Current Month', '#00708D', 'solid', 3.2, 8),
                        ('Previous Month', '#59AFC0', 'solid', 2.4, 6),
                        ('Last Year', '#B9D6DD', 'dash', 2.2, 5),
                    ]

                def build_comparison_trend_figure(metric_key, title, primary_color):
                    fig = go.Figure()
                    if monthly_scope_for_metrics.empty:
                        return fig

                    event_cols = ['rental_id', 'rental_started_at_EST', 'rental_days', 'revenue_amount']
                    if metric_key == 'new_drivers':
                        event_cols.append('customer_id')

                    event_df = monthly_scope_for_metrics[event_cols].copy()
                    event_df['event_date'] = pd.to_datetime(event_df['rental_started_at_EST']).dt.floor('D')
                    event_df['month_start'] = event_df['event_date'].dt.to_period('M').dt.to_timestamp()
                    event_df['day_of_month'] = event_df['event_date'].dt.day

                    if metric_key == 'rentals':
                        rental_daily = event_df.dropna(subset=['rental_id']).copy()
                        rental_daily = rental_daily.sort_values(['month_start', 'rental_id', 'event_date'])
                        rental_daily = rental_daily.drop_duplicates(subset=['month_start', 'rental_id'], keep='first')
                        daily = rental_daily.groupby(['month_start', 'day_of_month'], as_index=False).agg(metric=('rental_id', 'nunique'))
                    elif metric_key == 'rental_days':
                        daily = event_df.groupby(['month_start', 'day_of_month'], as_index=False)['rental_days'].sum().rename(columns={'rental_days': 'metric'})
                    elif metric_key == 'revenue':
                        daily = event_df.groupby(['month_start', 'day_of_month'], as_index=False)['revenue_amount'].sum().rename(columns={'revenue_amount': 'metric'})
                    elif metric_key == 'new_drivers':
                        if 'customer_id' not in event_df.columns:
                            return fig
                        first_events = (
                            event_df.dropna(subset=['customer_id'])
                            .groupby('customer_id', as_index=False)['event_date']
                            .min()
                        )
                        if first_events.empty:
                            return fig
                        first_events['month_start'] = first_events['event_date'].dt.to_period('M').dt.to_timestamp()
                        first_events['day_of_month'] = first_events['event_date'].dt.day
                        daily = first_events.groupby(['month_start', 'day_of_month'], as_index=False).size().rename(columns={'size': 'metric'})
                    else:
                        return fig

                    if daily.empty:
                        return fig

                    daily = daily.sort_values(['month_start', 'day_of_month'])
                    daily['cum_metric'] = daily.groupby('month_start')['metric'].cumsum()

                    series_specs = _comparison_series_specs(primary_color)
                    month_series = [
                        (current_dt, *series_specs[0]),
                        (prev_dt, *series_specs[1]),
                        (current_dt - pd.DateOffset(years=1), *series_specs[2]),
                    ]

                    if metric_key == 'revenue':
                        hover_value = '$%{y:,.2f}'
                        y_tick = '$,.0f'
                        y_title = 'Revenue'
                    elif metric_key in {'rentals', 'new_drivers'}:
                        hover_value = '%{y:,.0f}'
                        y_tick = ',.0f'
                        y_title = 'Count'
                    else:
                        hover_value = '%{y:,.2f}'
                        y_tick = '.2f'
                        y_title = 'Days'

                    found_trace = False
                    for month_ts, label, color, dash_style, width, marker_size in month_series:
                        month_start = pd.Timestamp(month_ts).to_period('M').to_timestamp()
                        month_slice = daily[daily['month_start'] == month_start].copy()
                        if month_slice.empty:
                            continue

                        if label == 'Current Month':
                            today_ts = pd.Timestamp.now().normalize()
                            today_month = today_ts.to_period('M').to_timestamp()
                            if month_start == today_month:
                                target_day = min(int(today_ts.day), int(pd.Timestamp(month_start).days_in_month))
                                last_observed_day = int(month_slice['day_of_month'].max())
                                if target_day > last_observed_day:
                                    last_value = float(month_slice.sort_values('day_of_month')['cum_metric'].iloc[-1])
                                    extension_days = pd.DataFrame({
                                        'day_of_month': list(range(last_observed_day + 1, target_day + 1)),
                                        'cum_metric': last_value,
                                    })
                                    month_slice = pd.concat([
                                        month_slice[['day_of_month', 'cum_metric']],
                                        extension_days,
                                    ], ignore_index=True)
                                else:
                                    month_slice = month_slice[['day_of_month', 'cum_metric']].copy()
                            else:
                                month_slice = month_slice[['day_of_month', 'cum_metric']].copy()
                        else:
                            month_slice = month_slice[['day_of_month', 'cum_metric']].copy()

                        marker_config = dict(size=marker_size, color=color)
                        if label == 'Current Month':
                            cum_diff = month_slice['cum_metric'].diff()
                            marker_sizes = [marker_size if index == 0 or pd.notna(delta) and delta > 0 else 0
                                            for index, delta in enumerate(cum_diff)]
                            marker_config = dict(size=marker_sizes, color=color)

                        found_trace = True
                        fig.add_trace(go.Scatter(
                            x=month_slice['day_of_month'],
                            y=month_slice['cum_metric'],
                            mode='lines+markers',
                            name=label,
                            line=dict(color=color, width=width, dash=dash_style),
                            marker=marker_config,
                            hovertemplate=f'<b>{label}</b><br>Day: %{{x}}<br>Value: {hover_value}<extra></extra>'
                        ))

                    if not found_trace:
                        return fig

                    _apply_standard_figure_layout(
                        fig,
                        title,
                        xaxis=dict(title='Day of Month', tickmode='array', tickvals=[1, 5, 10, 15, 20, 25, 30], showgrid=False, automargin=True),
                        yaxis=dict(showgrid=True, zeroline=False, tickformat=y_tick, title=y_title, automargin=True),
                        height=380,
                        show_legend=True,
                        legend_y=1.08,
                    )
                    return fig

                days_trend_fig = build_comparison_trend_figure('rental_days', 'Rental Days Comparison', '#00708D')
                revenue_trend_fig = build_comparison_trend_figure('revenue', 'Revenue Comparison', '#2C353B')
                rentals_trend_fig = build_comparison_trend_figure('rentals', 'Rentals Comparison', '#00708D')
                new_drivers_trend_fig = build_comparison_trend_figure('new_drivers', 'New Drivers Comparison', '#2C353B')

                insight_lines = []
                if critical_metric_key is None:
                    insight_lines.append('Insufficient prior-period data to compute month-over-month changes for the selected filters.')
                else:
                    critical = metric_results[critical_metric_key]
                    if critical['mom_pct'] is not None and critical['mom_diff'] is not None:
                        direction_text = 'increased' if critical['mom_diff'] > 0 else ('decreased' if critical['mom_diff'] < 0 else 'remained stable')
                        if critical['is_currency']:
                            diff_text = f"${critical['mom_diff']:+,.2f}"
                        elif critical['is_count']:
                            diff_text = f"{critical['mom_diff']:+,.0f}"
                        else:
                            diff_text = f"{critical['mom_diff']:+,.2f}"
                        insight_lines.append(
                            f"{critical['label']} {direction_text} month-over-month by {critical['mom_pct']:+.2f}% ({diff_text}), which is the most critical shift this period."
                        )

                revenue_mom = metric_results['revenue']['mom_diff']
                rentals_mom = metric_results['rentals']['mom_diff']
                if revenue_mom is not None and rentals_mom is not None:
                    if revenue_mom < 0 and rentals_mom < 0:
                        insight_lines.append('Revenue decline is aligned with lower rental volume, indicating softer utilization this month.')
                    elif revenue_mom > 0 and rentals_mom > 0:
                        insight_lines.append('Revenue growth tracks higher rental volume, suggesting stronger utilization this month.')

                context_header = f"{current_label} vs {prev_label} and {yoy_label}"
                target_month_debug = f"Target comparison month: {comparison_month}"
                global_scope_debug = f"Global date scope: [{scope_start_label}] to [{scope_end_label}]"

                monthly_content = html.Div([
                    html.Div([
                        html.Div('Monthly Performance Story', className='section-subtitle'),
                        html.Div(target_month_debug, className='monthly-context-line'),
                        html.Div(global_scope_debug, className='monthly-context-line'),
                        html.Div(context_header, className='monthly-context-line'),
                        html.Div(selected_month_message or '', className='monthly-selected-note', style={'display': 'block' if selected_month_message else 'none'}),
                    ], className='monthly-story-header-card'),

                    html.Div(critical_title, className='critical-change-title'),

                    dbc.Row(styled_cards, className='g-3 dashboard-kpi-row overview-kpi-row monthly-kpi-row'),

                    dbc.Row([
                        dbc.Col(dcc.Graph(figure=days_trend_fig, className='dashboard-graph', config={'displayModeBar': False, 'responsive': True}), xs=12, xl=3, className='dashboard-graph-col'),
                        dbc.Col(dcc.Graph(figure=revenue_trend_fig, className='dashboard-graph', config={'displayModeBar': False, 'responsive': True}), xs=12, xl=3, className='dashboard-graph-col'),
                        dbc.Col(dcc.Graph(figure=rentals_trend_fig, className='dashboard-graph', config={'displayModeBar': False, 'responsive': True}), xs=12, xl=3, className='dashboard-graph-col'),
                        dbc.Col(dcc.Graph(figure=new_drivers_trend_fig, className='dashboard-graph', config={'displayModeBar': False, 'responsive': True}), xs=12, xl=3, className='dashboard-graph-col'),
                    ], className='g-3 dashboard-chart-row monthly-chart-grid'),

                    html.Div([
                        html.Div('Key Insights', className='monthly-insight-title'),
                        html.Div(' '.join(insight_lines), className='monthly-insight-body')
                    ], className='monthly-insight-card')
                ], className='monthly-story-card monthly-comparison-shell')
    
    if active_tab == 'dealer' and not dealer_agg.empty:
        # Fleet Efficiency: Revenue per Vehicle
        dealer_efficiency_data = dealer_agg.copy()
        dealer_efficiency_data['revenue_per_vehicle'] = dealer_efficiency_data['total_revenue'] / dealer_efficiency_data['vehicles'].replace(0, 1)
        dealer_efficiency_data = dealer_efficiency_data.sort_values('revenue_per_vehicle', ascending=False)
        dealer_revenue_per_vehicle_fig = px.bar(
            dealer_efficiency_data, x='station_name', y='revenue_per_vehicle',
            title='Revenue per Vehicle (Fleet Efficiency)',
            color_discrete_sequence=['#00708D']
        )
        dealer_revenue_per_vehicle_fig.update_traces(
            text=[f"${v:,.0f}" for v in dealer_efficiency_data['revenue_per_vehicle']],
            textposition="outside", textfont=dict(size=9, color='#00708D'),
            hovertemplate='<b>%{x}</b><br>Revenue per Vehicle: $%{y:,.0f}<extra></extra>'
        )
        dealer_revenue_per_vehicle_fig.update_layout(
            template='plotly_white', yaxis=dict(tickformat='$,.0f', title='Revenue per Vehicle'),
            xaxis=dict(showgrid=False, title=''), margin=dict(l=10, r=10, t=45, b=64)
        )

        # Fleet Efficiency: Rentals per Vehicle
        dealer_efficiency_data['rentals_per_vehicle'] = dealer_efficiency_data['rentals'] / dealer_efficiency_data['vehicles'].replace(0, 1)
        dealer_efficiency_data = dealer_efficiency_data.sort_values('rentals_per_vehicle', ascending=False)
        dealer_rentals_per_vehicle_fig = px.bar(
            dealer_efficiency_data, x='station_name', y='rentals_per_vehicle',
            title='Rentals per Vehicle (Fleet Utilization)',
            color_discrete_sequence=['#f59e0b']
        )
        dealer_rentals_per_vehicle_fig.update_traces(
            text=[f"{v:.1f}" for v in dealer_efficiency_data['rentals_per_vehicle']],
            textposition="outside", textfont=dict(size=9, color='#f59e0b'),
            hovertemplate='<b>%{x}</b><br>Rentals per Vehicle: %{y:.1f}<extra></extra>'
        )
        dealer_rentals_per_vehicle_fig.update_layout(
            template='plotly_white', yaxis=dict(tickformat='.1f', title='Rentals per Vehicle'),
            xaxis=dict(showgrid=False, title=''), margin=dict(l=10, r=10, t=45, b=64)
        )

        # Fleet Composition: Number of Vehicles per Dealer
        dealer_vehicle_count_data = dealer_agg.sort_values('vehicles', ascending=False)[['station_name', 'vehicles', 'dealer_name']]
        dealer_num_vehicles_fig = px.bar(
            dealer_vehicle_count_data, x='station_name', y='vehicles',
            title='Number of Vehicles per Dealer',
            color_discrete_sequence=['#10b981']
        )
        dealer_num_vehicles_fig.update_traces(
            text=[f"{int(v)}" for v in dealer_vehicle_count_data['vehicles']],
            textposition="outside", textfont=dict(size=9, color='#10b981'),
            hovertemplate='<b>%{x}</b><br>Vehicles: %{y:.0f}<extra></extra>'
        )
        dealer_num_vehicles_fig.update_layout(
            template='plotly_white', yaxis=dict(tickformat=',d', title='Number of Vehicles'),
            xaxis=dict(showgrid=False, title=''), margin=dict(l=10, r=10, t=45, b=64)
        )

        # Fleet Composition: Vehicle Mix by Dealer (Stacked by Type)
        vehicle_mix_data = filtered_df.groupby(['station_name', 'vehicle_type']).agg(
            vehicle_count=('VIN', lambda x: x.dropna().astype(str).str.strip().replace('', pd.NA).dropna().nunique())
        ).reset_index()
        vehicle_mix_pivot = vehicle_mix_data.pivot(index='station_name', columns='vehicle_type', values='vehicle_count').fillna(0)
        vehicle_mix_pivot = vehicle_mix_pivot.reindex(dealer_agg['station_name']).fillna(0)
        dealer_vehicle_mix_fig = go.Figure()
        for vtype in vehicle_mix_pivot.columns:
            dealer_vehicle_mix_fig.add_trace(go.Bar(
                x=vehicle_mix_pivot.index, y=vehicle_mix_pivot[vtype], name=str(vtype),
                hovertemplate='<b>%{x}</b><br>' + str(vtype) + ': %{y:.0f}<extra></extra>'
            ))
        dealer_vehicle_mix_fig.update_layout(
            title='Vehicle Mix by Dealer (Stacked by Type)', barmode='stack',
            template='plotly_white', yaxis=dict(tickformat=',d', title='Number of Vehicles'),
            xaxis=dict(showgrid=False, title=''), margin=dict(l=10, r=10, t=45, b=64)
        )

        # Model Performance: Mirai by Dealer
        mirai_source_col = None
        for candidate_col in ['vehicle_model', 'vehicle_type', 'Vehicle']:
            if candidate_col in filtered_df.columns:
                mirai_source_col = candidate_col
                break
        if mirai_source_col is not None:
            mirai_df = filtered_df[
                filtered_df[mirai_source_col].astype(str).str.contains('Mirai', case=False, na=False)
            ]
        else:
            mirai_df = filtered_df.iloc[0:0].copy()

        if not mirai_df.empty:
            mirai_data = mirai_df.groupby('station_name').agg(
                mirai_rentals=('rental_id', 'count'),
                mirai_revenue=('revenue_amount', 'sum'),
                mirai_vehicles=('VIN', lambda x: x.dropna().astype(str).str.strip().replace('', pd.NA).dropna().nunique())
            ).reset_index()
            mirai_data['mirai_utilization'] = mirai_data['mirai_rentals'] / mirai_data['mirai_vehicles'].replace(0, 1)
            mirai_data = mirai_data.sort_values('mirai_revenue', ascending=False)
            dealer_mirai_performance_fig = go.Figure()
            dealer_mirai_performance_fig.add_trace(go.Bar(
                x=mirai_data['station_name'], y=mirai_data['mirai_rentals'], name='Rentals',
                marker_color='#06b6d4', hovertemplate='<b>%{x}</b><br>Rentals: %{y:.0f}<extra></extra>'
            ))
            dealer_mirai_performance_fig.add_trace(go.Scatter(
                x=mirai_data['station_name'], y=mirai_data['mirai_revenue'], name='Revenue',
                yaxis='y2', mode='lines+markers', line=dict(color='#8b5cf6', width=3),
                hovertemplate='<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra></extra>'
            ))
            dealer_mirai_performance_fig.update_layout(
                title='Mirai Performance by Dealer (Rentals & Revenue)',
                template='plotly_white', hovermode='x unified',
                yaxis=dict(title='Rentals', tickformat=',d'),
                yaxis2=dict(title='Revenue ($)', overlaying='y', side='right', tickformat='$,.0f'),
                xaxis=dict(showgrid=False, title=''), margin=dict(l=10, r=10, t=45, b=64)
            )
        else:
            dealer_mirai_performance_fig = go.Figure()

        # Driver Quality: Repeat Driver Rate (by dealer)
        driver_id_col = 'customer_id' if 'customer_id' in filtered_df.columns else 'renter_name'
        driver_rentals_by_dealer = filtered_df.groupby(['station_name', driver_id_col]).agg(
            driver_rentals=('rental_id', 'count')
        ).reset_index()
        driver_summary_by_dealer = driver_rentals_by_dealer.groupby('station_name').agg(
            repeat_drivers=('driver_rentals', lambda x: (x >= 2).sum()),
            total_drivers=(driver_id_col, 'nunique')
        ).reset_index()
        driver_summary_by_dealer['repeat_driver_pct'] = (driver_summary_by_dealer['repeat_drivers'] / driver_summary_by_dealer['total_drivers'].replace(0, 1)) * 100
        driver_summary_by_dealer = driver_summary_by_dealer.sort_values('repeat_driver_pct', ascending=False)
        dealer_repeat_driver_rate_fig = px.bar(
            driver_summary_by_dealer, x='station_name', y='repeat_driver_pct',
            title='Repeat Driver Rate (% with 2+ rentals)',
            color_discrete_sequence=['#ec4899']
        )
        dealer_repeat_driver_rate_fig.update_traces(
            text=[f"{v:.1f}%" for v in driver_summary_by_dealer['repeat_driver_pct']],
            textposition="outside", textfont=dict(size=9, color='#ec4899'),
            hovertemplate='<b>%{x}</b><br>Repeat Driver Rate: %{y:.1f}%<extra></extra>'
        )
        dealer_repeat_driver_rate_fig.update_layout(
            template='plotly_white', yaxis=dict(tickformat='.1f', title='Repeat Driver %'),
            xaxis=dict(showgrid=False, title=''), margin=dict(l=10, r=10, t=45, b=64)
        )

        # Driver Quality: Rentals per Driver (by dealer)
        rentals_per_driver_df = (
            driver_rentals_by_dealer.groupby('station_name', as_index=False)['driver_rentals']
            .mean()
            .rename(columns={'driver_rentals': 'rentals_per_driver'})
        )
        driver_summary_by_dealer = driver_summary_by_dealer.merge(rentals_per_driver_df, on='station_name', how='left')
        driver_summary_by_dealer['rentals_per_driver'] = driver_summary_by_dealer['rentals_per_driver'].fillna(0)

        rentals_per_driver_plot = driver_summary_by_dealer.sort_values('rentals_per_driver', ascending=False)
        dealer_rentals_per_driver_fig = px.bar(
            rentals_per_driver_plot,
            x='station_name', y='rentals_per_driver',
            title='Rentals per Driver (Engagement Metric)',
            color_discrete_sequence=['#8b5cf6']
        )
        dealer_rentals_per_driver_fig.update_traces(
            text=[f"{v:.1f}" for v in rentals_per_driver_plot['rentals_per_driver']],
            textposition="outside", textfont=dict(size=9, color='#8b5cf6'),
            hovertemplate='<b>%{x}</b><br>Rentals per Driver: %{y:.1f}<extra></extra>'
        )
        dealer_rentals_per_driver_fig.update_layout(
            template='plotly_white', yaxis=dict(tickformat='.1f', title='Rentals per Driver'),
            xaxis=dict(showgrid=False, title=''), margin=dict(l=10, r=10, t=45, b=64)
        )

        # Efficiency Scatter Plot
        scatter_data = dealer_efficiency_data.copy()
        dealer_efficiency_scatter_fig = px.scatter(
            scatter_data, x='rentals_per_vehicle', y='revenue_per_vehicle', size='vehicles',
            hover_name='station_name', color='station_name',
            title='Dealer Efficiency Matrix (Rentals vs Revenue per Vehicle)',
            labels={'rentals_per_vehicle': 'Rentals per Vehicle', 'revenue_per_vehicle': 'Revenue per Vehicle ($)'}
        )
        dealer_efficiency_scatter_fig.update_layout(
            template='plotly_white', showlegend=False,
            xaxis=dict(title='Rentals per Vehicle', showgrid=True),
            yaxis=dict(title='Revenue per Vehicle ($)', tickformat='$,.0f', showgrid=True),
            margin=dict(l=10, r=10, t=45, b=64), hovermode='closest'
        )
    else:
        dealer_revenue_per_vehicle_fig = go.Figure()
        dealer_rentals_per_vehicle_fig = go.Figure()
        dealer_num_vehicles_fig = go.Figure()
        dealer_vehicle_mix_fig = go.Figure()
        dealer_mirai_performance_fig = go.Figure()
        dealer_repeat_driver_rate_fig = go.Figure()
        dealer_rentals_per_driver_fig = go.Figure()
        dealer_efficiency_scatter_fig = go.Figure()

    result = (f"${total_rev:,.0f}", f"{total_rentals:,.0f}", f"{total_days:,.0f}", f"${avg_rev:.2f}", f"{total_kms:,.0f}", f"{avg_kms:,.0f}",
            trend_rev, trend_rentals, trend_days,
            projected_month_end_revenue, projected_month_end_rentals, projected_month_end_days,
            cum_revenue_summary, cum_rentals_summary, cum_days_summary,
            cum_revenue_fig, cum_rentals_fig, cum_days_fig,
            cum_forecast_confidence, cum_forecast_explanation,
            cum_reconciliation_warning, cum_reconciliation_warning_style,
            dealer_agg.to_dict('records'), vehicle_agg.to_dict('records'),
            top10_fig, mileage_scatter_fig,
            f"{mileage_count_15000:,}", f"{mileage_count_15_20:,}", f"{mileage_count_20:,}", f"{highest_mileage:,}",
            selected_summary_children, selected_summary_style,
            empty_state_style, card_15000_style, card_15_20_style, card_20_style,
            high_mileage_data,
            rental_data,
            rental_kpi_total_days, rental_kpi_completed_days, rental_kpi_ongoing_days,
            rental_kpi_ongoing_count, rental_kpi_avg_duration, rental_kpi_long_ongoing,
            rental_insight, rental_status_breakdown_fig, rental_active_trend_fig, rental_completed_ongoing_fig,
            driver_agg.to_dict('records'),
            driver_kpi_total, driver_kpi_new, driver_kpi_new_pct, driver_kpi_avg_tenure, driver_kpi_overall_tenure, driver_kpi_inactive_pct,
            driver_insight,
            driver_new_over_time_fig, driver_active_vs_new_fig, driver_tenure_bucket_fig, driver_segment_fig, driver_cohort_fig, driver_gap_fig,
            driver_top_table_data,
            monthly_content, 
            dealer_revenue_per_vehicle_fig, dealer_rentals_per_vehicle_fig, 
            dealer_num_vehicles_fig, dealer_vehicle_mix_fig, 
            dealer_mirai_performance_fig, 
            dealer_repeat_driver_rate_fig, dealer_rentals_per_driver_fig, 
            dealer_efficiency_scatter_fig)

    _cache_set(cache_key, result)
    return result


@app.callback(
    Output('veh-selected-vehicle', 'data'),
    [Input('vehicle_top10_chart', 'clickData'),
     Input('vehicle_mileage_chart', 'clickData'),
     Input('veh_clean_filter_btn', 'n_clicks')],
    State('veh-selected-vehicle', 'data'),
    prevent_initial_call=True
)
def update_vehicle_tab_selection(top_click, mileage_click, clear_clicks, current_selection):
    from dash import ctx, no_update

    trigger = ctx.triggered_id
    if trigger == 'veh_clean_filter_btn':
        return None

    click_payload = top_click if trigger == 'vehicle_top10_chart' else mileage_click
    if not click_payload or 'points' not in click_payload or not click_payload['points']:
        return no_update

    point = click_payload['points'][0]
    custom = point.get('customdata', [])

    selected_vin = None
    selected_5vin = None
    selected_dealer = None
    selected_label = point.get('y')

    if isinstance(custom, (list, tuple)):
        if len(custom) > 0 and pd.notna(custom[0]) and str(custom[0]).strip() not in ('', 'nan'):
            selected_vin = str(custom[0]).strip()
        if len(custom) > 1 and pd.notna(custom[1]) and str(custom[1]).strip() not in ('', 'nan'):
            selected_5vin = str(custom[1]).strip()
        if len(custom) > 2 and pd.notna(custom[2]):
            selected_dealer = str(custom[2]).strip()
        if len(custom) > 3 and pd.notna(custom[3]) and str(custom[3]).strip():
            selected_label = str(custom[3]).strip()

    selected = {
        'VIN': selected_vin,
        '5VIN': selected_5vin,
        'dealer': selected_dealer,
        'label': selected_label,
    }

    if current_selection:
        same_vin = selected.get('VIN') and current_selection.get('VIN') and selected.get('VIN') == current_selection.get('VIN')
        same_5vin = (not selected.get('VIN')) and selected.get('5VIN') and current_selection.get('5VIN') and selected.get('5VIN') == current_selection.get('5VIN')
        if same_vin or same_5vin:
            return None

    return selected


@app.callback(
    Output('veh-selected-mileage-band', 'data'),
    [Input('veh_kpi_card_15000_btn', 'n_clicks'),
     Input('veh_kpi_card_15_20_btn', 'n_clicks'),
     Input('veh_kpi_card_20_btn', 'n_clicks'),
     Input('veh_clean_filter_btn', 'n_clicks')],
    State('veh-selected-mileage-band', 'data'),
    prevent_initial_call=True
)
def update_vehicle_mileage_band(clicks_gt15k, clicks_15_20, clicks_ge20k, clear_clicks, current_band):
    from dash import ctx, no_update
    trigger = ctx.triggered_id
    if trigger == 'veh_clean_filter_btn':
        return None
    band_map = {
        'veh_kpi_card_15000_btn': 'gt15000',
        'veh_kpi_card_15_20_btn': 'band15_20',
        'veh_kpi_card_20_btn': 'ge20000',
    }
    clicked_band = band_map.get(trigger)
    if not clicked_band:
        return no_update
    # Toggle: clicking the active band clears it
    if current_band == clicked_band:
        return None
    return clicked_band


# Callback: Expenses Analysis
@app.callback(
    [Output('exp_validation_summary', 'children'),
     Output('exp_kpi_total', 'children'),
     Output('exp_kpi_count', 'children'),
     Output('exp_kpi_avg', 'children'),
     Output('exp_kpi_vehicles', 'children'),
     Output('exp_kpi_avg_vehicle', 'children'),
     Output('exp_kpi_match_rate', 'children'),
     Output('exp_trend_chart', 'figure'),
     Output('exp_category_chart', 'figure'),
     Output('exp_dealer_chart', 'figure'),
     Output('exp_model_chart', 'figure'),
     Output('exp_stacked_chart', 'figure'),
    Output('exp_vehicle_table', 'data'),
    Output('exp-stacked-trace-map', 'data')],
    [Input('exp_dealer_filter', 'value'),
     Input('exp_category_filter', 'value'),
     Input('exp_vehicle_filter', 'value'),
     Input('exp_year_filter', 'value'),
     Input('exp_unit_status_filter', 'value'),
     Input('exp_time_year_filter', 'value'),
     Input('exp_time_month_filter', 'value'),
     Input('exp_date_range', 'start_date'),
    Input('exp_date_range', 'end_date'),
    Input('fleet_status_filter', 'value'),
    Input('data-refresh-counter', 'data')]
)
def update_expenses(dealers, categories, vehicles, model_years, exp_unit_statuses, exp_years, exp_months, exp_start, exp_end, fleet_statuses=None, _refresh=None):
    filt = get_filtered_expense_df(
        dealers, categories, vehicles, model_years,
        exp_years, exp_months, exp_start, exp_end,
        fleet_statuses=fleet_statuses,
        exp_unit_statuses=exp_unit_statuses
    )

    # Validation summary
    total_rows = len(filt)
    matched = int(filt['VIN'].notna().sum())
    unmatched = total_rows - matched
    match_rate = matched / total_rows * 100 if total_rows > 0 else 0
    validation_msg = (
        f"{total_rows} invoices shown | {matched} matched to fleet "
        f"({match_rate:.1f}%) | {unmatched} unmatched"
    )

    # KPIs
    total_cost = filt['total'].sum()
    inv_count = len(filt)
    avg_per_inv = total_cost / inv_count if inv_count > 0 else 0
    unique_vehicles = filt['5VIN_key'].nunique()
    avg_per_vehicle = total_cost / unique_vehicles if unique_vehicles > 0 else 0

    # Monthly trend
    if not filt.empty and filt['year_month_dt'].notna().any():
        trend_data = filt.groupby('year_month_dt')['total'].sum().reset_index()
        min_dt = trend_data['year_month_dt'].min()
        max_dt = trend_data['year_month_dt'].max()
        if pd.notna(min_dt) and pd.notna(max_dt):
            all_months = pd.date_range(start=min_dt, end=max_dt, freq='MS')
            complete = (
                pd.DataFrame({'year_month_dt': all_months})
                .merge(trend_data, on='year_month_dt', how='left')
                .fillna(0)
            )
        else:
            complete = trend_data
        complete = complete.sort_values('year_month_dt')
        trend_fig = go.Figure()
        trend_fig.add_trace(go.Scatter(
            x=complete['year_month_dt'],
            y=complete['total'],
            mode='lines+markers+text',
            line=dict(width=3, shape='spline', color='#d4420b'),
            marker=dict(size=6, color='#d4420b'),
            text=[f"${v:,.2f}" for v in complete['total']],
            textposition='top center',
            textfont=dict(size=10, color='#d4420b'),
            hovertemplate='<b>%{x|%b %Y}</b><br>Expenses: $%{y:,.2f}<extra></extra>',
            showlegend=False
        ))
        trend_fig.update_layout(title='Monthly Expense Trend')
        trend_fig.update_layout(
            template='plotly_white',
            yaxis=dict(tickformat='$,.0f', title='Total Expenses'),
            xaxis=dict(type='date', tickformat='%b %Y', tickmode='linear',
                       dtick='M1', showgrid=False, title='', tickangle=45),
            hovermode='x unified'
        )
    else:
        trend_fig = go.Figure()

    # By Work Category
    cat_data = filt.groupby('Work Category')['total'].sum().reset_index().sort_values('total', ascending=False)
    if not cat_data.empty:
        cat_fig = px.bar(
            cat_data, x='Work Category', y='total',
            title='Expenses by Work Category', color_discrete_sequence=['#d4420b']
        )
        cat_fig.update_traces(
            text=[f"${v:,.2f}" for v in cat_data['total']],
            textposition="outside",
            textfont=dict(size=9, color='#d4420b'),
            marker=dict(opacity=0.85),
            hovertemplate='<b>%{x}</b><br>Expenses: $%{y:,.2f}<extra></extra>'
        )
        cat_fig.update_layout(
            template='plotly_white',
            yaxis=dict(tickformat='$,.0f', title='Total Expenses'),
            xaxis=dict(showgrid=False, title='', tickangle=30),
            hovermode='x unified'
        )
    else:
        cat_fig = go.Figure()

    # By Dealer
    dealer_data = filt.groupby('Dealer Name')['total'].sum().reset_index().sort_values('total', ascending=False)
    dealer_data = _append_dealer_branding(dealer_data, 'Dealer Name', prefix='dealer')
    if not dealer_data.empty:
        dealer_fig = px.bar(
            dealer_data, x='Dealer Name', y='total',
            title='Expenses by Dealer', color_discrete_sequence=['#d4420b']
        )
        dealer_fig.update_traces(
            text=[f"${v:,.2f}" for v in dealer_data['total']],
            textposition="outside",
            textfont=dict(size=9, color='#d4420b'),
            marker=dict(opacity=0.85),
            customdata=dealer_data[['dealer_name', 'dealer_short']].values,
            hovertemplate='<b>%{customdata[0]}</b><br>Label: %{customdata[1]}<br>Expenses: $%{y:,.2f}<extra></extra>'
        )
        dealer_fig.update_layout(
            template='plotly_white',
            yaxis=dict(tickformat='$,.0f', title='Total Expenses'),
            xaxis=dict(showgrid=False, title=''),
            hovermode='x unified',
            margin=dict(l=10, r=10, t=45, b=45)
        )
    else:
        dealer_fig = go.Figure()

    # By Model / Model Year
    model_filt = filt.dropna(subset=['Model', 'MY']).copy()
    if not model_filt.empty:
        model_filt['Model_Year'] = model_filt['Model'] + ' ' + model_filt['MY'].astype(int).astype(str)
        model_data = model_filt.groupby('Model_Year')['total'].sum().reset_index().sort_values('total', ascending=False)
        model_fig = px.bar(
            model_data, x='Model_Year', y='total',
            title='Expenses by Model / Model Year', color_discrete_sequence=['#d4420b']
        )
        model_fig.update_traces(
            text=[f"${v:,.2f}" for v in model_data['total']],
            textposition="outside",
            textfont=dict(size=9, color='#d4420b'),
            marker=dict(opacity=0.85),
            hovertemplate='<b>%{x}</b><br>Expenses: $%{y:,.2f}<extra></extra>'
        )
        model_fig.update_layout(
            template='plotly_white',
            yaxis=dict(tickformat='$,.0f', title='Total Expenses'),
            xaxis=dict(showgrid=False, title=''),
            hovermode='x unified'
        )
    else:
        model_fig = go.Figure()

    # Stacked: Dealer x Work Category
    stacked_data = filt.groupby(['Dealer Name', 'Work Category'])['total'].sum().reset_index()
    stacked_data = _append_dealer_branding(stacked_data, 'Dealer Name', prefix='dealer')
    if not stacked_data.empty:
        expense_category_palette = [
            '#00708D',
            '#2C353B',
            '#59AFC0',
            '#6e7680',
            '#B9D6DD',
            '#7a8a98',
            '#c7cdd3',
            '#9fb9c0',
        ]
        stacked_fig = px.bar(
            stacked_data, x='Dealer Name', y='total', color='Work Category', custom_data=['Work Category'],
            title='Expenses by Dealer and Work Category', barmode='stack',
            color_discrete_sequence=expense_category_palette
        )
        trace_map = [trace.name for trace in stacked_fig.data]
        stacked_fig.update_traces(
            hovertemplate='<b>%{x}</b><br>Category: %{fullData.name}<br>Expenses: $%{y:,.2f}<extra></extra>'
        )
        stacked_fig.update_layout(
            template='plotly_white',
            yaxis=dict(tickformat='$,.0f', title='Total Expenses'),
            xaxis=dict(showgrid=False, title=''),
            hovermode='closest',
            clickmode='event',
            margin=dict(l=10, r=10, t=45, b=45),
            legend=dict(
                title='Work Category',
                itemclick='toggleothers',
                itemdoubleclick='toggle',
                bgcolor='rgba(255,255,255,0.38)',
                bordercolor='rgba(44,53,59,0.18)',
                borderwidth=1,
                font=dict(color='#1f2d3d', size=11)
            )
        )
    else:
        stacked_fig = go.Figure()
        trace_map = []

    # Vehicle detail table
    veh_filt = filt.dropna(subset=['5VIN_key'])
    if not veh_filt.empty:
        veh_data = (
            veh_filt
            .groupby(['5VIN_key', 'VIN', 'vehicle_type', 'Model', 'MY', 'Dealer Name', 'Status'], dropna=False)
            .agg(invoice_count=('total', 'count'), total_cost=('total', 'sum'))
            .reset_index()
        )
        veh_data['MY'] = veh_data['MY'].apply(lambda x: int(x) if pd.notna(x) else None)
        veh_data = veh_data.sort_values('total_cost', ascending=False)
        veh_table_data = veh_data.to_dict('records')
    else:
        veh_table_data = []

    return (
        validation_msg,
        f"${total_cost:,.2f}",
        f"{inv_count:,}",
        f"${avg_per_inv:,.2f}",
        f"{unique_vehicles:,}",
        f"${avg_per_vehicle:,.2f}",
        f"{match_rate:.1f}%",
        trend_fig, cat_fig, dealer_fig, model_fig, stacked_fig,
        veh_table_data,
        trace_map
    )


@app.callback(
    Output('exp-drill-selection', 'data'),
    [Input('exp_stacked_chart', 'clickData'),
     Input('exp_clear_selection_btn', 'n_clicks')],
    [State('exp-drill-selection', 'data'),
     State('exp-stacked-trace-map', 'data')],
    prevent_initial_call=True
)
def update_exp_drill_selection(click_data, clear_clicks, current_selection, trace_map):
    from dash import ctx, no_update

    def _normalize_category(value):
        if value is None:
            return None
        text = str(value).strip()
        text = text.replace(' / ', '/').replace('/ ', '/').replace(' /', '/')
        return text

    def _extract_category(point):
        # 1) Direct trace name from click point payload
        point_data = point.get('data')
        if isinstance(point_data, dict) and point_data.get('name'):
            return _normalize_category(point_data.get('name'))

        # 2) Full trace metadata, if present
        full_data = point.get('fullData')
        if isinstance(full_data, dict) and full_data.get('name'):
            return _normalize_category(full_data.get('name'))

        # 3) Resolve by curveNumber against precomputed trace map
        curve_number = point.get('curveNumber')
        try:
            curve_idx = int(curve_number)
        except (TypeError, ValueError):
            curve_idx = None

        # 4) Resolve by precomputed trace map
        if curve_idx is not None and isinstance(trace_map, list) and 0 <= curve_idx < len(trace_map):
            mapped_name = trace_map[curve_idx]
            if mapped_name:
                return _normalize_category(mapped_name)

        # 5) Fallback to customdata payload
        customdata = point.get('customdata')
        if isinstance(customdata, (list, tuple)) and len(customdata) > 0:
            return _normalize_category(customdata[0])
        if isinstance(customdata, str):
            return _normalize_category(customdata)

        return None

    trigger = ctx.triggered_id
    if trigger == 'exp_clear_selection_btn':
        return None

    if trigger == 'exp_stacked_chart':
        if not click_data or 'points' not in click_data or not click_data['points']:
            return None

        point = click_data['points'][0]
        dealer = str(point.get('x')).strip() if point.get('x') is not None else None
        category = _extract_category(point)

        # If we can't resolve an exact segment/category, clear instead of keeping stale prior selection
        if dealer is None or category is None:
            return None

        selected = {'dealer': dealer, 'category': category}
        if current_selection == selected:
            return None
        return selected

    return no_update


@app.callback(
    [Output('exp_drilldown_container', 'style'),
     Output('exp_drilldown_empty', 'style'),
     Output('exp_drilldown_empty', 'children'),
    Output('exp_selected_slice', 'children'),
     Output('exp_detail_total', 'children'),
     Output('exp_detail_labor', 'children'),
     Output('exp_detail_parts', 'children'),
     Output('exp_detail_misc', 'children'),
     Output('exp_detail_count', 'children'),
     Output('exp_drilldown_table', 'data')],
    [Input('exp-drill-selection', 'data'),
     Input('exp_dealer_filter', 'value'),
     Input('exp_category_filter', 'value'),
     Input('exp_vehicle_filter', 'value'),
     Input('exp_year_filter', 'value'),
     Input('exp_unit_status_filter', 'value'),
     Input('exp_time_year_filter', 'value'),
     Input('exp_time_month_filter', 'value'),
     Input('exp_date_range', 'start_date'),
    Input('exp_date_range', 'end_date'),
    Input('fleet_status_filter', 'value')]
)
def update_expense_drilldown(selection, dealers, categories, vehicles, model_years, exp_unit_statuses, exp_years, exp_months, exp_start, exp_end, fleet_statuses=None):
    no_selection_msg = "Select a dealer and category from the chart to see detailed expenses"

    def _normalize_category(value):
        if value is None:
            return None
        text = str(value).strip()
        text = text.replace(' / ', '/').replace('/ ', '/').replace(' /', '/')
        return text

    if not selection:
        return (
            {'display': 'none'},
            {'display': 'block', 'marginBottom': '10px', 'color': '#67707d'},
            no_selection_msg,
            '',
            '$0.00', '$0.00', '$0.00', '$0.00', '0.00',
            []
        )

    filt = get_filtered_expense_df(
        dealers, categories, vehicles, model_years,
        exp_years, exp_months, exp_start, exp_end,
        fleet_statuses=fleet_statuses,
        exp_unit_statuses=exp_unit_statuses
    )
    selected_dealer = str(selection.get('dealer')).strip() if selection.get('dealer') is not None else None
    selected_category = _normalize_category(selection.get('category')) if selection.get('category') is not None else None

    if selected_dealer is None or selected_category is None:
        return (
            {'display': 'none'},
            {'display': 'block', 'marginBottom': '10px', 'color': '#67707d'},
            no_selection_msg,
            '',
            '$0.00', '$0.00', '$0.00', '$0.00', '0.00',
            []
        )

    # Normalize category/dealer values for robust matching
    dealer_series = filt['Dealer Name'].astype(str).str.strip()
    category_series = filt['Work Category'].astype(str).str.strip().str.replace(' / ', '/', regex=False).str.replace('/ ', '/', regex=False).str.replace(' /', '/', regex=False)
    detail_df = filt[
        (dealer_series == selected_dealer) &
        (category_series == selected_category)
    ].copy()

    if detail_df.empty:
        return (
            {'display': 'none'},
            {'display': 'block', 'marginBottom': '10px', 'color': '#67707d'},
            'No detailed expenses found for the selected dealer/category with current filters',
            '',
            '$0.00', '$0.00', '$0.00', '$0.00', '0.00',
            []
        )

    detail_df['Date of submission'] = detail_df['Date of submission'].dt.strftime('%Y-%m-%d')
    detail_df['MY'] = detail_df['MY'].apply(lambda x: int(x) if pd.notna(x) else None)

    display_cols = [
        'Date of submission', 'Dealer Name', 'Work Category', 'Work description',
        'SAP#', 'Labor', 'Parts', 'Misc', 'total', 'VIN', 'Model', 'MY'
    ]
    detail_df = detail_df[display_cols].sort_values('Date of submission', ascending=False)

    total_expense = detail_df['total'].sum()
    total_labor = detail_df['Labor'].sum()
    total_parts = detail_df['Parts'].sum()
    total_misc = detail_df['Misc'].sum()
    invoice_count = float(len(detail_df))
    selected_label = f"Selected Dealer: {selected_dealer} | Selected Category: {selected_category}"

    return (
        {'display': 'block', 'marginBottom': '14px'},
        {'display': 'none'},
        no_selection_msg,
        selected_label,
        f'${total_expense:,.2f}',
        f'${total_labor:,.2f}',
        f'${total_parts:,.2f}',
        f'${total_misc:,.2f}',
        f'{invoice_count:.2f}',
        detail_df.to_dict('records')
    )


@app.callback(
    Output('exp-vehicle-selection', 'data'),
    [Input('exp_vehicle_table', 'active_cell'),
     Input('exp_clear_vehicle_selection_btn', 'n_clicks')],
    [State('exp_vehicle_table', 'derived_virtual_data'),
     State('exp_vehicle_table', 'data'),
     State('exp-vehicle-selection', 'data')],
    prevent_initial_call=True
)
def update_exp_vehicle_selection(active_cell, clear_clicks, virtual_rows, all_rows, current_selection):
    from dash import ctx, no_update

    trigger = ctx.triggered_id
    if trigger == 'exp_clear_vehicle_selection_btn':
        return None

    if trigger == 'exp_vehicle_table':
        rows = virtual_rows if isinstance(virtual_rows, list) else all_rows
        if not active_cell or not isinstance(rows, list):
            return no_update

        row_index = active_cell.get('row')
        if row_index is None or row_index < 0 or row_index >= len(rows):
            return no_update

        selected_row = rows[row_index]
        selected = {
            'VIN': selected_row.get('VIN'),
            '5VIN_key': selected_row.get('5VIN_key')
        }

        if current_selection == selected:
            return None
        return selected

    return no_update


@app.callback(
    [Output('exp_vehicle_drilldown_container', 'style'),
     Output('exp_vehicle_drilldown_empty', 'style'),
     Output('exp_vehicle_drilldown_empty', 'children'),
     Output('exp_vehicle_sel_vin', 'children'),
     Output('exp_vehicle_sel_5vin', 'children'),
     Output('exp_vehicle_inv_count', 'children'),
     Output('exp_vehicle_total_cost', 'children'),
     Output('exp_vehicle_invoice_table', 'data')],
    [Input('exp-vehicle-selection', 'data'),
     Input('exp_dealer_filter', 'value'),
     Input('exp_category_filter', 'value'),
     Input('exp_vehicle_filter', 'value'),
     Input('exp_year_filter', 'value'),
     Input('exp_unit_status_filter', 'value'),
     Input('exp_time_year_filter', 'value'),
     Input('exp_time_month_filter', 'value'),
     Input('exp_date_range', 'start_date'),
    Input('exp_date_range', 'end_date'),
    Input('fleet_status_filter', 'value')]
)
def update_vehicle_invoice_drilldown(selection, dealers, categories, vehicles, model_years, exp_unit_statuses, exp_years, exp_months, exp_start, exp_end, fleet_statuses=None):
    empty_msg = 'Click a vehicle row to view the invoice details included in that summary'

    if not selection:
        return (
            {'display': 'none'},
            {'display': 'block', 'marginBottom': '10px', 'color': '#67707d'},
            empty_msg,
            '', '', '0', '$0.00',
            []
        )

    filt = get_filtered_expense_df(
        dealers, categories, vehicles, model_years,
        exp_years, exp_months, exp_start, exp_end,
        fleet_statuses=fleet_statuses,
        exp_unit_statuses=exp_unit_statuses
    )

    selected_vin = str(selection.get('VIN')).strip() if selection.get('VIN') is not None else None
    selected_5vin = str(selection.get('5VIN_key')).strip() if selection.get('5VIN_key') is not None else None

    if selected_vin:
        detail_df = filt[filt['VIN'].astype(str).str.strip() == selected_vin].copy()
    elif selected_5vin:
        detail_df = filt[filt['5VIN_key'].astype(str).str.strip() == selected_5vin].copy()
    else:
        detail_df = pd.DataFrame(columns=filt.columns)

    if detail_df.empty:
        return (
            {'display': 'none'},
            {'display': 'block', 'marginBottom': '10px', 'color': '#67707d'},
            'No invoice details found for the selected vehicle with current filters',
            selected_vin or 'N/A', selected_5vin or 'N/A', '0', '$0.00',
            []
        )

    detail_df['Date of submission'] = detail_df['Date of submission'].dt.strftime('%Y-%m-%d')

    display_cols = [
        'Date of submission', 'Dealer Name', 'Invoice #', 'SAP#',
        'Work Category', 'Work description', 'total', '5VIN_key'
    ]
    detail_df = detail_df[display_cols].sort_values('Date of submission', ascending=False)

    invoice_count = len(detail_df)
    total_cost = detail_df['total'].sum()

    return (
        {'display': 'block', 'marginBottom': '14px'},
        {'display': 'none'},
        empty_msg,
        selected_vin or 'N/A',
        selected_5vin or 'N/A',
        f'{invoice_count}',
        f'${total_cost:,.2f}',
        detail_df.to_dict('records')
    )


# Callback: Refresh Data button — reload all source files and update dropdown options
@app.callback(
    [Output('data-refresh-counter', 'data'),
     Output('station_filter', 'options'),
     Output('vehicle_type_filter', 'options'),
     Output('license_plate_filter', 'options'),
     Output('vin_filter', 'options'),
    Output('renter_filter_rental', 'options'),
    Output('renter_filter_driver', 'options'),
     Output('year_filter', 'options'),
     Output('month_filter', 'options'),
    Output('fleet_status_filter', 'options'),
     Output('date_range', 'start_date', allow_duplicate=True),
     Output('date_range', 'end_date', allow_duplicate=True),
     Output('exp_dealer_filter', 'options'),
     Output('exp_category_filter', 'options'),
     Output('exp_vehicle_filter', 'options'),
     Output('exp_year_filter', 'options'),
    Output('exp_unit_status_filter', 'options'),
     Output('exp_time_year_filter', 'options'),
     Output('exp_time_month_filter', 'options'),
     Output('exp_date_range', 'start_date'),
     Output('exp_date_range', 'end_date')],
    Input('refresh-data-btn', 'n_clicks'),
    State('data-refresh-counter', 'data'),
    prevent_initial_call=True
)
def refresh_all_data(n_clicks, counter):
    if not n_clicks:
        raise dash.exceptions.PreventUpdate

    _reload_data()

    return (
        (counter or 0) + 1,
        [{'label': s, 'value': s} for s in sorted(df['station_name'].unique())],
        [{'label': v, 'value': v} for v in sorted(df['vehicle_type'].unique())],
        [{'label': l, 'value': l} for l in sorted(df['license_plate_number'].unique())],
        [{'label': v, 'value': v} for v in sorted([x for x in df['VIN'].unique() if pd.notna(x)])],
        [{'label': r, 'value': r} for r in sorted(df['renter_name'].unique())],
        [
            {'label': f"{row['renter_name']} (ID: {row['customer_id']})", 'value': row['renter_name']}
            for _, row in df[['customer_id', 'renter_name']].drop_duplicates('customer_id').sort_values('renter_name').iterrows()
        ],
        [{'label': str(y), 'value': y} for y in sorted(df['start_year'].unique())],
        [{'label': m, 'value': m} for m in sorted(df['start_month_name'].unique(), key=lambda m: datetime.strptime(m, '%B').month)],
        [{'label': s, 'value': s} for s in fleet_status_values],
        df['rental_started_at_EST'].min().date(),
        df['rental_started_at_EST'].max().date(),
        [{'label': d, 'value': d} for d in sorted(inv_df['Dealer Name'].dropna().unique(), key=lambda x: str(x))],
        [{'label': c, 'value': c} for c in sorted(inv_df['Work Category'].dropna().unique(), key=lambda x: str(x))],
        [{'label': v, 'value': v} for v in sorted(inv_df['Vehicle'].dropna().unique(), key=lambda x: str(x))],
        [{'label': str(int(y)), 'value': int(y)} for y in sorted(inv_df['MY'].dropna().unique())],
        [{'label': s, 'value': s} for s in EXPENSE_UNIT_STATUS_OPTIONS],
        [{'label': str(y), 'value': y} for y in inv_sub_years],
        [{'label': m, 'value': m} for m in inv_sub_months],
        inv_date_min,
        inv_date_max,
    )


if __name__ == '__main__':
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', '8051'))
    debug = os.getenv('DEBUG', '').strip().lower() in {'1', 'true', 'yes'}

    if debug:
        app.run(debug=True, host=host, port=port)
    else:
        from waitress import serve
        serve(server, host=host, port=port, threads=8)