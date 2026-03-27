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

EXPENSE_UNIT_STATUS_OPTIONS = ['Onboarded', 'Offboarded', 'Sold']


def _resolve_data_path(env_var_name, default_filename):
    base_dir = Path(__file__).resolve().parent
    configured = os.getenv(env_var_name)
    path = Path(configured) if configured else base_dir / default_filename
    return path

# Load rental data
rental_file_path = _resolve_data_path('RENTAL_FILE_PATH', 'PastRentalDetails_2026-2-25.xlsx')
df = pd.read_excel(rental_file_path)

# Load fleet data
fleet_file_path = _resolve_data_path('FLEET_FILE_PATH', 'Kinto Fleet_3-19-26.xlsx')
fleet_df = pd.read_excel(fleet_file_path, sheet_name='data', header=0)

# Data cleaning and processing for rental data
df = df[df['user_groups'] == "Rideshare Drivers"]
df = df[df['Pre-Tax Charge'] >= 0]

# Assume date columns are 'rental_started_at_EST' and 'rental_end_datetime_EST'
date_cols = ['rental_started_at_EST', 'rental_end_datetime_EST']
for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors='coerce')

# Remove rows with invalid dates
df = df.dropna(subset=date_cols)

# Derived fields
df['rental_days'] = (df['rental_end_datetime_EST'] - df['rental_started_at_EST']).dt.days
df['rental_hours'] = (df['rental_end_datetime_EST'] - df['rental_started_at_EST']).dt.total_seconds() / 3600
df['start_year'] = df['rental_started_at_EST'].dt.year
df['start_month'] = df['rental_started_at_EST'].dt.month
df['start_month_name'] = df['rental_started_at_EST'].dt.strftime('%B')
df['year_month'] = df['rental_started_at_EST'].dt.strftime('%Y-%m')
df['year_month_dt'] = pd.to_datetime(df['year_month'] + '-01')
df['start_day_of_week'] = df['rental_started_at_EST'].dt.strftime('%A')
df['start_hour'] = df['rental_started_at_EST'].dt.hour

# Merge fleet data for VIN enrichment
# Normalize license plates for accurate matching
df['license_plate_normalized'] = df['license_plate_number'].fillna('').str.strip().str.upper().str.replace('-', '', regex=False)
fleet_df['plate_normalized'] = fleet_df['Plate Number'].fillna('').str.strip().str.upper().str.replace('-', '', regex=False)

# Left join to keep all rental records
df = df.merge(fleet_df[['plate_normalized', 'VIN']], 
              left_on='license_plate_normalized', 
              right_on='plate_normalized', 
              how='left')

# Create 5VIN field (last 5 characters of VIN)
df['5VIN'] = df['VIN'].apply(lambda x: str(x)[-5:] if pd.notna(x) and len(str(x)) >= 5 else (str(x) if pd.notna(x) else None))

# Drop temporary normalization columns
df = df.drop(columns=['license_plate_normalized', 'plate_normalized'])

# Add fleet fields to rental df (join by VIN)
fleet_enrichment_lookup = fleet_df[['VIN', 'Status', 'Model', 'MY', 'Colour']].dropna(subset=['VIN']).drop_duplicates('VIN')
df = df.merge(fleet_enrichment_lookup, on='VIN', how='left')

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
inv_df['Status'] = inv_df['Status'].fillna('Unknown').astype(str)

# Add year_month for time series
inv_df['year_month'] = inv_df['Date of submission'].dt.strftime('%Y-%m')
inv_df['year_month_dt'] = pd.to_datetime(inv_df['year_month'] + '-01', errors='coerce')
inv_df['sub_month_name'] = inv_df['Date of submission'].dt.strftime('%B')

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


def _reload_data():
    """Re-read all source Excel files and recompute all global dataframes."""
    global df, fleet_df, inv_df
    global inv_total_rows, inv_matched, inv_unmatched
    global inv_sub_years, inv_sub_months, inv_date_min, inv_date_max
    global fleet_status_values

    # --- Rental + Fleet ---
    _df = pd.read_excel(rental_file_path)
    _fleet_df = pd.read_excel(fleet_file_path, sheet_name='data', header=0)

    _df = _df[_df['user_groups'] == "Rideshare Drivers"]
    _df = _df[_df['Pre-Tax Charge'] >= 0]
    for col in ['rental_started_at_EST', 'rental_end_datetime_EST']:
        _df[col] = pd.to_datetime(_df[col], errors='coerce')
    _df = _df.dropna(subset=['rental_started_at_EST', 'rental_end_datetime_EST'])
    _df['rental_days'] = (_df['rental_end_datetime_EST'] - _df['rental_started_at_EST']).dt.days
    _df['rental_hours'] = (_df['rental_end_datetime_EST'] - _df['rental_started_at_EST']).dt.total_seconds() / 3600
    _df['start_year'] = _df['rental_started_at_EST'].dt.year
    _df['start_month'] = _df['rental_started_at_EST'].dt.month
    _df['start_month_name'] = _df['rental_started_at_EST'].dt.strftime('%B')
    _df['year_month'] = _df['rental_started_at_EST'].dt.strftime('%Y-%m')
    _df['year_month_dt'] = pd.to_datetime(_df['year_month'] + '-01')
    _df['start_day_of_week'] = _df['rental_started_at_EST'].dt.strftime('%A')
    _df['start_hour'] = _df['rental_started_at_EST'].dt.hour
    _df['license_plate_normalized'] = _df['license_plate_number'].fillna('').str.strip().str.upper().str.replace('-', '', regex=False)
    _fleet_df['plate_normalized'] = _fleet_df['Plate Number'].fillna('').str.strip().str.upper().str.replace('-', '', regex=False)
    _df = _df.merge(_fleet_df[['plate_normalized', 'VIN']], left_on='license_plate_normalized', right_on='plate_normalized', how='left')
    _df['5VIN'] = _df['VIN'].apply(lambda x: str(x)[-5:] if pd.notna(x) and len(str(x)) >= 5 else (str(x) if pd.notna(x) else None))
    _df = _df.drop(columns=['license_plate_normalized', 'plate_normalized'])
    _fleet_enrichment_lookup = _fleet_df[['VIN', 'Status', 'Model', 'MY', 'Colour']].dropna(subset=['VIN']).drop_duplicates('VIN')
    _df = _df.merge(_fleet_enrichment_lookup, on='VIN', how='left')

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
    _inv_df['Status'] = _inv_df['Status'].fillna('Unknown').astype(str)
    _inv_df['year_month'] = _inv_df['Date of submission'].dt.strftime('%Y-%m')
    _inv_df['year_month_dt'] = pd.to_datetime(_inv_df['year_month'] + '-01', errors='coerce')
    _inv_df['sub_month_name'] = _inv_df['Date of submission'].dt.strftime('%B')

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


# App
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# State store for tracking previous tab selections and reset state
app.layout_children_reference = None

# Layout
app.layout = dbc.Container([
    # Hidden store to track state
    dcc.Store(id='app-state-store', data={'previous_tab': 'overview'}),
    dcc.Store(id='exp-drill-selection', data=None),
    dcc.Store(id='exp-stacked-trace-map', data=[]),
    dcc.Store(id='exp-vehicle-selection', data=None),
    dcc.Store(id='veh-selected-vehicle', data=None),
    dcc.Store(id='veh-selected-mileage-band', data=None),
    dcc.Store(id='data-refresh-counter', data=0),
    
    html.Div([
        html.Button(
            html.Img(src='/assets/KINTO-Logo.png', style={'height': '50px', 'pointer-events': 'none'}),
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
    ], style={'display': 'flex', 'align-items': 'center', 'margin-bottom': '20px'}),
    
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
        dcc.Tab(label='Time Trends', value='time'),
        dcc.Tab(label='Expenses Analysis', value='expenses'),
    ], className='tab-style', style={'margin': '12px 0'}),

    # Global Filters (always visible)
    html.Div([
        html.Hr(),
        html.H3("Filters"),
        dbc.Row([
            dbc.Col([
                html.Label("Station Name (Dealer)", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '6px'}),
                dcc.Dropdown(
                    id='station_filter',
                    options=[{'label': s, 'value': s} for s in sorted(df['station_name'].unique())],
                    multi=True,
                    placeholder="Select dealers"
                )
            ], xs=12, md=6, lg=3),
            dbc.Col([
                html.Label("Vehicle Type", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '6px'}),
                dcc.Dropdown(
                    id='vehicle_type_filter',
                    options=[{'label': v, 'value': v} for v in sorted(df['vehicle_type'].unique())],
                    multi=True,
                    placeholder="Select vehicle types"
                )
            ], xs=12, md=6, lg=3),
            dbc.Col([
                html.Label("VIN", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '6px'}),
                dcc.Dropdown(
                    id='vin_filter',
                    options=[{'label': v, 'value': v} for v in sorted([x for x in df['VIN'].unique() if pd.notna(x)])],
                    multi=True,
                    placeholder="Select VINs"
                )
            ], xs=12, md=6, lg=3),
            dbc.Col([
                html.Label("License Plate", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '6px'}),
                dcc.Dropdown(
                    id='license_plate_filter',
                    options=[{'label': l, 'value': l} for l in sorted(df['license_plate_number'].unique())],
                    multi=True,
                    placeholder="Select license plates"
                )
            ], xs=12, md=6, lg=3),
        ], className='g-3 mb-3'),

        html.Hr(style={'margin': '8px 0 14px 0', 'opacity': '0.2'}),

        dbc.Row([
            dbc.Col([
                html.Label("Renter Name", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '6px'}),
                dcc.Dropdown(
                    id='renter_filter',
                    options=[{'label': r, 'value': r} for r in sorted(df['renter_name'].unique())],
                    multi=True,
                    placeholder="Select renters"
                )
            ], xs=12, md=6, lg=6),
            dbc.Col([
                html.Label("Unit Status", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '6px'}),
                dcc.Dropdown(
                    id='fleet_status_filter',
                    options=[{'label': s, 'value': s} for s in fleet_status_values],
                    multi=True,
                    placeholder="All Unit Statuses"
                )
            ], xs=12, md=6, lg=6),
        ], className='g-3 mb-3'),

        html.Hr(style={'margin': '8px 0 14px 0', 'opacity': '0.2'}),

        dbc.Row([
            dbc.Col([
                html.Label("Year", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '6px'}),
                dcc.Dropdown(
                    id='year_filter',
                    options=[{'label': str(y), 'value': y} for y in sorted(df['start_year'].unique())],
                    multi=True,
                    placeholder="Select years"
                )
            ], xs=12, md=4, lg=3),
            dbc.Col([
                html.Label("Month", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '6px'}),
                dcc.Dropdown(
                    id='month_filter',
                    options=[{'label': m, 'value': m} for m in sorted(df['start_month_name'].unique())],
                    multi=True,
                    placeholder="Select months"
                )
            ], xs=12, md=4, lg=3),
            dbc.Col([
                html.Label("Date Range", style={'fontSize': '0.85rem', 'fontWeight': '600', 'marginBottom': '6px'}),
                dcc.DatePickerRange(
                    id='date_range',
                    start_date=df['rental_started_at_EST'].min().date(),
                    end_date=df['rental_started_at_EST'].max().date(),
                    style={'width': '100%'}
                )
            ], xs=12, md=4, lg=6),
        ], className='g-3 mb-2'),
    ], id='rental-filters-div', style={'margin-bottom': '20px'}),

    # Tab Content Containers
    html.Div(id='overview-content', children=[
        # Executive Overview
        html.Hr(),
        html.H3("Executive Overview", className='section-title'),
        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Total Revenue", className='kpi-label'), html.Div(id='kpi_revenue', className='kpi-value')])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Total Rentals", className='kpi-label'), html.Div(id='kpi_rentals', className='kpi-value')])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Total Rental Days", className='kpi-label'), html.Div(id='kpi_rental_days', className='kpi-value')])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Avg Revenue/Rental", className='kpi-label'), html.Div(id='kpi_avg_rev', className='kpi-value')])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Avg Rental Days", className='kpi-label'), html.Div(id='kpi_avg_days', className='kpi-value')])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([html.Div("Avg KMs Traveled", className='kpi-label'), html.Div(id='kpi_avg_kms', className='kpi-value')])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
        ], className='g-3 dashboard-kpi-row overview-kpi-row'),
        dbc.Row([
            dbc.Col(dcc.Graph(id='trend_revenue', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=4, className='dashboard-graph-col'),
            dbc.Col(dcc.Graph(id='trend_rentals', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=4, className='dashboard-graph-col'),
            dbc.Col(dcc.Graph(id='trend_rental_days', className='dashboard-graph', config={'responsive': True, 'displayModeBar': False}), xs=12, xl=4, className='dashboard-graph-col'),
        ], className='g-3 dashboard-chart-row'),
        html.Hr(style={'margin': '16px 0 10px 0'}),
        html.H5("Cumulative Performance (Month-to-Date Comparison)", className='section-subtitle'),
        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Projected Month-End Revenue", className='kpi-label'),
                html.Div(id='cum_proj_rev', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, xl=4, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Projected Month-End Rentals", className='kpi-label'),
                html.Div(id='cum_proj_rentals', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, xl=4, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Projected Month-End Rental Days", className='kpi-label'),
                html.Div(id='cum_proj_days', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, xl=4, className='dashboard-kpi-col'),
        ], className='g-3 dashboard-kpi-row'),
        dbc.Row([
            dbc.Col([
                html.Div(id='cum_revenue_summary', className='cum-summary-text'),
                dcc.Graph(id='cum_revenue_chart', className='dashboard-graph dashboard-graph-tall', config={'responsive': True, 'displayModeBar': False})
            ], xs=12, xl=4, className='dashboard-graph-col'),
            dbc.Col([
                html.Div(id='cum_rentals_summary', className='cum-summary-text'),
                dcc.Graph(id='cum_rentals_chart', className='dashboard-graph dashboard-graph-tall', config={'responsive': True, 'displayModeBar': False})
            ], xs=12, xl=4, className='dashboard-graph-col'),
            dbc.Col([
                html.Div(id='cum_days_summary', className='cum-summary-text'),
                dcc.Graph(id='cum_days_chart', className='dashboard-graph dashboard-graph-tall', config={'responsive': True, 'displayModeBar': False})
            ], xs=12, xl=4, className='dashboard-graph-col'),
        ], className='g-3 mt-2 dashboard-chart-row'),
    ], style={'display': 'block'}),
    
    html.Div(id='monthly-content', children=[
        # Monthly Comparison
        html.Hr(),
        html.H3("Monthly Comparison", className='section-title'),
        dbc.Row([
            dbc.Col([
                html.Label("Select Month for Comparison"),
                dcc.Dropdown(
                    id='comparison_month',
                    options=[{'label': ym, 'value': ym} for ym in sorted(df['year_month'].unique())],
                    placeholder="Select year-month"
                )
            ], width=4),
        ]),
        html.Div(id='monthly_comparison', className='monthly-comparison-container'),
    ], style={'display': 'none'}),
    
    html.Div(id='dealer-content', children=[
        # Dealer Performance
        html.Hr(),
        html.H3("Dealer Performance"),
        dbc.Row([
            dbc.Col(dcc.Graph(id='dealer_revenue_chart'), width=6),
            dbc.Col(dcc.Graph(id='dealer_days_chart'), width=6),
        ]),
        dash_table.DataTable(
            id='dealer_table',
            columns=[
                {'name': 'Station Name', 'id': 'station_name'},
                {'name': 'Total Revenue', 'id': 'total_revenue', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Rentals', 'id': 'rentals', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Rental Days', 'id': 'rental_days', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Avg Revenue', 'id': 'avg_revenue', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Avg Rental Days', 'id': 'avg_days', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
            ],
            data=[],
            sort_action='native',
            filter_action='native',
            page_size=10,
        ),
    ], style={'display': 'none'}),
    
    html.Div(id='vehicle-content', children=[
        # Vehicle Performance
        html.Hr(),
        html.H3("Vehicle Performance"),

        html.H5("Vehicle Performance Visual Summary", style={'marginTop': '10px'}),
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
            page_size=10,
        ),
    ], style={'display': 'none'}),
    
    html.Div(id='rental-content', children=[
        # Rental Detail Table
        html.Hr(),
        html.H3("Rental Details"),
        dash_table.DataTable(
            id='rental_table',
            columns=[
                {'name': 'Rental ID', 'id': 'rental_id', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'Rental Start Date', 'id': 'rental_started_at_EST'},
                {'name': 'Rental End Date', 'id': 'rental_end_datetime_EST'},
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
            page_size=10,
        ),
    ], style={'display': 'none'}),
    
    html.Div(id='driver-content', children=[
        # Driver Analysis
        html.Hr(),
        html.H3("Driver Analysis", className='section-title'),
        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Total Drivers", className='kpi-label'),
                html.Div(id='driver_kpi_total', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("New Drivers (Period)", className='kpi-label'),
                html.Div(id='driver_kpi_new', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("% New vs Total", className='kpi-label'),
                html.Div(id='driver_kpi_new_pct', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=2, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Avg Driver Tenure", className='kpi-label'),
                html.Div(id='driver_kpi_avg_tenure', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=3, className='dashboard-kpi-col'),
            dbc.Col(dbc.Card([dbc.CardBody([
                html.Div("Returning Drivers", className='kpi-label'),
                html.Div(id='driver_kpi_returning', className='kpi-value')
            ])], className='kpi-card dashboard-kpi-card'), xs=12, sm=6, xl=3, className='dashboard-kpi-col'),
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
                {'name': 'Customer ID', 'id': 'customer_id'},
                {'name': 'Renter Name', 'id': 'renter_name'},
                {'name': 'First Rental Date', 'id': 'first_rental_date'},
                {'name': 'Tenure (Days)', 'id': 'driver_tenure_days', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'Total Rentals', 'id': 'rentals', 'type': 'numeric', 'format': Format(precision=0, scheme=Scheme.fixed)},
                {'name': 'Total Revenue', 'id': 'revenue', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Total Rental Days', 'id': 'rental_days', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
            ],
            data=[],
            sort_action='native',
            filter_action='native',
            page_size=10,
            style_table={'overflowX': 'auto'},
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
            page_size=10,
        ),
    ], style={'display': 'none'}),
    
    html.Div(id='time-content', children=[
        # Time Analysis
        html.Hr(),
        html.H3("Time Trends"),
        dbc.Row([
            dbc.Col(dcc.Graph(id='rentals_by_month'), width=4),
            dbc.Col(dcc.Graph(id='rentals_by_dow'), width=4),
            dbc.Col(dcc.Graph(id='rentals_by_hour'), width=4),
        ]),
        dbc.Row([
            dbc.Col(dcc.Graph(id='days_by_month'), width=6),
            dbc.Col(dcc.Graph(id='revenue_by_month'), width=6),
        ]),
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
                        options=[{'label': d, 'value': d} for d in sorted(inv_df['Dealer Name'].dropna().unique())],
                        multi=True,
                        placeholder="All Dealers"
                    )
                ], width=3),
                dbc.Col([
                    html.Label("Work Category"),
                    dcc.Dropdown(
                        id='exp_category_filter',
                        options=[{'label': c, 'value': c} for c in sorted(inv_df['Work Category'].dropna().unique())],
                        multi=True,
                        placeholder="All Categories"
                    )
                ], width=3),
                dbc.Col([
                    html.Label("Vehicle Model"),
                    dcc.Dropdown(
                        id='exp_vehicle_filter',
                        options=[{'label': v, 'value': v} for v in sorted(inv_df['Vehicle'].dropna().unique())],
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
                page_size=10,
                style_cell={'padding': '8px', 'fontSize': '13px', 'whiteSpace': 'normal', 'height': 'auto', 'textAlign': 'left'},
                style_table={'overflowX': 'auto'},
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
            page_size=15,
            style_data={'cursor': 'pointer'},
            style_data_conditional=[
                {'if': {'state': 'active'}, 'backgroundColor': '#eef6fb', 'border': '1px solid #00708D'},
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
                page_size=12,
                style_cell={'padding': '8px', 'fontSize': '13px', 'whiteSpace': 'normal', 'height': 'auto', 'textAlign': 'left'},
                style_table={'overflowX': 'auto'},
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
     Output('renter_filter', 'value'),
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
            None, None, None, None, None, None, None,
            reset_start, reset_end, 'overview', None, updated_store
        )

    previous_tab = stored_data.get('previous_tab', 'overview')

    if current_tab == previous_tab and current_tab != 'overview':
        updated_store = {'previous_tab': 'overview', 'last_logo_clicks': logo_clicks or 0}
        return (
            None, None, None, None, None, None, None,
            reset_start, reset_end, 'overview', None, updated_store
        )

    updated_store = {'previous_tab': current_tab, 'last_logo_clicks': logo_clicks or 0}
    from dash import no_update
    return (
        no_update, no_update, no_update, no_update, no_update, no_update,
        no_update, no_update, no_update, no_update, no_update, updated_store
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
    status_series = filt['Status'].fillna('Unknown').astype(str)
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
     Output('time-content', 'style'),
     Output('expenses-content', 'style'),
     Output('rental-filters-div', 'style')],
    [Input('main-tabs', 'value')]
)
def update_tab_visibility(selected_tab):
    tabs = ['overview', 'monthly', 'dealer', 'vehicle', 'rental', 'driver', 'time', 'expenses']
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
     Input('renter_filter', 'value'),
     Input('year_filter', 'value'),
     Input('month_filter', 'value'),
     Input('date_range', 'start_date'),
     Input('date_range', 'end_date'),
    Input('vin_filter', 'value'),
    Input('fleet_status_filter', 'value'),
    Input('data-refresh-counter', 'data')]
)
def update_comparison_month_options(stations, vehicle_types, plates, renters, years, months, start_date, end_date, vins, fleet_statuses=None, _refresh=None):
    filtered_df = df.copy()
    
    if stations:
        filtered_df = filtered_df[filtered_df['station_name'].isin(stations)]
    if vehicle_types:
        filtered_df = filtered_df[filtered_df['vehicle_type'].isin(vehicle_types)]
    if plates:
        filtered_df = filtered_df[filtered_df['license_plate_number'].isin(plates)]
    if vins:
        filtered_df = filtered_df[filtered_df['VIN'].isin(vins)]
    if renters:
        filtered_df = filtered_df[filtered_df['renter_name'].isin(renters)]
    if years:
        filtered_df = filtered_df[filtered_df['start_year'].isin(years)]
    if months:
        filtered_df = filtered_df[filtered_df['start_month_name'].isin(months)]
    if fleet_statuses:
        filtered_df = filtered_df[filtered_df['Status'].isin(fleet_statuses)]
    if start_date and end_date:
        filtered_df = filtered_df[(filtered_df['rental_started_at_EST'].dt.date >= pd.to_datetime(start_date).date()) & 
                                  (filtered_df['rental_started_at_EST'].dt.date <= pd.to_datetime(end_date).date())]
    
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
     Output('driver_table', 'data'),
    Output('driver_kpi_total', 'children'),
    Output('driver_kpi_new', 'children'),
    Output('driver_kpi_new_pct', 'children'),
    Output('driver_kpi_avg_tenure', 'children'),
    Output('driver_kpi_returning', 'children'),
    Output('driver_insight_summary', 'children'),
    Output('driver_new_over_time_chart', 'figure'),
    Output('driver_active_vs_new_chart', 'figure'),
    Output('driver_tenure_bucket_chart', 'figure'),
    Output('driver_segment_chart', 'figure'),
    Output('driver_cohort_heatmap', 'figure'),
    Output('driver_gap_chart', 'figure'),
    Output('driver_top_table', 'data'),
     Output('rentals_by_month', 'figure'),
     Output('rentals_by_dow', 'figure'),
     Output('rentals_by_hour', 'figure'),
     Output('days_by_month', 'figure'),
     Output('revenue_by_month', 'figure'),
    Output('monthly_comparison', 'children'),
    Output('dealer_revenue_chart', 'figure'),
    Output('dealer_days_chart', 'figure')],
    [Input('station_filter', 'value'),
     Input('vehicle_type_filter', 'value'),
     Input('license_plate_filter', 'value'),
     Input('renter_filter', 'value'),
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
def update_all(stations, vehicle_types, plates, renters, years, months, start_date, end_date, comparison_month, active_tab, vins, fleet_statuses=None, selected_vehicle=None, selected_band=None, _refresh=None):
    filtered_df = df.copy()
    
    if stations:
        filtered_df = filtered_df[filtered_df['station_name'].isin(stations)]
    if vehicle_types:
        filtered_df = filtered_df[filtered_df['vehicle_type'].isin(vehicle_types)]
    if plates:
        filtered_df = filtered_df[filtered_df['license_plate_number'].isin(plates)]
    if vins:
        filtered_df = filtered_df[filtered_df['VIN'].isin(vins)]
    if renters:
        filtered_df = filtered_df[filtered_df['renter_name'].isin(renters)]
    if years:
        filtered_df = filtered_df[filtered_df['start_year'].isin(years)]
    if months:
        filtered_df = filtered_df[filtered_df['start_month_name'].isin(months)]
    if fleet_statuses:
        filtered_df = filtered_df[filtered_df['Status'].isin(fleet_statuses)]
    if start_date and end_date:
        filtered_df = filtered_df[(filtered_df['rental_started_at_EST'].dt.date >= pd.to_datetime(start_date).date()) & 
                                  (filtered_df['rental_started_at_EST'].dt.date <= pd.to_datetime(end_date).date())]
    
    # KPIs
    total_rev = filtered_df['total_to_charge'].sum()
    total_rentals = len(filtered_df)
    total_days = filtered_df['rental_days'].sum()
    avg_rev = total_rev / total_rentals if total_rentals > 0 else 0
    avg_days = total_days / total_rentals if total_rentals > 0 else 0
    avg_kms = filtered_df['kms_traveled'].mean()
    
    # Trends with complete monthly series and data labels
    trend_data_rev = build_complete_monthly_series(filtered_df, 'total_to_charge')
    trend_rev = px.line(trend_data_rev, x='year_month_dt', y='total_to_charge', 
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

    def _build_cumulative_figure(cum_df, metric_col, title, y_title, value_format, projected_value_text):
        fig = go.Figure()
        if cum_df.empty:
            return _empty_cum_figure(title, y_title)

        latest_ts = cum_df['date'].max()
        latest_day = int(latest_ts.day)
        current_month = latest_ts.to_period('M').to_timestamp()
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
            month_name = pd.Timestamp(month_start).strftime('%b %Y')
            fig.add_trace(go.Scatter(
                x=month_slice['day_of_month'],
                y=month_slice[f'cum_{metric_col}'],
                customdata=[['Actual'] for _ in range(len(month_slice))],
                mode='lines+markers',
                name=label,
                line=dict(color=color, width=width),
                marker=dict(size=6 if label == 'Current Month' else 4),
                hovertemplate=f'<b>{label} ({month_name})</b><br>Day: %{{x}}<br>Value: {value_format}<br>Type: %{{customdata[0]}}<extra></extra>'
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
                    hovertemplate=f'<b>Avg 6 Months</b><br>Day: %{{x}}<br>Value: {value_format}<br>Type: %{{customdata[0]}}<extra></extra>'
                ))

        current_month_df = cum_df[(cum_df['month_start'] == current_month) & (cum_df['day_of_month'] <= latest_day)].copy()
        if not current_month_df.empty:
            current_last_day = int(current_month_df['day_of_month'].max())
            current_actual = float(current_month_df[current_month_df['day_of_month'] == current_last_day][f'cum_{metric_col}'].iloc[-1])
            total_days_current_month = int(pd.Timestamp(latest_ts).days_in_month)

            if current_last_day > 1 and total_days_current_month > current_last_day and projected_value_text != 'Projection not available':
                projected_final = float(projected_value_text)
                fig.add_trace(go.Scatter(
                    x=[current_last_day, total_days_current_month],
                    y=[current_actual, projected_final],
                    customdata=[['Projected'], ['Projected']],
                    mode='lines+markers',
                    name='Projected',
                    line=dict(color='#00708D', width=3, dash='dash'),
                    marker=dict(size=6, symbol='diamond'),
                    hovertemplate=f'<b>Current Month Projection</b><br>Day: %{{x}}<br>Value: {value_format}<br>Type: %{{customdata[0]}}<extra></extra>'
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

    def _build_mtd_summary(cum_df, metric_col):
        if cum_df.empty:
            return html.Span('MTD vs Prev: N/A', style={'color': '#6b7280'})

        latest_ts = cum_df['date'].max()
        latest_day = int(latest_ts.day)
        current_month = latest_ts.to_period('M').to_timestamp()
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

    if filtered_df.empty:
        projected_month_end_revenue = html.Span('Projection not available', style={'color': '#6b7280'})
        projected_month_end_rentals = html.Span('Projection not available', style={'color': '#6b7280'})
        projected_month_end_days = html.Span('Projection not available', style={'color': '#6b7280'})
        cum_revenue_summary = html.Span('MTD vs Prev: N/A', style={'color': '#6b7280'})
        cum_rentals_summary = html.Span('MTD vs Prev: N/A', style={'color': '#6b7280'})
        cum_days_summary = html.Span('MTD vs Prev: N/A', style={'color': '#6b7280'})
        cum_revenue_fig = _empty_cum_figure('Revenue (Cumulative by Month)', 'Revenue')
        cum_rentals_fig = _empty_cum_figure('Rentals (Cumulative by Month)', 'Rentals')
        cum_days_fig = _empty_cum_figure('Rental Days (Cumulative by Month)', 'Rental Days')
    else:
        daily_df = filtered_df[['rental_started_at_EST', 'total_to_charge', 'rental_days']].copy()
        daily_df['date'] = pd.to_datetime(daily_df['rental_started_at_EST']).dt.floor('D')
        daily_df['month_start'] = daily_df['date'].dt.to_period('M').dt.to_timestamp()
        daily_df['day_of_month'] = daily_df['date'].dt.day

        daily_agg = daily_df.groupby(['month_start', 'date', 'day_of_month'], as_index=False).agg(
            revenue=('total_to_charge', 'sum'),
            rentals=('date', 'size'),
            rental_days=('rental_days', 'sum')
        ).sort_values(['month_start', 'date'])

        for metric in ['revenue', 'rentals', 'rental_days']:
            daily_agg[f'cum_{metric}'] = daily_agg.groupby('month_start')[metric].cumsum()

        latest_date = pd.to_datetime(daily_agg['date']).max()
        latest_day = int(latest_date.day)
        current_month = pd.Timestamp(latest_date).to_period('M').to_timestamp()
        current_month_days = int(pd.Timestamp(latest_date).days_in_month)

        def _project_metric(metric_col):
            current_month_rows = daily_agg[(daily_agg['month_start'] == current_month) & (daily_agg['day_of_month'] <= latest_day)]
            if current_month_rows.empty:
                return None
            current_last_day = int(current_month_rows['day_of_month'].max())
            if current_last_day <= 1:
                return None
            current_cum = float(current_month_rows[current_month_rows['day_of_month'] == current_last_day][f'cum_{metric_col}'].iloc[-1])
            return (current_cum / current_last_day) * current_month_days

        def _previous_month_final(metric_col):
            prev_month = (pd.Timestamp(current_month) - pd.DateOffset(months=1)).to_period('M').to_timestamp()
            prev_rows = daily_agg[daily_agg['month_start'] == prev_month]
            if prev_rows.empty:
                return None
            return float(prev_rows[f'cum_{metric_col}'].max())

        def _format_projected_kpi(projected_raw, metric_col, is_currency=False):
            if projected_raw is None:
                return html.Span('Projection not available', style={'color': '#6b7280'})

            value_text = f"${projected_raw:,.2f}" if is_currency else f"{projected_raw:,.2f}"
            prev_final = _previous_month_final(metric_col)
            if prev_final is None or prev_final == 0:
                return html.Div([
                    html.Div(value_text),
                    html.Div('vs last month: N/A', style={'fontSize': '0.78rem', 'fontWeight': '600', 'color': '#6b7280', 'marginTop': '2px'})
                ], style={'lineHeight': '1.1'})

            delta_pct = ((projected_raw - prev_final) / prev_final) * 100
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

        proj_revenue_raw = _project_metric('revenue')
        proj_rentals_raw = _project_metric('rentals')
        proj_days_raw = _project_metric('rental_days')

        projected_month_end_revenue = _format_projected_kpi(proj_revenue_raw, 'revenue', is_currency=True)
        projected_month_end_rentals = _format_projected_kpi(proj_rentals_raw, 'rentals', is_currency=False)
        projected_month_end_days = _format_projected_kpi(proj_days_raw, 'rental_days', is_currency=False)

        cum_revenue_summary = _build_mtd_summary(daily_agg, 'revenue')
        cum_rentals_summary = _build_mtd_summary(daily_agg, 'rentals')
        cum_days_summary = _build_mtd_summary(daily_agg, 'rental_days')

        cum_revenue_fig = _build_cumulative_figure(
            daily_agg,
            metric_col='revenue',
            title='Revenue (Cumulative by Month)',
            y_title='Revenue',
            value_format='$%{y:,.2f}',
            projected_value_text=str(proj_revenue_raw) if proj_revenue_raw is not None else 'Projection not available'
        )
        cum_rentals_fig = _build_cumulative_figure(
            daily_agg,
            metric_col='rentals',
            title='Rentals (Cumulative by Month)',
            y_title='Rentals',
            value_format='%{y:,.2f}',
            projected_value_text=str(proj_rentals_raw) if proj_rentals_raw is not None else 'Projection not available'
        )
        cum_days_fig = _build_cumulative_figure(
            daily_agg,
            metric_col='rental_days',
            title='Rental Days (Cumulative by Month)',
            y_title='Rental Days',
            value_format='%{y:,.2f}',
            projected_value_text=str(proj_days_raw) if proj_days_raw is not None else 'Projection not available'
        )
    
    # Dealer table
    dealer_agg = filtered_df.groupby('station_name').agg({
        'total_to_charge': 'sum',
        'rental_id': 'count',
        'rental_days': 'sum'
    }).reset_index()
    dealer_agg.columns = ['station_name', 'total_revenue', 'rentals', 'rental_days']
    dealer_agg['avg_revenue'] = dealer_agg['total_revenue'] / dealer_agg['rentals']
    dealer_agg['avg_days'] = dealer_agg['rental_days'] / dealer_agg['rentals']
    
    # Vehicle performance (vehicle-level view using VIN as primary key)
    def _first_valid(series):
        valid = series.dropna()
        return valid.iloc[0] if not valid.empty else None

    vehicle_view_df = filtered_df.copy()
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
        revenue=('total_to_charge', 'sum'),
        avg_kms=('kms_traveled', 'mean')
    ).reset_index(drop=True)

    vehicle_perf_df['avg_revenue'] = vehicle_perf_df['revenue'] / vehicle_perf_df['rentals']

    # Current mileage from full rental history (not date-limited), aggregated by VIN
    mileage_history = df[['VIN', 'mileage_end']].copy()
    mileage_history['mileage_end'] = pd.to_numeric(mileage_history['mileage_end'], errors='coerce')
    mileage_lookup = (
        mileage_history.dropna(subset=['VIN', 'mileage_end'])
        .groupby('VIN', as_index=False)['mileage_end']
        .max()
        .rename(columns={'mileage_end': 'current_mileage'})
    )

    vehicle_perf_df = vehicle_perf_df.merge(mileage_lookup, on='VIN', how='left')
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

    # Top 10 vehicles by rentals (with 5VIN + dealer and current mileage)
    top10_df = vehicle_perf_tab_df.sort_values('rentals', ascending=False).head(10).copy()
    top10_df['display_5vin'] = top10_df['5VIN'].fillna(top10_df['VIN'].astype(str).str[-5:]).fillna('N/A')
    top10_df['dealer_display'] = top10_df['station_name'].fillna('N/A')
    top10_df['label'] = top10_df['display_5vin'].astype(str) + ' - ' + top10_df['dealer_display'].astype(str)
    top10_df = top10_df.sort_values('rentals', ascending=True)

    if not top10_df.empty:
        top10_fig = go.Figure(go.Bar(
            x=top10_df['rentals'],
            y=top10_df['label'],
            orientation='h',
            marker=dict(color='#00708D', opacity=0.86),
            text=[f"{int(round(float(v)))}" for v in top10_df['rentals']],
            textposition='outside',
            customdata=top10_df[['VIN', '5VIN', 'station_name', 'label', 'current_mileage']].values,
            hovertemplate='<b>%{y}</b><br>Rentals: %{x:.0f}<br>Current Mileage: %{customdata[4]:,.0f} km<br>VIN: %{customdata[0]}<br>Dealer: %{customdata[2]}<extra></extra>'
        ))
        top10_fig.update_layout(
            title='Top 10 Vehicles by Rentals',
            template='plotly_white',
            xaxis=dict(title='Rentals', tickformat='.0f'),
            yaxis=dict(title='5VIN - Dealer Name'),
            margin=dict(l=10, r=10, t=45, b=10)
        )
    else:
        top10_fig = go.Figure()

    # Mileage-focused visual (Top 10 by current mileage)
    top_mileage_df = vehicle_perf_tab_df.dropna(subset=['current_mileage']).sort_values('current_mileage', ascending=False).head(10).copy()
    top_mileage_df['display_5vin'] = top_mileage_df['5VIN'].fillna(top_mileage_df['VIN'].astype(str).str[-5:]).fillna('N/A')
    top_mileage_df['dealer_display'] = top_mileage_df['station_name'].fillna('N/A')
    top_mileage_df['label'] = top_mileage_df['display_5vin'].astype(str) + ' - ' + top_mileage_df['dealer_display'].astype(str)
    top_mileage_df = top_mileage_df.sort_values('current_mileage', ascending=True)

    if not top_mileage_df.empty:
        mileage_scatter_fig = go.Figure(go.Bar(
            x=top_mileage_df['current_mileage'],
            y=top_mileage_df['label'],
            orientation='h',
            marker=dict(color='#2C353B', opacity=0.86),
            text=[f"{int(round(float(v))):,}" for v in top_mileage_df['current_mileage']],
            textposition='outside',
            customdata=top_mileage_df[['VIN', '5VIN', 'station_name', 'label', 'rentals']].values,
            hovertemplate='<b>%{y}</b><br>Current Mileage: %{x:,.0f} km<br>Rentals: %{customdata[4]:.0f}<br>VIN: %{customdata[0]}<br>Dealer: %{customdata[2]}<extra></extra>'
        ))
        mileage_scatter_fig.update_layout(
            title='Top 10 Vehicles by Current Mileage',
            template='plotly_white',
            xaxis=dict(title='Current Mileage (km)', tickformat=',.0f'),
            yaxis=dict(title='5VIN - Dealer Name'),
            margin=dict(l=10, r=10, t=45, b=10)
        )
    else:
        mileage_scatter_fig = go.Figure()

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
    
    # Rental table
    rental_table_df = filtered_df[['rental_id', 'rental_started_at_EST', 'rental_end_datetime_EST', 'renter_name', 'station_name', 'Model', 'license_plate_number', '5VIN', 'rental_days', 'kms_traveled', 'total_to_charge']].copy()
    rental_table_df['rental_started_at_EST'] = rental_table_df['rental_started_at_EST'].dt.strftime('%Y-%m-%d')
    rental_table_df['rental_end_datetime_EST'] = rental_table_df['rental_end_datetime_EST'].dt.strftime('%Y-%m-%d')
    rental_data = rental_table_df.to_dict('records')
    
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

    filtered_driver_df = filtered_df.copy()
    full_history_df = df.copy()

    if 'customer_id' in filtered_driver_df.columns:
        filtered_driver_df['customer_id'] = filtered_driver_df['customer_id'].astype(str).str.strip()
        filtered_driver_df.loc[filtered_driver_df['customer_id'].isin(['', 'nan', 'None']), 'customer_id'] = pd.NA
    else:
        filtered_driver_df['customer_id'] = pd.NA

    if 'customer_id' in full_history_df.columns:
        full_history_df['customer_id'] = full_history_df['customer_id'].astype(str).str.strip()
        full_history_df.loc[full_history_df['customer_id'].isin(['', 'nan', 'None']), 'customer_id'] = pd.NA
    else:
        full_history_df['customer_id'] = pd.NA

    filtered_driver_df['customer_id'] = filtered_driver_df['customer_id'].fillna('RENTER:' + filtered_driver_df['renter_name'].fillna('Unknown').astype(str))
    full_history_df['customer_id'] = full_history_df['customer_id'].fillna('RENTER:' + full_history_df['renter_name'].fillna('Unknown').astype(str))

    full_first_rental = (
        full_history_df
        .dropna(subset=['rental_started_at_EST'])
        .groupby('customer_id', as_index=False)['rental_started_at_EST']
        .min()
        .rename(columns={'rental_started_at_EST': 'first_rental_date'})
    )

    filtered_driver_df = filtered_driver_df.merge(full_first_rental, on='customer_id', how='left')

    if filtered_driver_df.empty:
        driver_agg = pd.DataFrame(columns=['customer_id', 'renter_name', 'first_rental_date', 'tenure_bucket', 'driver_tenure_days', 'rentals', 'active_months', 'avg_days_between_rentals', 'rental_days', 'revenue', 'avg_duration', 'avg_revenue', 'avg_kms'])
        driver_top_table_data = []
        driver_kpi_total = '0'
        driver_kpi_new = '0'
        driver_kpi_new_pct = '0.0%'
        driver_kpi_avg_tenure = '0 days'
        driver_kpi_returning = '0.0%'
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
            revenue=('total_to_charge', 'sum'),
            avg_revenue=('total_to_charge', 'mean'),
            avg_kms=('kms_traveled', 'mean')
        ).reset_index()

        driver_agg = driver_agg.merge(active_months, on='customer_id', how='left')
        driver_agg = driver_agg.merge(avg_days_between, on='customer_id', how='left')
        driver_agg['active_months'] = driver_agg['active_months'].fillna(1)
        driver_agg['avg_days_between_rentals'] = driver_agg['avg_days_between_rentals'].fillna(0)
        driver_agg['first_rental_date'] = pd.to_datetime(driver_agg['first_rental_date']).dt.strftime('%Y-%m-%d')
        driver_agg['tenure_bucket'] = driver_agg['tenure_bucket'].astype(str).replace('nan', 'Unknown')

        period_start = pd.to_datetime(filtered_driver_df['rental_started_at_EST']).min().normalize()
        period_end = pd.to_datetime(filtered_driver_df['rental_started_at_EST']).max().normalize()

        first_dates_by_driver = filtered_driver_df[['customer_id', 'first_rental_date']].drop_duplicates('customer_id')
        new_drivers_period = first_dates_by_driver[
            (pd.to_datetime(first_dates_by_driver['first_rental_date']).dt.normalize() >= period_start) &
            (pd.to_datetime(first_dates_by_driver['first_rental_date']).dt.normalize() <= period_end)
        ]['customer_id'].nunique()

        total_drivers = int(driver_agg['customer_id'].nunique())
        pct_new = (new_drivers_period / total_drivers * 100) if total_drivers else 0
        avg_tenure_days = float(pd.to_numeric(driver_agg['driver_tenure_days'], errors='coerce').fillna(0).mean()) if total_drivers else 0

        first_month_per_driver = filtered_driver_df.groupby('customer_id')['first_rental_date'].first().dt.to_period('M').dt.to_timestamp()
        returned_after_first = filtered_driver_df.assign(
            first_month=filtered_driver_df['customer_id'].map(first_month_per_driver)
        ).groupby('customer_id').apply(lambda d: (d['rental_month'] > d['first_month']).any())
        returning_pct = float(returned_after_first.mean() * 100) if len(returned_after_first) else 0
        active_2plus_pct = float((driver_agg['active_months'] >= 2).mean() * 100) if total_drivers else 0

        driver_kpi_total = f"{total_drivers:,}"
        driver_kpi_new = f"{new_drivers_period:,}"
        driver_kpi_new_pct = f"{pct_new:.1f}%"
        driver_kpi_avg_tenure = f"{avg_tenure_days:,.0f} days"
        driver_kpi_returning = f"{active_2plus_pct:.1f}%"

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

        # Driver value segmentation
        segment_df = driver_agg[['customer_id', 'revenue']].copy()
        if len(segment_df) >= 3:
            q1, q2 = segment_df['revenue'].quantile([0.33, 0.66]).tolist()
            conditions = [segment_df['revenue'] <= q1, (segment_df['revenue'] > q1) & (segment_df['revenue'] <= q2), segment_df['revenue'] > q2]
            labels = ['Low activity', 'Medium', 'High value']
            segment_df['segment'] = labels[0]
            segment_df.loc[conditions[0], 'segment'] = labels[0]
            segment_df.loc[conditions[1], 'segment'] = labels[1]
            segment_df.loc[conditions[2], 'segment'] = labels[2]
        else:
            segment_df['segment'] = 'Low activity'

        segment_summary = segment_df.groupby('segment', as_index=False).agg(
            drivers=('customer_id', 'count'),
            revenue=('revenue', 'sum')
        )
        segment_order = ['Low activity', 'Medium', 'High value']
        segment_summary['segment'] = pd.Categorical(segment_summary['segment'], categories=segment_order, ordered=True)
        segment_summary = segment_summary.sort_values('segment')
        total_segment_revenue = segment_summary['revenue'].sum() if not segment_summary.empty else 0
        segment_summary['revenue_share'] = ((segment_summary['revenue'] / total_segment_revenue) * 100).fillna(0) if total_segment_revenue else 0

        driver_segment_fig = go.Figure()
        if not segment_summary.empty:
            driver_segment_fig.add_trace(go.Bar(
                x=segment_summary['segment'], y=segment_summary['drivers'], name='Drivers', marker_color='#00708D'
            ))
            driver_segment_fig.add_trace(go.Scatter(
                x=segment_summary['segment'], y=segment_summary['revenue_share'], name='Revenue Share %',
                mode='lines+markers', yaxis='y2', line=dict(color='#d4420b', width=2.5)
            ))
            _apply_standard_figure_layout(
                driver_segment_fig,
                'Driver Value Segmentation',
                xaxis=dict(showgrid=False, title='', automargin=True),
                yaxis=dict(title='Drivers', tickformat=',.0f', automargin=True),
                height=360,
                show_legend=True,
                legend_y=1.08,
            )
            driver_segment_fig.update_layout(
                yaxis2=dict(title='Revenue Share %', overlaying='y', side='right', tickformat='.1f', automargin=True)
            )
        else:
            driver_segment_fig = _empty_driver_figure('Driver Value Segmentation', 'Drivers')

        # Cohort heatmap
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

        # Engagement frequency (days between rentals)
        gap_summary = (
            driver_agg[['customer_id', 'avg_days_between_rentals']]
            .dropna()
            .sort_values('avg_days_between_rentals', ascending=False)
            .head(15)
        )

        if gap_summary.empty:
            driver_gap_fig = _empty_driver_figure('Avg Days Between Rentals', 'Days')
        else:
            driver_gap_fig = px.bar(
                gap_summary.sort_values('avg_days_between_rentals', ascending=True),
                x='avg_days_between_rentals',
                y='customer_id',
                orientation='h',
                title='Avg Days Between Rentals (Top 15 Drivers)',
                color_discrete_sequence=['#2C353B']
            )
            _apply_standard_figure_layout(
                driver_gap_fig,
                'Avg Days Between Rentals (Top 15 Drivers)',
                xaxis=dict(title='Days', tickformat='.1f', automargin=True),
                yaxis=dict(title='Customer ID', automargin=True),
                height=360,
            )

        # Top drivers table
        top_driver_df = driver_agg.sort_values(['rentals', 'revenue'], ascending=[False, False]).head(20).copy()
        driver_top_table_data = top_driver_df[
            ['customer_id', 'renter_name', 'first_rental_date', 'driver_tenure_days', 'rentals', 'revenue', 'rental_days']
        ].to_dict('records')

        # Storytelling summary
        monthly_new = active_new_monthly.sort_values('rental_month') if not active_new_monthly.empty else pd.DataFrame()
        if len(monthly_new) >= 2:
            latest_new = float(monthly_new['new_drivers'].iloc[-1])
            prev_new = float(monthly_new['new_drivers'].iloc[-2])
            if prev_new == 0:
                acquisition_text = f"New driver acquisition in the latest month is {latest_new:,.0f} drivers (no prior-month baseline)."
            else:
                pct_delta_new = ((latest_new - prev_new) / prev_new) * 100
                trend_word = 'increased' if pct_delta_new > 0 else ('decreased' if pct_delta_new < 0 else 'remained stable')
                acquisition_text = f"Driver acquisition {trend_word} {abs(pct_delta_new):.1f}% vs previous month."
        else:
            acquisition_text = 'Not enough monthly history to compare new-driver acquisition trend.'

        top_share = 0.0
        if total_segment_revenue and not top_driver_df.empty:
            top_share = (top_driver_df.head(10)['revenue'].sum() / total_segment_revenue) * 100

        retention_trend = 'improving' if active_2plus_pct >= 50 else 'declining'
        driver_insight = (
            f"{acquisition_text} Returning-driver rate is {active_2plus_pct:.1f}% and appears {retention_trend}. "
            f"High-value concentration: top 10 drivers contribute {top_share:.1f}% of selected-period revenue."
        )
    
    # Time analysis with data labels
    rentals_month_data = build_complete_monthly_series(filtered_df.groupby('year_month_dt').size().reset_index(name='count'), 'count')
    rentals_month = px.bar(rentals_month_data, x='year_month_dt', y='count', 
                           title='Rentals by Month', color_discrete_sequence=['#00708D'])
    rentals_month.update_traces(
        text=[f"{int(v)}" for v in rentals_month_data['count']],
        textposition="outside",
        textfont=dict(size=9, color='#00708D'),
        marker=dict(opacity=0.85),
        hovertemplate='<b>%{x|%b %Y}</b><br>Rentals: %{y}<extra></extra>'
    )
    _apply_standard_figure_layout(
        rentals_month,
        'Rentals by Month',
        xaxis=_monthly_time_axis(len(rentals_month_data)),
        yaxis=dict(tickformat=',.0f', title='Rentals', automargin=True),
        height=380,
    )

    rentals_dow_data = filtered_df.groupby('start_day_of_week').size().reset_index(name='count').sort_values('count', ascending=False)
    rentals_dow = px.bar(rentals_dow_data, x='start_day_of_week', y='count', 
                         title='Rentals by Day of Week', color_discrete_sequence=['#00708D'])
    rentals_dow.update_traces(
        text=[f"{int(v)}" for v in rentals_dow_data['count']],
        textposition="outside",
        textfont=dict(size=9, color='#00708D'),
        marker=dict(opacity=0.85),
        hovertemplate='%{x}<br>Rentals: %{y}<extra></extra>'
    )
    rentals_dow.update_layout(
        template='plotly_white',
        yaxis=dict(tickformat=',.0f', title='Rentals'),
        xaxis=dict(showgrid=False, title=''),
        hovermode='x unified'
    )

    rentals_hour_data = filtered_df.groupby('start_hour').size().reset_index(name='count').sort_values('start_hour')
    rentals_hour = px.bar(rentals_hour_data, x='start_hour', y='count',
                          title='Rentals by Hour', color_discrete_sequence=['#00708D'])
    rentals_hour.update_traces(
        text=[f"{int(v)}" for v in rentals_hour_data['count']],
        textposition="outside",
        textfont=dict(size=9, color='#00708D'),
        marker=dict(opacity=0.85),
        hovertemplate='Hour %{x}:00<br>Rentals: %{y}<extra></extra>'
    )
    rentals_hour.update_layout(
        template='plotly_white',
        yaxis=dict(tickformat=',.0f', title='Rentals'),
        xaxis=dict(showgrid=False, title='Hour of Day'),
        hovermode='x unified'
    )

    days_month_data = build_complete_monthly_series(filtered_df, 'rental_days')
    days_month = px.bar(days_month_data, x='year_month_dt', y='rental_days',
                        title='Rental Days by Month', color_discrete_sequence=['#00708D'])
    days_month.update_traces(
        text=[f"{v:.2f}" for v in days_month_data['rental_days']],
        textposition="outside",
        textfont=dict(size=9, color='#00708D'),
        marker=dict(opacity=0.85),
        hovertemplate='<b>%{x|%b %Y}</b><br>Days: %{y:.2f}<extra></extra>'
    )
    _apply_standard_figure_layout(
        days_month,
        'Rental Days by Month',
        xaxis=_monthly_time_axis(len(days_month_data)),
        yaxis=dict(tickformat='.2f', title='Rental Days', automargin=True),
        height=380,
    )

    rev_month_data = build_complete_monthly_series(filtered_df, 'total_to_charge')
    rev_month = px.bar(rev_month_data, x='year_month_dt', y='total_to_charge',
                       title='Revenue by Month', color_discrete_sequence=['#00708D'])
    rev_month.update_traces(
        text=[f"${v:,.2f}" for v in rev_month_data['total_to_charge']],
        textposition="outside",
        textfont=dict(size=9, color='#00708D'),
        marker=dict(opacity=0.85),
        hovertemplate='<b>%{x|%b %Y}</b><br>Revenue: $%{y:,.2f}<extra></extra>'
    )
    _apply_standard_figure_layout(
        rev_month,
        'Revenue by Month',
        xaxis=_monthly_time_axis(len(rev_month_data)),
        yaxis=dict(tickformat='$,.0f', title='Revenue', automargin=True),
        height=380,
    )
    
    # Monthly Comparison (Executive view)
    if filtered_df.empty:
        monthly_content = dbc.Alert("No data available for the selected filters", color="info")
    else:
        selected_month_message = None
        if not comparison_month or comparison_month not in filtered_df['year_month'].unique():
            latest = filtered_df['year_month_dt'].max()
            if pd.isna(latest):
                monthly_content = dbc.Alert("No valid month data available after filtering", color="info")
            else:
                comparison_month = latest.strftime('%Y-%m')
                selected_month_message = f"Showing latest available month: {comparison_month}"

        if 'monthly_content' not in locals():
            current_dt = pd.to_datetime(f"{comparison_month}-01", errors='coerce')
            if pd.isna(current_dt):
                monthly_content = dbc.Alert("Invalid month selected for comparison", color="warning")
            else:
                prev_dt = current_dt - pd.DateOffset(months=1)
                prev_month_str = prev_dt.strftime('%Y-%m')
                same_month_last_year_str = (current_dt - pd.DateOffset(years=1)).strftime('%Y-%m')

                def get_metric(as_month, metric):
                    if as_month is None:
                        return None
                    subset = filtered_df[filtered_df['year_month'] == as_month]
                    if subset.empty:
                        return None
                    if metric == 'rental_days':
                        return float(subset['rental_days'].sum())
                    if metric == 'rentals':
                        return float(len(subset))
                    if metric == 'revenue':
                        return float(subset['total_to_charge'].sum())
                    return None

                def format_value(value, is_currency=False):
                    if value is None or pd.isna(value):
                        return 'N/A'
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
                    ('rentals', 'Rentals', False),
                    ('revenue', 'Revenue', True),
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
                    yoy_val = get_metric(same_month_last_year_str, metric_key)

                    mom_diff, mom_pct, _, mom_arrow, mom_color, mom_pct_text = compute_change(current_val, prev_val)
                    yoy_diff, yoy_pct, _, yoy_arrow, yoy_color, yoy_pct_text = compute_change(current_val, yoy_val)

                    metric_results[metric_key] = {
                        'label': metric_label,
                        'current': current_val,
                        'mom_diff': mom_diff,
                        'mom_pct': mom_pct,
                        'is_currency': is_currency,
                    }

                    if mom_pct is not None:
                        mom_candidates.append((metric_key, abs(mom_pct)))

                    mom_diff_text = 'N/A' if mom_diff is None else (f"${mom_diff:+,.2f}" if is_currency else f"{mom_diff:+,.2f}")
                    yoy_diff_text = 'N/A' if yoy_diff is None else (f"${yoy_diff:+,.2f}" if is_currency else f"{yoy_diff:+,.2f}")

                    metric_cards.append((metric_key, dbc.Card(
                        dbc.CardBody([
                            html.Div(metric_label, className='kpi-label'),
                            html.Div(format_value(current_val, is_currency), className='kpi-value'),
                            html.Div(f"{mom_arrow} {mom_pct_text} vs last month", style={'color': mom_color, 'fontWeight': '600', 'fontSize': '0.92rem', 'marginTop': '8px', 'minHeight': '22px'}),
                            html.Div(f"Diff: {mom_diff_text}", style={'color': '#6b7280', 'fontSize': '0.85rem', 'minHeight': '20px'}),
                            html.Div(f"{yoy_arrow} {yoy_pct_text} vs last year", style={'color': yoy_color, 'fontWeight': '600', 'fontSize': '0.92rem', 'marginTop': '10px', 'minHeight': '22px'}),
                            html.Div(f"Diff: {yoy_diff_text}", style={'color': '#6b7280', 'fontSize': '0.85rem', 'minHeight': '20px'}),
                        ], className='monthly-kpi-card-body'),
                        className='kpi-card dashboard-kpi-card monthly-kpi-card'
                    )))

                critical_metric_key = None
                if mom_candidates:
                    critical_metric_key = sorted(mom_candidates, key=lambda x: x[1], reverse=True)[0][0]

                metric_card_map = {metric_key: card for metric_key, card in metric_cards}
                metric_order = [metric_key for metric_key, _, _ in metric_specs]
                sorted_by_change = [k for k, _ in sorted(mom_candidates, key=lambda x: x[1], reverse=True)]
                ordered_metric_keys = sorted_by_change + [k for k in metric_order if k not in sorted_by_change]

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
                            style={'border': '2px solid #00708D', 'borderRadius': '12px', 'boxShadow': '0 6px 18px rgba(0, 112, 141, 0.18)', 'backgroundColor': '#f8fcfe'}
                        )
                    styled_cards.append(
                        dbc.Col([
                            card_node
                        ], xs=12, xl=4, className='dashboard-kpi-col monthly-kpi-col')
                    )

                monthly_agg = (
                    filtered_df.groupby('year_month_dt')
                    .agg(rental_days=('rental_days', 'sum'),
                         rentals=('rental_id', 'count'),
                         revenue=('total_to_charge', 'sum'))
                    .reset_index()
                    .sort_values('year_month_dt')
                )
                monthly_agg = monthly_agg.tail(12)

                def build_trend_figure(value_col, title, is_currency=False):
                    fig = go.Figure()
                    if monthly_agg.empty:
                        return fig

                    marker_colors = ['#00708D' if pd.notna(x) and x != current_dt else '#d4420b' for x in monthly_agg['year_month_dt']]
                    fig.add_trace(go.Scatter(
                        x=monthly_agg['year_month_dt'],
                        y=monthly_agg[value_col],
                        mode='lines+markers',
                        line=dict(color='#00708D', width=2.5),
                        marker=dict(size=8, color=marker_colors),
                        hovertemplate='<b>%{x|%b %Y}</b><br>' + (f'{title}: $%{{y:,.2f}}' if is_currency else f'{title}: %{{y:,.2f}}') + '<extra></extra>'
                    ))
                    _apply_standard_figure_layout(
                        fig,
                        title,
                        xaxis=_monthly_time_axis(len(monthly_agg)),
                        yaxis=dict(showgrid=True, zeroline=False, tickformat='$,.0f' if is_currency else '.2f', title='', automargin=True),
                        height=360,
                    )
                    return fig

                rentals_trend_fig = build_trend_figure('rentals', 'Rentals (Last 12 Months)', is_currency=False)
                days_trend_fig = build_trend_figure('rental_days', 'Rental Days (Last 12 Months)', is_currency=False)
                revenue_trend_fig = build_trend_figure('revenue', 'Revenue (Last 12 Months)', is_currency=True)

                insight_lines = []
                if critical_metric_key is None:
                    insight_lines.append('Insufficient prior-period data to compute month-over-month changes for the selected filters.')
                else:
                    critical = metric_results[critical_metric_key]
                    if critical['mom_pct'] is not None and critical['mom_diff'] is not None:
                        direction_text = 'increased' if critical['mom_diff'] > 0 else ('decreased' if critical['mom_diff'] < 0 else 'remained stable')
                        diff_text = f"${critical['mom_diff']:+,.2f}" if critical['is_currency'] else f"{critical['mom_diff']:+,.2f}"
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

                monthly_content = html.Div([
                    html.Div('Monthly Performance Story', className='section-subtitle'),
                    html.Div(context_header, style={'fontSize': '0.95rem', 'color': '#4b5563', 'marginBottom': '10px'}),
                    html.Div(selected_month_message or '', style={'color': '#0b66a0', 'marginBottom': '8px', 'display': 'block' if selected_month_message else 'none'}),
                    html.Div(critical_title, className='critical-change-title'),

                    dbc.Row(styled_cards, className='g-3 dashboard-kpi-row monthly-kpi-row', style={'marginBottom': '14px'}),

                    dbc.Row([
                        dbc.Col(dcc.Graph(figure=rentals_trend_fig, className='dashboard-graph', config={'displayModeBar': False, 'responsive': True}), xs=12, xl=4, className='dashboard-graph-col'),
                        dbc.Col(dcc.Graph(figure=days_trend_fig, className='dashboard-graph', config={'displayModeBar': False, 'responsive': True}), xs=12, xl=4, className='dashboard-graph-col'),
                        dbc.Col(dcc.Graph(figure=revenue_trend_fig, className='dashboard-graph', config={'displayModeBar': False, 'responsive': True}), xs=12, xl=4, className='dashboard-graph-col'),
                    ], className='g-3 dashboard-chart-row'),

                    dbc.Alert(' '.join(insight_lines), color='light', style={'marginTop': '10px', 'border': '1px solid #e5e7eb'})
                ], className='monthly-story-card')
    
    if active_tab == 'dealer' and not dealer_agg.empty:
        dealer_revenue_fig = px.bar(
            dealer_agg.sort_values('total_revenue', ascending=False),
            x='station_name',
            y='total_revenue',
            title='Dealer Revenue Ranking',
            color_discrete_sequence=['#00708D']
        )
        dealer_revenue_fig.update_traces(
            text=[f"${v:,.2f}" for v in dealer_agg.sort_values('total_revenue', ascending=False)['total_revenue']],
            textposition="outside",
            textfont=dict(size=9, color='#00708D'),
            marker=dict(opacity=0.85),
            hovertemplate='<b>%{x}</b><br>Revenue: $%{y:,.2f}<extra></extra>'
        )
        dealer_revenue_fig.update_layout(
            template='plotly_white',
            yaxis=dict(tickformat='$,.0f', title='Total Revenue'),
            xaxis=dict(showgrid=False, title=''),
            hovermode='x unified'
        )

        dealer_days_fig = px.bar(
            dealer_agg.sort_values('rental_days', ascending=False),
            x='station_name',
            y='rental_days',
            title='Dealer Rental Days Ranking',
            color_discrete_sequence=['#00708D']
        )
        dealer_days_fig.update_traces(
            text=[f"{v:.2f}" for v in dealer_agg.sort_values('rental_days', ascending=False)['rental_days']],
            textposition="outside",
            textfont=dict(size=9, color='#00708D'),
            marker=dict(opacity=0.85),
            hovertemplate='<b>%{x}</b><br>Days: %{y:.2f}<extra></extra>'
        )
        dealer_days_fig.update_layout(
            template='plotly_white',
            yaxis=dict(tickformat='.2f', title='Total Rental Days'),
            xaxis=dict(showgrid=False, title=''),
            hovermode='x unified'
        )
    else:
        dealer_revenue_fig = go.Figure()
        dealer_days_fig = go.Figure()

    return (f"${total_rev:,.0f}", f"{total_rentals:,.0f}", f"{total_days:,.0f}", f"${avg_rev:.2f}", f"{avg_days:,.0f}", f"{avg_kms:,.0f}",
            trend_rev, trend_rentals, trend_days,
            projected_month_end_revenue, projected_month_end_rentals, projected_month_end_days,
            cum_revenue_summary, cum_rentals_summary, cum_days_summary,
            cum_revenue_fig, cum_rentals_fig, cum_days_fig,
            dealer_agg.to_dict('records'), vehicle_agg.to_dict('records'),
            top10_fig, mileage_scatter_fig,
            f"{mileage_count_15000:,}", f"{mileage_count_15_20:,}", f"{mileage_count_20:,}", f"{highest_mileage:,}",
            selected_summary_children, selected_summary_style,
            empty_state_style, card_15000_style, card_15_20_style, card_20_style,
            high_mileage_data,
            rental_data, driver_agg.to_dict('records'),
            driver_kpi_total, driver_kpi_new, driver_kpi_new_pct, driver_kpi_avg_tenure, driver_kpi_returning,
            driver_insight,
            driver_new_over_time_fig, driver_active_vs_new_fig, driver_tenure_bucket_fig, driver_segment_fig, driver_cohort_fig, driver_gap_fig,
            driver_top_table_data,
            rentals_month, rentals_dow, rentals_hour, days_month, rev_month, monthly_content, dealer_revenue_fig, dealer_days_fig)


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
            hovertemplate='<b>%{x}</b><br>Expenses: $%{y:,.2f}<extra></extra>'
        )
        dealer_fig.update_layout(
            template='plotly_white',
            yaxis=dict(tickformat='$,.0f', title='Total Expenses'),
            xaxis=dict(showgrid=False, title=''),
            hovermode='x unified'
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
    if not stacked_data.empty:
        stacked_fig = px.bar(
            stacked_data, x='Dealer Name', y='total', color='Work Category', custom_data=['Work Category'],
            title='Expenses by Dealer and Work Category', barmode='stack',
            color_discrete_sequence=px.colors.qualitative.Set2
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
            legend=dict(
                title='Work Category',
                itemclick='toggleothers',
                itemdoubleclick='toggle'
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
     Output('renter_filter', 'options'),
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
        [{'label': str(y), 'value': y} for y in sorted(df['start_year'].unique())],
        [{'label': m, 'value': m} for m in sorted(df['start_month_name'].unique())],
        [{'label': s, 'value': s} for s in fleet_status_values],
        df['rental_started_at_EST'].min().date(),
        df['rental_started_at_EST'].max().date(),
        [{'label': d, 'value': d} for d in sorted(inv_df['Dealer Name'].dropna().unique())],
        [{'label': c, 'value': c} for c in sorted(inv_df['Work Category'].dropna().unique())],
        [{'label': v, 'value': v} for v in sorted(inv_df['Vehicle'].dropna().unique())],
        [{'label': str(int(y)), 'value': int(y)} for y in sorted(inv_df['MY'].dropna().unique())],
        [{'label': s, 'value': s} for s in EXPENSE_UNIT_STATUS_OPTIONS],
        [{'label': str(y), 'value': y} for y in inv_sub_years],
        [{'label': m, 'value': m} for m in inv_sub_months],
        inv_date_min,
        inv_date_max,
    )


if __name__ == '__main__':
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', '8050'))
    debug = os.getenv('DEBUG', '').strip().lower() in {'1', 'true', 'yes'}

    if debug:
        app.run(debug=True, host=host, port=port)
    else:
        from waitress import serve
        serve(server, host=host, port=port, threads=8)