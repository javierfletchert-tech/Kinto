#!/usr/bin/env python3
"""
Script to update app.py to integrate fleet data
Reads the current app.py and modifies it to include VIN data
"""

import re

# Read the current app.py
with open('app.py', 'r') as f:
    content = f.read()

# 1. Replace the file loading section
old_file_loading = '''import pandas as pd
import dash
from dash import html, dcc, Input, Output, State, dash_table
from dash.dash_table.Format import Format, Scheme
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

file_path = r"C:\\Users\\fletchj\\VS Studio\\rental-analysis-app\\PastRentalDetails_2026-2-25.xlsx"
df = pd.read_excel(file_path)

# Data cleaning and processing
df = df[df['user_groups'] == "Rideshare Drivers"]
df = df[df['Pre-Tax Charge'] >= 0]

# Assume date columns are 'rental_started_at_EST' and 'rental_end_datetime_EST' - adjust if different
date_cols = ['rental_started_at_EST', 'rental_end_datetime_EST']  # Add all date/time columns
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
df['start_hour'] = df['rental_started_at_EST'].dt.hour'''

new_file_loading = '''import pandas as pd
import dash
from dash import html, dcc, Input, Output, State, dash_table
from dash.dash_table.Format import Format, Scheme
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Load rental data
rental_file_path = r"C:\\Users\\fletchj\\VS Studio\\rental-analysis-app\\PastRentalDetails_2026-2-25.xlsx"
df = pd.read_excel(rental_file_path)

# Load fleet data
fleet_file_path = r"C:\\Users\\fletchj\\VS Studio\\rental-analysis-app\\Kinto Fleet_3-19-26.xlsx"
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
df = df.drop(columns=['license_plate_normalized', 'plate_normalized'])'''

content = content.replace(old_file_loading, new_file_loading)
print("✓ Updated file loading and fleet merge")

# 2. Add VIN filter to the UI after License Plate filter
# Find the License Plate filter section and add VIN filter after it
license_plate_section = '''            dbc.Col([
                html.Label("License Plate"),
                dcc.Dropdown(
                    id='license_plate_filter',
                    options=[{'label': l, 'value': l} for l in sorted(df['license_plate_number'].unique())],
                    multi=True,
                    placeholder="Select license plates"
                )
            ], width=3),
        ]),
        dbc.Row(['''

vin_filter_addition = '''            dbc.Col([
                html.Label("License Plate"),
                dcc.Dropdown(
                    id='license_plate_filter',
                    options=[{'label': l, 'value': l} for l in sorted(df['license_plate_number'].unique())],
                    multi=True,
                    placeholder="Select license plates"
                )
            ], width=3),
            dbc.Col([
                html.Label("VIN"),
                dcc.Dropdown(
                    id='vin_filter',
                    options=[{'label': v, 'value': v} for v in sorted([x for x in df['VIN'].unique() if pd.notna(x)])],
                    multi=True,
                    placeholder="Select VINs"
                )
            ], width=3),
        ]),
        dbc.Row(['''

content = content.replace(license_plate_section, vin_filter_addition)
print("✓ Added VIN filter to UI")

# 3. Add VIN input to the main callback
# Find the main callback definition
old_callback_inputs = '''    [Input('station_filter', 'value'),
     Input('vehicle_type_filter', 'value'),
     Input('vehicle_id_filter', 'value'),
     Input('license_plate_filter', 'value'),
     Input('renter_filter', 'value'),
     Input('year_filter', 'value'),
     Input('month_filter', 'value'),
     Input('status_filter', 'value'),
     Input('date_range', 'start_date'),
     Input('date_range', 'end_date'),
    Input('comparison_month', 'value'),
    Input('main-tabs', 'value')]
)
def update_all(stations, vehicle_types, vehicle_ids, plates, renters, years, months, statuses, start_date, end_date, comparison_month, active_tab):'''

new_callback_inputs = '''    [Input('station_filter', 'value'),
     Input('vehicle_type_filter', 'value'),
     Input('vehicle_id_filter', 'value'),
     Input('license_plate_filter', 'value'),
     Input('renter_filter', 'value'),
     Input('year_filter', 'value'),
     Input('month_filter', 'value'),
     Input('status_filter', 'value'),
     Input('date_range', 'start_date'),
     Input('date_range', 'end_date'),
    Input('comparison_month', 'value'),
    Input('main-tabs', 'value'),
    Input('vin_filter', 'value')]
)
def update_all(stations, vehicle_types, vehicle_ids, plates, renters, years, months, statuses, start_date, end_date, comparison_month, active_tab, vins):'''

content = content.replace(old_callback_inputs, new_callback_inputs)
print("✓ Added VIN input to callback")

# 4. Add VIN filtering logic after the plate filtering
old_filtering = '''    if plates:
        filtered_df = filtered_df[filtered_df['license_plate_number'].isin(plates)]
    if renters:'''

new_filtering = '''    if plates:
        filtered_df = filtered_df[filtered_df['license_plate_number'].isin(plates)]
    if vins:
        filtered_df = filtered_df[filtered_df['VIN'].isin(vins)]
    if renters:'''

content = content.replace(old_filtering, new_filtering)
print("✓ Added VIN filtering logic")

# 5. Update rental table to include VIN columns
old_rental_columns = '''                {'name': 'Rental ID', 'id': 'rental_id', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Renter Name', 'id': 'renter_name'},
                {'name': 'Station Name', 'id': 'station_name'},
                {'name': 'Vehicle ID', 'id': 'vehicle_id'},
                {'name': 'License Plate', 'id': 'license_plate_number'},
                {'name': 'Rental Days', 'id': 'rental_days', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'KMs Traveled', 'id': 'kms_traveled', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Total to Charge', 'id': 'total_to_charge', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},'''

new_rental_columns = '''                {'name': 'Rental ID', 'id': 'rental_id', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Renter Name', 'id': 'renter_name'},
                {'name': 'Station Name', 'id': 'station_name'},
                {'name': 'Vehicle ID', 'id': 'vehicle_id'},
                {'name': 'License Plate', 'id': 'license_plate_number'},
                {'name': 'VIN', 'id': 'VIN'},
                {'name': '5VIN', 'id': '5VIN'},
                {'name': 'Rental Days', 'id': 'rental_days', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'KMs Traveled', 'id': 'kms_traveled', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},
                {'name': 'Total to Charge', 'id': 'total_to_charge', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},'''

content = content.replace(old_rental_columns, new_rental_columns)
print("✓ Updated rental table columns")

# 6. Update rental data extraction to include VIN
old_rental_data = '''    # Rental table
    rental_data = filtered_df[['rental_id', 'renter_name', 'station_name', 'vehicle_id', 'license_plate_number', 'rental_days', 'kms_traveled', 'total_to_charge']].to_dict('records')'''

new_rental_data = '''    # Rental table
    rental_data = filtered_df[['rental_id', 'renter_name', 'station_name', 'vehicle_id', 'license_plate_number', 'VIN', '5VIN', 'rental_days', 'kms_traveled', 'total_to_charge']].to_dict('records')'''

content = content.replace(old_rental_data, new_rental_data)
print("✓ Updated rental data extraction")

# 7. Update vehicle table to include VIN columns
old_vehicle_columns = '''                {'name': 'Vehicle ID', 'id': 'vehicle_id'},
                {'name': 'License Plate', 'id': 'license_plate_number'},
                {'name': 'Vehicle Type', 'id': 'vehicle_type'},
                {'name': 'Rentals', 'id': 'rentals', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},'''

new_vehicle_columns = '''                {'name': 'Vehicle ID', 'id': 'vehicle_id'},
                {'name': 'License Plate', 'id': 'license_plate_number'},
                {'name': 'VIN', 'id': 'VIN'},
                {'name': '5VIN', 'id': '5VIN'},
                {'name': 'Vehicle Type', 'id': 'vehicle_type'},
                {'name': 'Rentals', 'id': 'rentals', 'type': 'numeric', 'format': Format(precision=2, scheme=Scheme.fixed)},'''

content = content.replace(old_vehicle_columns, new_vehicle_columns)
print("✓ Updated vehicle table columns")

# 8. Update vehicle aggregation to include VIN
old_vehicle_agg = '''    # Vehicle table
    vehicle_agg = filtered_df.groupby(['vehicle_id', 'license_plate_number', 'vehicle_type']).agg({
        'rental_id': 'count',
        'rental_days': 'sum',
        'total_to_charge': 'sum',
        'kms_traveled': 'mean'
    }).reset_index()
    vehicle_agg.columns = ['vehicle_id', 'license_plate_number', 'vehicle_type', 'rentals', 'rental_days', 'revenue', 'avg_kms']
    vehicle_agg['avg_revenue'] = vehicle_agg['revenue'] / vehicle_agg['rentals']'''

new_vehicle_agg = '''    # Vehicle table
    vehicle_agg = filtered_df.groupby(['vehicle_id', 'license_plate_number', 'VIN', '5VIN', 'vehicle_type']).agg({
        'rental_id': 'count',
        'rental_days': 'sum',
        'total_to_charge': 'sum',
        'kms_traveled': 'mean'
    }).reset_index()
    vehicle_agg.columns = ['vehicle_id', 'license_plate_number', 'VIN', '5VIN', 'vehicle_type', 'rentals', 'rental_days', 'revenue', 'avg_kms']
    vehicle_agg['avg_revenue'] = vehicle_agg['revenue'] / vehicle_agg['rentals']'''

content = content.replace(old_vehicle_agg, new_vehicle_agg)
print("✓ Updated vehicle aggregation")

# Write the updated content back
with open('app.py', 'w') as f:
    f.write(content)

print("\n✅ Successfully updated app.py with fleet data integration!")
print("   - Added fleet data loading from Kinto Fleet_3-19-26.xlsx")
print("   - Merged rental and fleet data on license plate")
print("   - Added VIN and 5VIN columns")
print("   - Added VIN global filter")
print("   - Updated rental and vehicle tables with VIN fields")
