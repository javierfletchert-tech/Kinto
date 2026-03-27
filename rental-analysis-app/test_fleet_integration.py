#!/usr/bin/env python3
"""Quick test to verify fleet data integration"""

import app

print('✓ Import successful')
print()
print(f'Dataset shape: {app.df.shape}')
print(f'Total rental records: {len(app.df)}')
print()
print(f'VIN non-null count: {app.df["VIN"].notna().sum()}')
print(f'VIN match rate: {app.df["VIN"].notna().sum() / len(app.df) * 100:.1f}%')
print()
print(f'5VIN non-null count: {app.df["5VIN"].notna().sum()}')
print()
print('Sample VIN values:')
print(app.df[app.df['VIN'].notna()][['license_plate_number', 'VIN', '5VIN']].head(10))
print()
print('Unique VINs available for filtering:', len(app.df['VIN'].dropna().unique()))
