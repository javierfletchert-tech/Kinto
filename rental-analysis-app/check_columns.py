import pandas as pd

file_path = r"C:\Users\fletchj\VS Studio\rental-analysis-app\PastRentalDetails_2026-2-25.xlsx"
df = pd.read_excel(file_path)

print("Columns in the Excel file:")
print(df.columns.tolist())

print("\nFirst few rows:")
print(df.head())

print("\nData types:")
print(df.dtypes)