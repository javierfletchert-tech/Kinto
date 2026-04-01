# Rideshare Rental Performance Dashboard

An interactive business intelligence dashboard for analyzing rideshare rental performance data.

## Features

- **Executive Overview**: KPI cards and trend charts
- **Monthly Comparison**: MoM and YoY analysis with dynamic selection
- **Dealer Performance**: Revenue and rental days rankings with drill-down
- **Vehicle Performance**: Performance metrics by vehicle
- **Rental Details**: Filterable and sortable data table
- **Driver Analysis**: Metrics by renter
- **Issues Analysis**: Impact analysis of rentals with issues
- **Time Analysis**: Charts by month, day of week, and hour

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. (Optional) Override default data files with environment variables:
   ```bash
   RENTAL_FILE_PATH=/path/to/PastRentalDetails_2026-2-25.xlsx
   FLEET_FILE_PATH=/path/to/Kinto Fleet_3-19-26.xlsx
   INVOICE_FILE_PATH=/path/to/Invoices consolidated.xlsx
   ```
   If not provided, the app loads these files from the project folder.

3. Run the app:
   ```bash
   python app.py
   ```

4. Open http://127.0.0.1:5000/ in your browser.

## Publish to Azure (App Service)

1. Log in and create resources:
   ```bash
   az login
   az group create --name <RESOURCE_GROUP> --location <REGION>
   az appservice plan create --name <APP_PLAN> --resource-group <RESOURCE_GROUP> --sku B1 --is-linux
   az webapp create --resource-group <RESOURCE_GROUP> --plan <APP_PLAN> --name <APP_NAME> --runtime "PYTHON|3.11"
   ```

2. Set production startup command (Dash via Gunicorn):
   ```bash
   az webapp config set --resource-group <RESOURCE_GROUP> --name <APP_NAME> --startup-file "gunicorn --bind=0.0.0.0 --timeout 600 app:server"
   ```

3. Configure app settings (if your data files are stored outside the repo):
   ```bash
   az webapp config appsettings set --resource-group <RESOURCE_GROUP> --name <APP_NAME> --settings RENTAL_FILE_PATH="/home/site/wwwroot/PastRentalDetails_2026-2-25.xlsx" FLEET_FILE_PATH="/home/site/wwwroot/Kinto Fleet_3-19-26.xlsx" INVOICE_FILE_PATH="/home/site/wwwroot/Invoices consolidated.xlsx"
   ```

4. Deploy code from this folder:
   ```bash
   az webapp up --name <APP_NAME> --resource-group <RESOURCE_GROUP> --runtime "PYTHON|3.11"
   ```

5. Open your app:
   ```bash
   az webapp browse --resource-group <RESOURCE_GROUP> --name <APP_NAME>
   ```

## Data Requirements

The Excel file should contain the following columns (based on the actual data structure):
- `user_groups`
- `Pre-Tax Charge`
- `rental_started_at_EST` (start datetime)
- `rental_end_datetime_EST` (end datetime)
- `total_to_charge`
- `exempted_payment_ids`
- `station_name`
- `vehicle_type`
- `vehicle_id`
- `license_plate_number`
- `renter_name`
- `reservation_status`
- `kms_traveled`
- `rental_id`

The app automatically filters for "Rideshare Drivers", removes negative Pre-Tax Charges, and drops rows with invalid dates.

## Sample Data

A sample dataset (`sample_rentals.xlsx`) is included for testing. The app currently uses this sample data. Uncomment the original path and comment the sample path to use your actual data.

## Notes

- Ensure the Excel file is accessible from the script's working directory.
- Date columns are automatically converted to datetime.
- Filters are applied globally across all visualizations.
- The monthly comparison requires selecting a year-month from the dropdown.

## Daily Data Update (One Command)

Render cannot read files directly from your local PC. To publish new daily Excel data to Render:

1. Run this command from `rental-analysis-app`:
   ```powershell
   .\scripts\publish-data.ps1 \
     -RentalSource "C:\path\PastRentalDetails_2026-2-26.xlsx" \
     -FleetSource "C:\path\Kinto Fleet_3-20-26.xlsx" \
     -InvoiceSource "C:\path\Invoices consolidated.xlsx"
   ```

2. Or if you already replaced files in the repo folder, run:
   ```powershell
   .\scripts\publish-data.ps1
   ```

What it does:
- Copies source files into project data files
- Commits only the three Excel files
- Pushes to `main` (Render auto-deploy)

Optional:
- Add `-TriggerRenderHook` and set env var `RENDER_DEPLOY_HOOK_URL` to force a deploy hook call.