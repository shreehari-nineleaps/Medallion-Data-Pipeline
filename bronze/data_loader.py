import os
import logging
import httplib2
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from google_auth_httplib2 import AuthorizedHttp
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pathlib import Path
import sys

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG, GOOGLE_SHEETS_CONFIG, SHEET_RANGES, LOG_CONFIG

# Set up logging
log_dir = Path(__file__).parent.parent / LOG_CONFIG['log_dir']
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['level']),
    format=LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(log_dir / 'data_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Create and return a PostgreSQL connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return None


def get_sheets_service():
    """Create and return Google Sheets service."""
    try:
        creds = Credentials.from_service_account_file(
            GOOGLE_SHEETS_CONFIG['credentials_path'],
            scopes=GOOGLE_SHEETS_CONFIG['scopes']
        )
        unverified_http = httplib2.Http(disable_ssl_certificate_validation=True)
        authorized_http = AuthorizedHttp(creds, http=unverified_http)
        service = build("sheets", "v4", http=authorized_http)
        logger.info("Google Sheets service created successfully")
        return service
    except Exception as e:
        logger.error(f"Error creating Google Sheets service: {e}")
        return None


def fetch_sheet_data(service, sheet_range):
    """Fetch data from Google Sheets."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEETS_CONFIG['spreadsheet_id'],
            range=sheet_range
        ).execute()

        rows = result.get("values", [])
        if not rows:
            logger.warning(f"No data found in range: {sheet_range}")
            return pd.DataFrame()

        df = pd.DataFrame(rows[1:], columns=rows[0])
        logger.info(f"Fetched {len(df):,} rows from {sheet_range}")
        return df
    except Exception as e:
        logger.error(f"Error fetching data from {sheet_range}: {e}")
        return pd.DataFrame()


def load_suppliers_to_bronze(df):
    """Load suppliers data to PostgreSQL bronze.suppliers table."""
    if df.empty:
        logger.warning("No suppliers data to load")
        return False

    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO bronze.suppliers (supplier_name, contact_email, phone_number)
        VALUES (%s, %s, %s)
        ON CONFLICT (contact_email) DO UPDATE SET
            supplier_name = EXCLUDED.supplier_name,
            phone_number = EXCLUDED.phone_number,
            updated_at = CURRENT_TIMESTAMP;
        """

        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row['supplier_name'],
                row['contact_email'],
                row['phone_number']
            ))

        conn.commit()
        logger.info(f"Successfully loaded {len(df):,} suppliers to bronze layer")
        return True

    except psycopg2.Error as e:
        logger.error(f"Error loading suppliers data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_warehouses_to_bronze(df):
    """Load warehouses data to PostgreSQL bronze.warehouses table."""
    if df.empty:
        logger.warning("No warehouses data to load")
        return False

    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO bronze.warehouses (warehouse_name, location_city, storage_capacity)
        VALUES (%s, %s, %s)
        ON CONFLICT (warehouse_name) DO UPDATE SET
            location_city = EXCLUDED.location_city,
            storage_capacity = EXCLUDED.storage_capacity,
            updated_at = CURRENT_TIMESTAMP;
        """

        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row['warehouse_name'],
                row['location_city'],
                int(row['storage_capacity'])
            ))

        conn.commit()
        logger.info(f"Successfully loaded {len(df):,} warehouses to bronze layer")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading warehouses data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_products_to_bronze(df):
    """Load products data to PostgreSQL bronze.products table."""
    if df.empty:
        logger.warning("No products data to load")
        return False

    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO bronze.products (sku, product_name, unit_cost, supplier_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (sku) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            unit_cost = EXCLUDED.unit_cost,
            supplier_id = EXCLUDED.supplier_id,
            updated_at = CURRENT_TIMESTAMP;
        """

        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row['sku'],
                row['product_name'],
                float(row['unit_cost']),
                int(row['supplier_id'])
            ))

        conn.commit()
        logger.info(f"Successfully loaded {len(df):,} products to bronze layer")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading products data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_inventory_to_bronze(df):
    """Load inventory data to PostgreSQL bronze.inventory table."""
    if df.empty:
        logger.warning("No inventory data to load")
        return False

    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO bronze.inventory (product_id, warehouse_id, quantity_on_hand, last_stocked_date)
        VALUES (%s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            # Convert last_stocked_date to proper format
            last_stocked = pd.to_datetime(row['last_stocked_date']).strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute(insert_query, (
                int(row['product_id']),
                int(row['warehouse_id']),
                int(row['quantity_on_hand']),
                last_stocked
            ))

        conn.commit()
        logger.info(f"Successfully loaded {len(df):,} inventory records to bronze layer")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading inventory data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_shipments_to_bronze(df):
    """Load shipments data to PostgreSQL bronze.shipments table."""
    if df.empty:
        logger.warning("No shipments data to load")
        return False

    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO bronze.shipments (product_id, warehouse_id, quantity_shipped,
                                    shipment_date, destination, status, weight_kg)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            # Convert shipment_date to proper format
            shipment_date = pd.to_datetime(row['shipment_date']).strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute(insert_query, (
                int(row['product_id']),
                int(row['warehouse_id']),
                int(row['quantity_shipped']),
                shipment_date,
                row['destination'],
                row['status'],
                float(row['weight_kg'])
            ))

        conn.commit()
        logger.info(f"Successfully loaded {len(df):,} shipment records to bronze layer")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading shipments data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_sheet_to_bronze(sheet_name):
    """Load data from a single Google Sheet to bronze layer."""
    logger.info(f"📥 Loading {sheet_name} data to bronze layer...")

    # Get Google Sheets service
    service = get_sheets_service()
    if not service:
        return False

    # Fetch data from sheet
    sheet_range = SHEET_RANGES.get(sheet_name)
    if not sheet_range:
        logger.error(f"No range defined for sheet: {sheet_name}")
        return False

    df = fetch_sheet_data(service, sheet_range)
    if df.empty:
        logger.warning(f"No data found for {sheet_name}")
        return False

    # Load data to bronze layer based on sheet type
    load_functions = {
        'suppliers': load_suppliers_to_bronze,
        'products': load_products_to_bronze,
        'warehouses': load_warehouses_to_bronze,
        'inventory': load_inventory_to_bronze,
        'shipments': load_shipments_to_bronze
    }

    load_function = load_functions.get(sheet_name)
    if not load_function:
        logger.error(f"No load function defined for sheet: {sheet_name}")
        return False

    success = load_function(df)
    if success:
        logger.info(f"✅ Successfully loaded {sheet_name} data to bronze layer")
    else:
        logger.error(f"❌ Failed to load {sheet_name} data to bronze layer")

    return success


def load_all_data_to_bronze():
    """Load all data from Google Sheets to bronze layer in proper order."""
    logger.info("🚀 Starting Bronze Layer Data Loading Pipeline...")
    logger.info("=" * 80)

    # Define loading order (suppliers and warehouses first, then products, inventory, shipments)
    load_order = ['suppliers', 'warehouses', 'products', 'inventory', 'shipments']

    results = {}
    for sheet_name in load_order:
        logger.info(f"\n📊 Processing {sheet_name}...")
        logger.info("-" * 50)

        success = load_sheet_to_bronze(sheet_name)
        results[sheet_name] = success

        if not success:
            logger.error(f"❌ Failed to load {sheet_name}. Continuing with next sheet...")

    # Summary
    logger.info(f"\n🎯 BRONZE LAYER LOADING SUMMARY")
    logger.info("=" * 80)

    successful_loads = 0
    for sheet_name, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        logger.info(f"  {sheet_name:<12}: {status}")
        if success:
            successful_loads += 1

    total_loads = len(results)
    logger.info(f"\n📈 Overall: {successful_loads}/{total_loads} sheets loaded successfully")

    if successful_loads == total_loads:
        logger.info("🎉 All data loaded successfully to bronze layer!")
        return True
    else:
        logger.warning("⚠️  Some data loads failed. Check logs above.")
        return False


def verify_bronze_data():
    """Verify data was loaded correctly by checking record counts."""
    conn = get_db_connection()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        tables = ['suppliers', 'products', 'warehouses', 'inventory', 'shipments']

        logger.info("\n📊 Bronze Layer Record Counts:")
        logger.info("=" * 50)

        total_records = 0
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM bronze.{table}")
            count = cursor.fetchone()[0]
            logger.info(f"  {table:<12}: {count:>8,} records")
            total_records += count

        logger.info("-" * 50)
        logger.info(f"  {'TOTAL':<12}: {total_records:>8,} records")

        # Show sample data from each table
        logger.info(f"\n🔍 Sample Data Preview:")
        logger.info("=" * 50)

        for table in tables:
            cursor.execute(f"SELECT * FROM bronze.{table} LIMIT 1")
            sample = cursor.fetchone()
            if sample:
                logger.info(f"  {table}: Found sample record ✅")
            else:
                logger.warning(f"  {table}: No sample data found ⚠️")

    except psycopg2.Error as e:
        logger.error(f"Error verifying data: {e}")
    finally:
        cursor.close()
        conn.close()


def main():
    """Main function to run the bronze layer data loading pipeline."""
    logger.info("🥉 MEDALLION BRONZE LAYER - DATA LOADER")
    logger.info("=" * 80)
    logger.info("Loading raw data from Google Sheets to PostgreSQL")
    logger.info("=" * 80)

    # Load all data to bronze layer
    success = load_all_data_to_bronze()

    # Verify data was loaded
    verify_bronze_data()

    logger.info("=" * 80)
    if success:
        logger.info("🎉 Bronze layer data loading completed successfully!")
        logger.info("Next steps:")
        logger.info("  - Data is now available in bronze schema")
        logger.info("  - Silver layer transformations can be applied")
        logger.info("  - Gold layer aggregations can be created")
    else:
        logger.error("❌ Bronze layer data loading completed with errors!")
        logger.info("Check logs above for details on failed operations")

    logger.info("=" * 80)
    return success


if __name__ == "__main__":
    main()
