import os
import logging
import httplib2
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from google_auth_httplib2 import AuthorizedHttp
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from config import DB_CONFIG, GOOGLE_SHEETS_CONFIG, SHEET_RANGES

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_pipeline.log'),
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
        logger.info(f"Fetched {len(df)} rows from {sheet_range}")
        return df
    except Exception as e:
        logger.error(f"Error fetching data from {sheet_range}: {e}")
        return pd.DataFrame()

def push_suppliers_to_db(df):
    """Push suppliers data to PostgreSQL bronze.suppliers table."""
    if df.empty:
        logger.warning("No suppliers data to push")
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
        logger.info(f"Successfully inserted/updated {len(df)} suppliers")
        return True

    except psycopg2.Error as e:
        logger.error(f"Error inserting suppliers data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def push_products_to_db(df):
    """Push products data to PostgreSQL bronze.products table."""
    if df.empty:
        logger.warning("No products data to push")
        return False

    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        # First, get supplier mapping
        cursor.execute("SELECT supplier_name, supplier_id FROM bronze.suppliers")
        supplier_map = {row[0]: row[1] for row in cursor.fetchall()}

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
            supplier_id = supplier_map.get(row['supplier_name'])
            if supplier_id is None:
                logger.warning(f"Supplier '{row['supplier_name']}' not found in database")
                continue

            cursor.execute(insert_query, (
                row['sku'],
                row['product_name'],
                float(row['unit_cost']),
                supplier_id
            ))

        conn.commit()
        logger.info(f"Successfully inserted/updated {len(df)} products")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error inserting products data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def push_warehouses_to_db(df):
    """Push warehouses data to PostgreSQL bronze.warehouses table."""
    if df.empty:
        logger.warning("No warehouses data to push")
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
        logger.info(f"Successfully inserted/updated {len(df)} warehouses")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error inserting warehouses data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def push_inventory_to_db(df):
    """Push inventory data to PostgreSQL bronze.inventory table."""
    if df.empty:
        logger.warning("No inventory data to push")
        return False

    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        # Get product and warehouse mappings
        cursor.execute("SELECT sku, product_id FROM bronze.products")
        product_map = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute("SELECT warehouse_name, warehouse_id FROM bronze.warehouses")
        warehouse_map = {row[0]: row[1] for row in cursor.fetchall()}

        insert_query = """
        INSERT INTO bronze.inventory (product_id, warehouse_id, quantity_on_hand, last_stocked_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (product_id, warehouse_id) DO UPDATE SET
            quantity_on_hand = EXCLUDED.quantity_on_hand,
            last_stocked_date = EXCLUDED.last_stocked_date,
            updated_at = CURRENT_TIMESTAMP;
        """

        for _, row in df.iterrows():
            product_id = product_map.get(row['sku'])
            warehouse_id = warehouse_map.get(row['warehouse_name'])

            if product_id is None:
                logger.warning(f"Product with SKU '{row['sku']}' not found")
                continue
            if warehouse_id is None:
                logger.warning(f"Warehouse '{row['warehouse_name']}' not found")
                continue

            # Convert last_stocked_date to proper format
            last_stocked = pd.to_datetime(row['last_stocked_date']).strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute(insert_query, (
                product_id,
                warehouse_id,
                int(row['quantity_on_hand']),
                last_stocked
            ))

        conn.commit()
        logger.info(f"Successfully inserted/updated {len(df)} inventory records")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error inserting inventory data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def push_shipments_to_db(df):
    """Push shipments data to PostgreSQL bronze.shipments table."""
    if df.empty:
        logger.warning("No shipments data to push")
        return False

    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        # Get product and warehouse mappings
        cursor.execute("SELECT sku, product_id FROM bronze.products")
        product_map = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute("SELECT warehouse_name, warehouse_id FROM bronze.warehouses")
        warehouse_map = {row[0]: row[1] for row in cursor.fetchall()}

        insert_query = """
        INSERT INTO bronze.shipments (product_id, warehouse_id, quantity_shipped,
                                    shipment_date, destination, status, weight_kg)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            product_id = product_map.get(row['sku'])
            warehouse_id = warehouse_map.get(row['warehouse_name'])

            if product_id is None:
                logger.warning(f"Product with SKU '{row['sku']}' not found")
                continue
            if warehouse_id is None:
                logger.warning(f"Warehouse '{row['warehouse_name']}' not found")
                continue

            # Convert shipment_date to proper format
            shipment_date = pd.to_datetime(row['shipment_date']).strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute(insert_query, (
                product_id,
                warehouse_id,
                int(row['quantity_shipped']),
                shipment_date,
                row['destination'],
                row['status'],
                float(row['weight_kg'])
            ))

        conn.commit()
        logger.info(f"Successfully inserted {len(df)} shipment records")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error inserting shipments data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def load_single_sheet(sheet_name):
    """Load data from a single Google Sheet to database."""
    logger.info(f"Starting to load {sheet_name} data...")

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

    # Push data to database based on sheet type
    push_functions = {
        'suppliers': push_suppliers_to_db,
        'products': push_products_to_db,
        'warehouses': push_warehouses_to_db,
        'inventory': push_inventory_to_db,
        'shipments': push_shipments_to_db
    }

    push_function = push_functions.get(sheet_name)
    if not push_function:
        logger.error(f"No push function defined for sheet: {sheet_name}")
        return False

    success = push_function(df)
    if success:
        logger.info(f"Successfully loaded {sheet_name} data")
    else:
        logger.error(f"Failed to load {sheet_name} data")

    return success

def load_all_data():
    """Load all data from Google Sheets to database in proper order."""
    logger.info("Starting full data pipeline...")

    # Define loading order (suppliers and warehouses first, then products, inventory, shipments)
    load_order = ['suppliers', 'warehouses', 'products', 'inventory', 'shipments']

    results = {}
    for sheet_name in load_order:
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing {sheet_name}...")
        logger.info(f"{'='*50}")

        success = load_single_sheet(sheet_name)
        results[sheet_name] = success

        if not success:
            logger.error(f"Failed to load {sheet_name}. Continuing with next sheet...")

    # Summary
    logger.info(f"\n{'='*50}")
    logger.info("DATA PIPELINE SUMMARY")
    logger.info(f"{'='*50}")

    for sheet_name, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{sheet_name:<12}: {status}")

    successful_loads = sum(results.values())
    total_loads = len(results)

    logger.info(f"\nOverall: {successful_loads}/{total_loads} sheets loaded successfully")

    if successful_loads == total_loads:
        logger.info("🎉 All data loaded successfully!")
        return True
    else:
        logger.warning("⚠️  Some data loads failed. Check logs above.")
        return False

def verify_data_counts():
    """Verify data was loaded correctly by checking record counts."""
    conn = get_db_connection()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        tables = ['suppliers', 'products', 'warehouses', 'inventory', 'shipments']

        logger.info("\nDatabase Record Counts:")
        logger.info("-" * 30)

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM bronze.{table}")
            count = cursor.fetchone()[0]
            logger.info(f"{table:<12}: {count:>6} records")

    except psycopg2.Error as e:
        logger.error(f"Error verifying data counts: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function to run the data pipeline."""
    logger.info("🚀 Starting Supply Chain Data Pipeline")
    logger.info("=" * 60)

    # Load all data
    success = load_all_data()

    # Verify data was loaded
    verify_data_counts()

    if success:
        logger.info("\n🎉 Data pipeline completed successfully!")
    else:
        logger.error("\n❌ Data pipeline completed with errors!")

    return success

if __name__ == "__main__":
    main()
