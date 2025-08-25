import logging
import httplib2
import pandas as pd
import psycopg2

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

        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM bronze.suppliers")
        initial_count = cursor.fetchone()[0]

        # Use UPSERT to prevent duplicates
        upsert_query = """
        INSERT INTO bronze.suppliers (supplier_id, supplier_name, contact_email, phone_number)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (supplier_id) DO UPDATE SET
            supplier_name = EXCLUDED.supplier_name,
            contact_email = EXCLUDED.contact_email,
            phone_number = EXCLUDED.phone_number,
            updated_at = CURRENT_TIMESTAMP
        """

        for _, row in df.iterrows():
            cursor.execute(upsert_query, (
                int(row['supplier_id']),
                row['supplier_name'],
                row['contact_email'],
                row['phone_number']
            ))

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM bronze.suppliers")
        final_count = cursor.fetchone()[0]

        inserted = final_count - initial_count
        updated = len(df) - inserted

        conn.commit()
        logger.info(f"Successfully processed {len(df):,} suppliers: {inserted:,} inserted, {updated:,} updated")
        return True

    except (psycopg2.Error, ValueError) as e:
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

        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM bronze.warehouses")
        initial_count = cursor.fetchone()[0]

        # Use UPSERT to prevent duplicates
        upsert_query = """
        INSERT INTO bronze.warehouses (warehouse_id, warehouse_name, city, region, storage_capacity)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (warehouse_id) DO UPDATE SET
            warehouse_name = EXCLUDED.warehouse_name,
            city = EXCLUDED.city,
            region = EXCLUDED.region,
            storage_capacity = EXCLUDED.storage_capacity,
            updated_at = CURRENT_TIMESTAMP
        """

        for _, row in df.iterrows():
            cursor.execute(upsert_query, (
                int(row['warehouse_id']),
                row['warehouse_name'],
                row['city'],
                row['region'],
                int(row['storage_capacity'])
            ))

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM bronze.warehouses")
        final_count = cursor.fetchone()[0]

        inserted = final_count - initial_count
        updated = len(df) - inserted

        conn.commit()
        logger.info(f"Successfully processed {len(df):,} warehouses: {inserted:,} inserted, {updated:,} updated")
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

        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM bronze.products")
        initial_count = cursor.fetchone()[0]

        # Use UPSERT to prevent duplicates
        upsert_query = """
        INSERT INTO bronze.products (product_id, product_name, unit_cost, selling_price, supplier_id, product_category, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            unit_cost = EXCLUDED.unit_cost,
            selling_price = EXCLUDED.selling_price,
            supplier_id = EXCLUDED.supplier_id,
            product_category = EXCLUDED.product_category,
            status = EXCLUDED.status,
            updated_at = CURRENT_TIMESTAMP
        """

        for _, row in df.iterrows():
            cursor.execute(upsert_query, (
                int(row['product_id']),
                row['product_name'],
                float(row['unit_cost']),
                float(row['selling_price']),
                int(row['supplier_id']),
                row['product_category'],
                row.get('status', 'active')
            ))

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM bronze.products")
        final_count = cursor.fetchone()[0]

        inserted = final_count - initial_count
        updated = len(df) - inserted

        conn.commit()
        logger.info(f"Successfully processed {len(df):,} products: {inserted:,} inserted, {updated:,} updated")
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

        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM bronze.inventory")
        initial_count = cursor.fetchone()[0]

        # Use UPSERT to prevent duplicates
        upsert_query = """
        INSERT INTO bronze.inventory (inventory_id, product_id, warehouse_id, quantity_on_hand, last_stocked_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (inventory_id) DO UPDATE SET
            product_id = EXCLUDED.product_id,
            warehouse_id = EXCLUDED.warehouse_id,
            quantity_on_hand = EXCLUDED.quantity_on_hand,
            last_stocked_date = EXCLUDED.last_stocked_date,
            updated_at = CURRENT_TIMESTAMP
        """

        for _, row in df.iterrows():
            # Convert last_stocked_date to proper format
            last_stocked = pd.to_datetime(row['last_stocked_date']).strftime('%Y-%m-%d')

            cursor.execute(upsert_query, (
                int(row['inventory_id']),
                int(row['product_id']),
                int(row['warehouse_id']),
                int(row['quantity_on_hand']),
                last_stocked
            ))

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM bronze.inventory")
        final_count = cursor.fetchone()[0]

        inserted = final_count - initial_count
        updated = len(df) - inserted

        conn.commit()
        logger.info(f"Successfully processed {len(df):,} inventory records: {inserted:,} inserted, {updated:,} updated")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading inventory data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_retail_stores_to_bronze(df):
    """Load retail stores data to PostgreSQL bronze.retail_stores table."""
    if df.empty:
        logger.warning("No retail stores data to load")
        return False

    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM bronze.retail_stores")
        initial_count = cursor.fetchone()[0]

        # Use UPSERT to prevent duplicates
        upsert_query = """
        INSERT INTO bronze.retail_stores (retail_store_id, store_name, city, region, store_type, store_status)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (retail_store_id) DO UPDATE SET
            store_name = EXCLUDED.store_name,
            city = EXCLUDED.city,
            region = EXCLUDED.region,
            store_type = EXCLUDED.store_type,
            store_status = EXCLUDED.store_status,
            updated_at = CURRENT_TIMESTAMP
        """

        for _, row in df.iterrows():
            cursor.execute(upsert_query, (
                int(row['retail_store_id']),
                row['store_name'],
                row['city'],
                row['region'],
                row['store_type'],
                row.get('store_status', 'active')
            ))

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM bronze.retail_stores")
        final_count = cursor.fetchone()[0]

        inserted = final_count - initial_count
        updated = len(df) - inserted

        conn.commit()
        logger.info(f"Successfully processed {len(df):,} retail stores: {inserted:,} inserted, {updated:,} updated")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading retail stores data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_supply_orders_to_bronze(df):
    """Load supply orders data to PostgreSQL bronze.supply_orders table."""
    if df.empty:
        logger.warning("No supply orders data to load")
        return False

    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM bronze.supply_orders")
        initial_count = cursor.fetchone()[0]

        # Use UPSERT to prevent duplicates
        upsert_query = """
        INSERT INTO bronze.supply_orders (supply_order_id, product_id, warehouse_id, retail_store_id,
                                        quantity, price, total_invoice, order_date, shipped_date,
                                        delivered_date, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (supply_order_id) DO UPDATE SET
            product_id = EXCLUDED.product_id,
            warehouse_id = EXCLUDED.warehouse_id,
            retail_store_id = EXCLUDED.retail_store_id,
            quantity = EXCLUDED.quantity,
            price = EXCLUDED.price,
            total_invoice = EXCLUDED.total_invoice,
            order_date = EXCLUDED.order_date,
            shipped_date = EXCLUDED.shipped_date,
            delivered_date = EXCLUDED.delivered_date,
            status = EXCLUDED.status,
            updated_at = CURRENT_TIMESTAMP
        """

        for _, row in df.iterrows():
            # Convert dates to proper format
            order_date = pd.to_datetime(row['order_date']).strftime('%Y-%m-%d')
            shipped_date = pd.to_datetime(row['shipped_date']).strftime('%Y-%m-%d') if pd.notna(row['shipped_date']) and row['shipped_date'] else None
            delivered_date = pd.to_datetime(row['delivered_date']).strftime('%Y-%m-%d') if pd.notna(row['delivered_date']) and row['delivered_date'] else None

            cursor.execute(upsert_query, (
                int(row['supply_order_id']),
                int(row['product_id']),
                int(row['warehouse_id']),
                int(row['retail_store_id']),
                int(row['quantity']),
                float(row['price']),
                float(row['total_invoice']),
                order_date,
                shipped_date,
                delivered_date,
                row.get('status', 'pending')
            ))

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM bronze.supply_orders")
        final_count = cursor.fetchone()[0]

        inserted = final_count - initial_count
        updated = len(df) - inserted

        conn.commit()
        logger.info(f"Successfully processed {len(df):,} supply order records: {inserted:,} inserted, {updated:,} updated")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading supply orders data: {e}")
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
        'retail_stores': load_retail_stores_to_bronze,
        'supply_orders': load_supply_orders_to_bronze
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

    # Define loading order (suppliers and warehouses first, then products, retail_stores, inventory, supply_orders)
    load_order = ['suppliers', 'warehouses', 'products', 'retail_stores', 'inventory', 'supply_orders']

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
        tables = ['suppliers', 'products', 'warehouses', 'inventory', 'retail_stores', 'supply_orders']

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
        logger.info("\n🔍 Sample Data Preview:")
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
