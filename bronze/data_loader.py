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
    """Create database connection."""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        return None


def get_sheets_service():
    """Create Google Sheets service."""
    try:
        credentials = Credentials.from_service_account_file(
            GOOGLE_SHEETS_CONFIG['credentials_path'],
            scopes=GOOGLE_SHEETS_CONFIG['scopes']
        )
        # Create HTTP client with SSL verification disabled for corporate environments
        unverified_http = httplib2.Http(disable_ssl_certificate_validation=True)
        authed_http = AuthorizedHttp(credentials, http=unverified_http)
        service = build('sheets', 'v4', http=authed_http)
        logger.info("Google Sheets service created successfully (SSL verification disabled)")
        return service
    except Exception as e:
        logger.error(f"Error creating Google Sheets service: {e}")
        return None


def fetch_sheet_data(service, range_name):
    """Fetch data from Google Sheets and return as DataFrame."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEETS_CONFIG['spreadsheet_id'],
            range=range_name
        ).execute()
        values = result.get('values', [])

        if not values:
            return pd.DataFrame()

        # Create DataFrame with headers from first row
        headers = values[0]
        data = values[1:]

        # Ensure all rows have the same number of columns as headers
        for i, row in enumerate(data):
            if len(row) < len(headers):
                data[i] = row + [''] * (len(headers) - len(row))

        df = pd.DataFrame(data, columns=headers)
        logger.info(f"Fetched {len(df)} rows from {range_name}")
        return df

    except Exception as e:
        logger.error(f"Error fetching data from {range_name}: {e}")
        return pd.DataFrame()


def safe_str_conversion(value):
    """Safely convert any value to string, handling None, NaN, etc."""
    if pd.isna(value) or value is None:
        return None
    return str(value).strip() if str(value).strip() else None


def load_suppliers_to_bronze    (df):
    """Load suppliers data to PostgreSQL bronze.suppliers table - RAW DATA."""
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

        upsert_query = """
        INSERT INTO bronze.suppliers (supplier_id, supplier_name, contact_email, phone_number)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (supplier_id) DO UPDATE SET
            supplier_name = EXCLUDED.supplier_name,
            contact_email = EXCLUDED.contact_email,
            phone_number = EXCLUDED.phone_number
        """

        success_count = 0
        error_count = 0

        for _, row in df.iterrows():
            try:
                # Convert all values to string, preserve exactly as received
                supplier_id_raw = safe_str_conversion(row.get('supplier_id', ''))
                supplier_name_raw = safe_str_conversion(row.get('supplier_name', ''))
                contact_email_raw = safe_str_conversion(row.get('contact_email', ''))
                phone_number_raw = safe_str_conversion(row.get('phone_number', ''))

                # Try to extract supplier_id for primary key, but be lenient
                supplier_id = None
                if supplier_id_raw:
                    try:
                        # Try to extract numeric part if possible
                        import re
                        numeric_match = re.search(r'\d+', supplier_id_raw)
                        if numeric_match:
                            supplier_id = int(numeric_match.group())
                        else:
                            # If no numeric part, skip this row
                            logger.warning(f"Skipping row with invalid supplier_id: {supplier_id_raw}")
                            error_count += 1
                            continue
                    except:
                        logger.warning(f"Skipping row with unparseable supplier_id: {supplier_id_raw}")
                        error_count += 1
                        continue

                cursor.execute(upsert_query, (
                    supplier_id,
                    supplier_name_raw,
                    contact_email_raw,
                    phone_number_raw
                ))
                success_count += 1

            except Exception as e:
                logger.warning(f"Error processing supplier row: {e}")
                error_count += 1
                continue

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM bronze.suppliers")
        final_count = cursor.fetchone()[0]

        inserted = final_count - initial_count
        updated = success_count - inserted

        conn.commit()
        logger.info(f"Suppliers processed: {success_count:,} successful, {error_count:,} errors, {inserted:,} inserted, {updated:,} updated")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading suppliers data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_warehouses_to_bronze(df):
    """Load warehouses data to PostgreSQL bronze.warehouses table - RAW DATA."""
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

        upsert_query = """
        INSERT INTO bronze.warehouses (warehouse_id, warehouse_name, city, region, storage_capacity)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (warehouse_id) DO UPDATE SET
            warehouse_name = EXCLUDED.warehouse_name,
            city = EXCLUDED.city,
            region = EXCLUDED.region,
            storage_capacity = EXCLUDED.storage_capacity
        """

        success_count = 0
        error_count = 0

        for _, row in df.iterrows():
            try:
                # Extract warehouse_id for primary key
                warehouse_id_raw = safe_str_conversion(row.get('warehouse_id', ''))
                warehouse_id = None
                if warehouse_id_raw:
                    try:
                        import re
                        numeric_match = re.search(r'\d+', warehouse_id_raw)
                        if numeric_match:
                            warehouse_id = int(numeric_match.group())
                        else:
                            logger.warning(f"Skipping row with invalid warehouse_id: {warehouse_id_raw}")
                            error_count += 1
                            continue
                    except:
                        logger.warning(f"Skipping row with unparseable warehouse_id: {warehouse_id_raw}")
                        error_count += 1
                        continue

                # Extract storage_capacity for integer field, but be lenient
                storage_capacity_raw = safe_str_conversion(row.get('storage_capacity', ''))
                storage_capacity = None
                if storage_capacity_raw:
                    try:
                        import re
                        numeric_match = re.search(r'-?\d+', storage_capacity_raw)
                        if numeric_match:
                            storage_capacity = int(numeric_match.group())
                        else:
                            storage_capacity = 0  # Default for invalid data
                    except:
                        storage_capacity = 0

                cursor.execute(upsert_query, (
                    warehouse_id,
                    safe_str_conversion(row.get('warehouse_name', '')),
                    safe_str_conversion(row.get('city', '')),
                    safe_str_conversion(row.get('region', '')),
                    storage_capacity
                ))
                success_count += 1

            except Exception as e:
                logger.warning(f"Error processing warehouse row: {e}")
                error_count += 1
                continue

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM bronze.warehouses")
        final_count = cursor.fetchone()[0]

        inserted = final_count - initial_count
        updated = success_count - inserted

        conn.commit()
        logger.info(f"Warehouses processed: {success_count:,} successful, {error_count:,} errors, {inserted:,} inserted, {updated:,} updated")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading warehouses data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_products_to_bronze(df):
    """Load products data to PostgreSQL bronze.products table - RAW DATA."""
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

        upsert_query = """
        INSERT INTO bronze.products (product_id, product_name, unit_cost, selling_price, supplier_id, product_category, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            unit_cost = EXCLUDED.unit_cost,
            selling_price = EXCLUDED.selling_price,
            supplier_id = EXCLUDED.supplier_id,
            product_category = EXCLUDED.product_category,
            status = EXCLUDED.status
        """

        success_count = 0
        error_count = 0

        for _, row in df.iterrows():
            try:
                # Extract product_id for primary key
                product_id_raw = safe_str_conversion(row.get('product_id', ''))
                product_id = None
                if product_id_raw:
                    try:
                        import re
                        numeric_match = re.search(r'\d+', product_id_raw)
                        if numeric_match:
                            product_id = int(numeric_match.group())
                        else:
                            logger.warning(f"Skipping row with invalid product_id: {product_id_raw}")
                            error_count += 1
                            continue
                    except:
                        logger.warning(f"Skipping row with unparseable product_id: {product_id_raw}")
                        error_count += 1
                        continue

                # Extract numeric fields but be lenient with dirty data
                def extract_decimal(raw_value):
                    if not raw_value:
                        return 0.0
                    try:
                        import re
                        # Remove currency symbols and extract numeric part
                        numeric_match = re.search(r'-?\d+\.?\d*', str(raw_value))
                        if numeric_match:
                            return float(numeric_match.group())
                        return 0.0
                    except:
                        return 0.0

                def extract_int(raw_value):
                    if not raw_value:
                        return None
                    try:
                        import re
                        numeric_match = re.search(r'\d+', str(raw_value))
                        if numeric_match:
                            return int(numeric_match.group())
                        return None
                    except:
                        return None

                unit_cost = extract_decimal(safe_str_conversion(row.get('unit_cost', '')))
                selling_price = extract_decimal(safe_str_conversion(row.get('selling_price', '')))
                supplier_id = extract_int(safe_str_conversion(row.get('supplier_id', '')))

                cursor.execute(upsert_query, (
                    product_id,
                    safe_str_conversion(row.get('product_name', '')),
                    unit_cost,
                    selling_price,
                    supplier_id,
                    safe_str_conversion(row.get('product_category', '')),
                    safe_str_conversion(row.get('status', 'active'))
                ))
                success_count += 1

            except Exception as e:
                logger.warning(f"Error processing product row: {e}")
                error_count += 1
                continue

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM bronze.products")
        final_count = cursor.fetchone()[0]

        inserted = final_count - initial_count
        updated = success_count - inserted

        conn.commit()
        logger.info(f"Products processed: {success_count:,} successful, {error_count:,} errors, {inserted:,} inserted, {updated:,} updated")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading products data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_inventory_to_bronze(df):
    """Load inventory data to PostgreSQL bronze.inventory table - RAW DATA."""
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

        upsert_query = """
        INSERT INTO bronze.inventory (inventory_id, product_id, warehouse_id, quantity_on_hand, last_stocked_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (inventory_id) DO UPDATE SET
            product_id = EXCLUDED.product_id,
            warehouse_id = EXCLUDED.warehouse_id,
            quantity_on_hand = EXCLUDED.quantity_on_hand,
            last_stocked_date = EXCLUDED.last_stocked_date
        """

        success_count = 0
        error_count = 0

        for _, row in df.iterrows():
            try:
                def extract_int(raw_value):
                    if not raw_value:
                        return None
                    try:
                        import re
                        numeric_match = re.search(r'-?\d+', str(raw_value))
                        if numeric_match:
                            return int(numeric_match.group())
                        return None
                    except:
                        return None

                def extract_date(raw_value):
                    if not raw_value or str(raw_value).strip() in ['', 'N/A', 'NULL', 'TBD']:
                        return None
                    try:
                        # Try to parse various date formats, but don't fail if we can't
                        import dateutil.parser
                        return dateutil.parser.parse(str(raw_value).strip()).date()
                    except:
                        return None

                inventory_id = extract_int(safe_str_conversion(row.get('inventory_id', '')))
                if not inventory_id:
                    logger.warning(f"Skipping row with invalid inventory_id")
                    error_count += 1
                    continue

                product_id = extract_int(safe_str_conversion(row.get('product_id', '')))
                warehouse_id = extract_int(safe_str_conversion(row.get('warehouse_id', '')))
                quantity = extract_int(safe_str_conversion(row.get('quantity_on_hand', '')))
                last_stocked = extract_date(safe_str_conversion(row.get('last_stocked_date', '')))

                cursor.execute(upsert_query, (
                    inventory_id,
                    product_id,
                    warehouse_id,
                    quantity if quantity is not None else 0,
                    last_stocked
                ))
                success_count += 1

            except Exception as e:
                logger.warning(f"Error processing inventory row: {e}")
                error_count += 1
                continue

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM bronze.inventory")
        final_count = cursor.fetchone()[0]

        inserted = final_count - initial_count
        updated = success_count - inserted

        conn.commit()
        logger.info(f"Inventory processed: {success_count:,} successful, {error_count:,} errors, {inserted:,} inserted, {updated:,} updated")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading inventory data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_retail_stores_to_bronze(df):
    """Load retail stores data to PostgreSQL bronze.retail_stores table - RAW DATA."""
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

        upsert_query = """
        INSERT INTO bronze.retail_stores (retail_store_id, store_name, city, region, store_type, store_status)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (retail_store_id) DO UPDATE SET
            store_name = EXCLUDED.store_name,
            city = EXCLUDED.city,
            region = EXCLUDED.region,
            store_type = EXCLUDED.store_type,
            store_status = EXCLUDED.store_status
        """

        success_count = 0
        error_count = 0

        for _, row in df.iterrows():
            try:
                def extract_int(raw_value):
                    if not raw_value:
                        return None
                    try:
                        import re
                        numeric_match = re.search(r'\d+', str(raw_value))
                        if numeric_match:
                            return int(numeric_match.group())
                        return None
                    except:
                        return None

                retail_store_id = extract_int(safe_str_conversion(row.get('retail_store_id', '')))
                if not retail_store_id:
                    logger.warning(f"Skipping row with invalid retail_store_id")
                    error_count += 1
                    continue

                cursor.execute(upsert_query, (
                    retail_store_id,
                    safe_str_conversion(row.get('store_name', '')),
                    safe_str_conversion(row.get('city', '')),
                    safe_str_conversion(row.get('region', '')),
                    safe_str_conversion(row.get('store_type', '')),
                    safe_str_conversion(row.get('store_status', 'active'))
                ))
                success_count += 1

            except Exception as e:
                logger.warning(f"Error processing retail store row: {e}")
                error_count += 1
                continue

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM bronze.retail_stores")
        final_count = cursor.fetchone()[0]

        inserted = final_count - initial_count
        updated = success_count - inserted

        conn.commit()
        logger.info(f"Retail stores processed: {success_count:,} successful, {error_count:,} errors, {inserted:,} inserted, {updated:,} updated")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading retail stores data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_supply_orders_to_bronze(df):
    """Load supply orders data to PostgreSQL bronze.supply_orders table - RAW DATA."""
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
            status = EXCLUDED.status
        """

        success_count = 0
        error_count = 0

        for _, row in df.iterrows():
            try:
                supply_order_id_raw = safe_str_conversion(row.get('supply_order_id', ''))
                supply_order_id = None
                if supply_order_id_raw:
                    try:
                        import re
                        numeric_match = re.search(r'\d+', supply_order_id_raw)
                        if numeric_match:
                            supply_order_id = int(numeric_match.group())
                        else:
                            logger.warning(f"Skipping row with invalid supply_order_id: {supply_order_id_raw}")
                            error_count += 1
                            continue
                    except:
                        logger.warning(f"Skipping row with unparseable supply_order_id: {supply_order_id_raw}")
                        error_count += 1
                        continue

                cursor.execute(upsert_query, (
                    supply_order_id,
                    safe_str_conversion(row.get('product_id', '')),
                    safe_str_conversion(row.get('warehouse_id', '')),
                    safe_str_conversion(row.get('retail_store_id', '')),
                    safe_str_conversion(row.get('quantity', '')),
                    safe_str_conversion(row.get('price', '')),
                    safe_str_conversion(row.get('total_invoice', '')),
                    safe_str_conversion(row.get('order_date', '')),
                    safe_str_conversion(row.get('shipped_date', '')),
                    safe_str_conversion(row.get('delivered_date', '')),
                    safe_str_conversion(row.get('status', ''))
                ))
                success_count += 1

            except Exception as e:
                logger.warning(f"Error processing supply order row: {e}")
                error_count += 1
                continue

        # Get final count
        cursor.execute("SELECT COUNT(*) FROM bronze.supply_orders")
        final_count = cursor.fetchone()[0]

        inserted = final_count - initial_count
        updated = success_count - inserted

        conn.commit()
        logger.info(f"Supply orders processed: {success_count:,} successful, {error_count:,} errors, {inserted:,} inserted, {updated:,} updated")
        return True

    except (psycopg2.Error, ValueError) as e:
        logger.error(f"Error loading supply orders data: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_sheet_to_bronze(sheet_name):
    """Load data from a single Google Sheet to bronze layer - ACCEPTS DIRTY DATA."""
    logger.info(f"📥 Loading {sheet_name} data to bronze layer (raw/unclean)...")

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

    logger.info(f"📊 Raw data loaded: {len(df)} rows with columns: {list(df.columns)}")

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
        logger.info(f"✅ Successfully loaded {sheet_name} raw data to bronze layer")
    else:
        logger.error(f"❌ Failed to load {sheet_name} data to bronze layer")

    return success


def load_all_data_to_bronze():
    """Load all data from Google Sheets to bronze layer - ACCEPTS DIRTY DATA."""
    logger.info("🚀 Starting bulk load of all raw data to bronze layer")
    logger.info("=" * 60)

    # Define load order (no dependencies since we're accepting dirty data)
    sheets_to_load = [
        'suppliers',
        'warehouses',
        'products',
        'retail_stores',
        'inventory',
        'supply_orders'
    ]

    successful_loads = 0
    failed_loads = 0

    for sheet in sheets_to_load:
        logger.info(f"\n📋 Processing {sheet}...")
        success = load_sheet_to_bronze(sheet)
        if success:
            successful_loads += 1
            logger.info(f"✅ {sheet} loaded successfully")
        else:
            failed_loads += 1
            logger.error(f"❌ {sheet} failed to load")

    logger.info(f"\n📊 Load Summary:")
    logger.info(f"  ✅ Successful: {successful_loads}")
    logger.info(f"  ❌ Failed: {failed_loads}")
    logger.info(f"  📈 Success Rate: {(successful_loads/(successful_loads+failed_loads)*100):.1f}%")

    return failed_loads == 0


def verify_bronze_data():
    """Verify bronze layer data - shows raw/dirty data as-is."""
    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        logger.info("🔍 Bronze Layer Data Verification (Raw/Unclean)")
        logger.info("=" * 60)

        tables = ['suppliers', 'products', 'warehouses', 'inventory', 'retail_stores', 'supply_orders']

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM bronze.{table}")
            count = cursor.fetchone()[0]
            logger.info(f"📊 bronze.{table:<15}: {count:>8,} records")

        # Show sample of dirty data
        logger.info(f"\n🔍 Sample Raw Data (showing data quality issues):")
        logger.info("-" * 60)

        # Show sample suppliers with potential issues
        cursor.execute("SELECT supplier_name, contact_email, phone_number FROM bronze.suppliers LIMIT 5")
        suppliers = cursor.fetchall()
        logger.info("Suppliers (raw):")
        for supplier in suppliers:
            logger.info(f"  - {supplier[0]} | {supplier[1]} | {supplier[2]}")

        # Show sample products with potential price issues
        cursor.execute("SELECT product_name, unit_cost, selling_price, product_category FROM bronze.products LIMIT 5")
        products = cursor.fetchall()
        logger.info("\nProducts (raw):")
        for product in products:
            logger.info(f"  - {product[0]} | Cost: {product[1]} | Price: {product[2]} | Cat: {product[3]}")

        cursor.close()
        conn.close()

        logger.info(f"\n✅ Bronze layer verification completed")
        logger.info("Note: Data shown above is RAW and may contain quality issues")
        logger.info("Silver layer will handle data cleaning and validation")
        return True

    except psycopg2.Error as e:
        logger.error(f"Error during verification: {e}")
        return False


def main():
    """Main function to load all data to bronze layer - HANDLES DIRTY DATA."""
    logger.info("🏗️  Medallion Architecture - Bronze Layer Data Loader")
    logger.info("🔥 LOADING RAW/UNCLEAN DATA - NO VALIDATION/CLEANING")
    logger.info("=" * 60)

    try:
        # Load all data to bronze layer
        success = load_all_data_to_bronze()

        # Verify what was loaded
        logger.info("\n🔍 Verifying loaded data...")
        verify_bronze_data()

        # Final summary
        logger.info("=" * 60)
        if success:
            logger.info("🎉 Bronze layer loading completed successfully!")
            logger.info("📋 Next steps:")
            logger.info("   1. Run silver_builder.py to clean and validate data")
            logger.info("   2. Bronze layer contains RAW data with quality issues")
            logger.info("   3. Silver layer will handle all data cleaning")
        else:
            logger.error("❌ Bronze layer loading completed with some errors")
            logger.info("   Check logs above for details")

        logger.info("=" * 60)
        return success

    except KeyboardInterrupt:
        logger.info("\n❌ Process interrupted by user")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error during bronze loading: {e}")
        return False


if __name__ == "__main__":
    main()
