"""
Silver Layer Builder for Medallion Data Pipeline
Optimized to read directly from bronze and clean data without intermediate copies
"""

import logging
import pandas as pd
import psycopg2
from datetime import datetime
import sys
from pathlib import Path

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG

logger = logging.getLogger(__name__)


class SilverBuilder:
    """Handles Silver layer ETL operations."""

    def __init__(self):
        self.run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.tables = ['suppliers', 'products', 'warehouses', 'inventory', 'retail_stores', 'supply_orders']
        self.stats = {}

    def get_connection(self):
        """Get database connection."""
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            return None

    def setup_schemas(self):
        """Create Silver and Audit schemas with tables."""
        logger.info("Setting up Silver and Audit schemas...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # Create schemas
            cursor.execute("CREATE SCHEMA IF NOT EXISTS silver")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS audit")

            # Create audit.rejected_rows table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit.rejected_rows (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(100) NOT NULL,
                    record JSONB NOT NULL,
                    reason TEXT NOT NULL,
                    run_id VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create audit.dq_results table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit.dq_results (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(100) NOT NULL,
                    check_name VARCHAR(200) NOT NULL,
                    pass_fail BOOLEAN NOT NULL,
                    bad_row_count INTEGER DEFAULT 0,
                    run_id VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create audit.etl_log table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit.etl_log (
                    id SERIAL PRIMARY KEY,
                    run_id VARCHAR(50) NOT NULL,
                    run_timestamp TIMESTAMP NOT NULL,
                    step_executed VARCHAR(100) NOT NULL,
                    table_name VARCHAR(100),
                    input_row_count INTEGER,
                    output_row_count INTEGER,
                    rejected_row_count INTEGER,
                    data_checksum VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
            logger.info("✅ Schemas and audit tables created successfully")
            return True

        except psycopg2.Error as e:
            logger.error(f"Error setting up schemas: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def process_table(self, table_name):
        """Process individual table: read from bronze, clean, validate, and save to silver."""
        logger.info(f"Processing {table_name}...")

        try:
            # Step 1: Read data from bronze with basic SQL cleaning
            df = self._read_and_clean_from_bronze(table_name)
            if df is None or df.empty:
                logger.warning(f"No data found for {table_name}")
                return False

            input_count = len(df)
            logger.info(f"Loaded {input_count:,} rows from bronze.{table_name}")

            # Step 2: Apply Python validations
            valid_df, invalid_df, rejection_reasons = self._apply_validations(table_name, df)

            # Step 3: Save valid rows to silver
            if not valid_df.empty:
                self._save_to_silver(table_name, valid_df)
                logger.info(f"✅ {len(valid_df):,} valid rows saved to silver.{table_name}")

            # Step 4: Save invalid rows to audit
            if not invalid_df.empty:
                self._save_rejected_rows(table_name, invalid_df, rejection_reasons)
                logger.warning(f"⚠️  {len(invalid_df):,} invalid rows saved to audit.rejected_rows")

            # Step 5: Log statistics
            self.stats[table_name] = {
                'input_rows': input_count,
                'valid_rows': len(valid_df),
                'invalid_rows': len(invalid_df)
            }

            # Step 6: Log to audit
            self.log_etl_step(f"process_{table_name}", table_name, input_count, len(valid_df), len(invalid_df))

            return True

        except Exception as e:
            logger.error(f"Error processing {table_name}: {e}")
            return False

    def _read_and_clean_from_bronze(self, table_name):
        """Read data from bronze table with SQL-based cleaning."""
        cleaning_queries = {
            'suppliers': """
                SELECT DISTINCT
                    supplier_id,
                    TRIM(INITCAP(supplier_name)) as supplier_name,
                    LOWER(TRIM(contact_email)) as contact_email,
                    REGEXP_REPLACE(TRIM(phone_number), '[^0-9+()-]', '', 'g') as phone_number,
                    created_at,
                    updated_at
                FROM bronze.suppliers
                WHERE supplier_name IS NOT NULL
                  AND contact_email IS NOT NULL
                  AND phone_number IS NOT NULL
                  AND supplier_id IS NOT NULL
            """,

            'products': """
                SELECT DISTINCT
                    product_id,
                    TRIM(INITCAP(product_name)) as product_name,
                    CAST(unit_cost AS NUMERIC(10,2)) as unit_cost,
                    CAST(selling_price AS NUMERIC(10,2)) as selling_price,
                    supplier_id,
                    TRIM(product_category) as product_category,
                    COALESCE(status, 'active') as status,
                    created_at,
                    updated_at
                FROM bronze.products
                WHERE product_name IS NOT NULL
                  AND unit_cost IS NOT NULL
                  AND selling_price IS NOT NULL
                  AND unit_cost::NUMERIC > 0
                  AND selling_price::NUMERIC > 0
                  AND product_id IS NOT NULL
                  AND supplier_id IS NOT NULL
                  AND product_category IS NOT NULL
            """,

            'warehouses': """
                SELECT DISTINCT
                    warehouse_id,
                    TRIM(INITCAP(warehouse_name)) as warehouse_name,
                    TRIM(INITCAP(city)) as city,
                    TRIM(INITCAP(region)) as region,
                    CAST(storage_capacity AS INTEGER) as storage_capacity,
                    created_at,
                    updated_at
                FROM bronze.warehouses
                WHERE warehouse_name IS NOT NULL
                  AND city IS NOT NULL
                  AND region IS NOT NULL
                  AND storage_capacity IS NOT NULL
                  AND storage_capacity::INTEGER > 0
                  AND warehouse_id IS NOT NULL
            """,

            'inventory': """
                SELECT DISTINCT
                    inventory_id,
                    product_id,
                    warehouse_id,
                    CAST(quantity_on_hand AS INTEGER) as quantity_on_hand,
                    last_stocked_date,
                    created_at,
                    updated_at
                FROM bronze.inventory
                WHERE product_id IS NOT NULL
                  AND warehouse_id IS NOT NULL
                  AND quantity_on_hand IS NOT NULL
                  AND quantity_on_hand::INTEGER >= 0
                  AND inventory_id IS NOT NULL
            """,

            'retail_stores': """
                SELECT DISTINCT
                    retail_store_id,
                    TRIM(INITCAP(store_name)) as store_name,
                    TRIM(INITCAP(city)) as city,
                    TRIM(INITCAP(region)) as region,
                    TRIM(INITCAP(store_type)) as store_type,
                    CASE
                        WHEN UPPER(TRIM(store_status)) = 'ACTIVE' THEN 'active'
                        WHEN UPPER(TRIM(store_status)) = 'CLOSED' THEN 'closed'
                        WHEN UPPER(TRIM(store_status)) IN ('UNDER RENOVATION', 'RENOVATION') THEN 'under renovation'
                        ELSE COALESCE(LOWER(TRIM(store_status)), 'active')
                    END as store_status,
                    created_at,
                    updated_at
                FROM bronze.retail_stores
                WHERE store_name IS NOT NULL
                  AND city IS NOT NULL
                  AND region IS NOT NULL
                  AND store_type IS NOT NULL
                  AND retail_store_id IS NOT NULL
            """,

            'supply_orders': """
                SELECT DISTINCT
                    supply_order_id,
                    product_id,
                    warehouse_id,
                    retail_store_id,
                    CAST(quantity AS INTEGER) as quantity,
                    CAST(price AS NUMERIC(10,2)) as price,
                    CAST(total_invoice AS NUMERIC(12,2)) as total_invoice,
                    order_date,
                    shipped_date,
                    delivered_date,
                    CASE
                        WHEN UPPER(TRIM(status)) = 'PENDING' THEN 'pending'
                        WHEN UPPER(TRIM(status)) = 'SHIPPED' THEN 'shipped'
                        WHEN UPPER(TRIM(status)) = 'DELIVERED' THEN 'delivered'
                        WHEN UPPER(TRIM(status)) IN ('CANCELLED', 'CANCELED') THEN 'canceled'
                        ELSE COALESCE(LOWER(TRIM(status)), 'pending')
                    END as status,
                    created_at,
                    updated_at
                FROM bronze.supply_orders
                WHERE product_id IS NOT NULL
                  AND warehouse_id IS NOT NULL
                  AND retail_store_id IS NOT NULL
                  AND quantity IS NOT NULL
                  AND price IS NOT NULL
                  AND total_invoice IS NOT NULL
                  AND order_date IS NOT NULL
                  AND quantity::INTEGER > 0
                  AND price::NUMERIC > 0
                  AND total_invoice::NUMERIC > 0
                  AND supply_order_id IS NOT NULL
            """
        }

        try:
            from sqlalchemy import create_engine
            engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

            query = cleaning_queries.get(table_name)
            if not query:
                logger.error(f"No cleaning query defined for {table_name}")
                return None

            df = pd.read_sql(query, engine)
            engine.dispose()
            return df

        except Exception as e:
            logger.error(f"Error reading from bronze.{table_name}: {e}")
            return None

    def _apply_validations(self, table_name, df):
        """Apply specific validations based on table type."""
        valid_mask = pd.Series([True] * len(df))
        rejection_reasons = [''] * len(df)

        if table_name == 'suppliers':
            # Email validation
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            invalid_email = ~df['contact_email'].str.match(email_pattern)
            valid_mask &= ~invalid_email
            rejection_reasons = [r if not ie else 'Invalid email format'
                               for r, ie in zip(rejection_reasons, invalid_email)]

            # Phone validation
            phone_pattern = r'^[\+]?[1-9][\d]{0,15}$'
            invalid_phone = ~df['phone_number'].str.replace(r'[()\s-]', '', regex=True).str.match(phone_pattern)
            valid_mask &= ~invalid_phone
            rejection_reasons = [r if not ip else 'Invalid phone format'
                               for r, ip in zip(rejection_reasons, invalid_phone)]

        elif table_name == 'products':
            # Unit cost range validation
            invalid_cost = (df['unit_cost'] <= 0) | (df['unit_cost'] > 100000)
            valid_mask &= ~invalid_cost
            rejection_reasons = [r if not ic else 'Unit cost out of range (0-100000)'
                               for r, ic in zip(rejection_reasons, invalid_cost)]

            # Selling price range validation
            invalid_selling_price = (df['selling_price'] <= 0) | (df['selling_price'] > 500000)
            valid_mask &= ~invalid_selling_price
            rejection_reasons = [r if not isp else 'Selling price out of range (0-500000)'
                               for r, isp in zip(rejection_reasons, invalid_selling_price)]

            # Selling price should be greater than unit cost
            invalid_margin = df['selling_price'] <= df['unit_cost']
            valid_mask &= ~invalid_margin
            rejection_reasons = [r if not im else 'Selling price must be greater than unit cost'
                               for r, im in zip(rejection_reasons, invalid_margin)]

            # Product category validation (should not be empty)
            invalid_category = df['product_category'].str.strip().eq('')
            valid_mask &= ~invalid_category
            rejection_reasons = [r if not ic else 'Empty product category'
                               for r, ic in zip(rejection_reasons, invalid_category)]

            # Status validation
            valid_statuses = ['active', 'discontinued']
            invalid_status = ~df['status'].isin(valid_statuses)
            valid_mask &= ~invalid_status
            rejection_reasons = [r if not ist else 'Invalid product status'
                               for r, ist in zip(rejection_reasons, invalid_status)]

        elif table_name == 'warehouses':
            # Storage capacity validation
            invalid_capacity = (df['storage_capacity'] <= 0) | (df['storage_capacity'] > 5000000)
            valid_mask &= ~invalid_capacity
            rejection_reasons = [r if not ic else 'Storage capacity out of range (0-5000000)'
                               for r, ic in zip(rejection_reasons, invalid_capacity)]

            # Region validation (should not be empty)
            invalid_region = df['region'].str.strip().eq('')
            valid_mask &= ~invalid_region
            rejection_reasons = [r if not ir else 'Empty region'
                               for r, ir in zip(rejection_reasons, invalid_region)]

            # City validation (should not be empty)
            invalid_city = df['city'].str.strip().eq('')
            valid_mask &= ~invalid_city
            rejection_reasons = [r if not ic else 'Empty city'
                               for r, ic in zip(rejection_reasons, invalid_city)]

        elif table_name == 'inventory':
            # Quantity validation
            invalid_quantity = df['quantity_on_hand'] < 0
            valid_mask &= ~invalid_quantity
            rejection_reasons = [r if not iq else 'Negative quantity'
                               for r, iq in zip(rejection_reasons, invalid_quantity)]

            # Date validation
            today = pd.Timestamp.now().date()
            future_date = pd.to_datetime(df['last_stocked_date']).dt.date > today
            valid_mask &= ~future_date
            rejection_reasons = [r if not fd else 'Future stocking date'
                               for r, fd in zip(rejection_reasons, future_date)]

        elif table_name == 'retail_stores':
            # Store status validation
            valid_statuses = ['active', 'closed', 'under renovation']
            invalid_status = ~df['store_status'].isin(valid_statuses)
            valid_mask &= ~invalid_status
            rejection_reasons = [r if not ist else 'Invalid store status'
                               for r, ist in zip(rejection_reasons, invalid_status)]

            # Store type validation (should not be empty)
            invalid_type = df['store_type'].str.strip().eq('')
            valid_mask &= ~invalid_type
            rejection_reasons = [r if not it else 'Empty store type'
                               for r, it in zip(rejection_reasons, invalid_type)]

            # Region validation (should not be empty)
            invalid_region = df['region'].str.strip().eq('')
            valid_mask &= ~invalid_region
            rejection_reasons = [r if not ir else 'Empty region'
                               for r, ir in zip(rejection_reasons, invalid_region)]

        elif table_name == 'supply_orders':
            # Status validation
            valid_statuses = ['pending', 'shipped', 'delivered', 'canceled']
            invalid_status = ~df['status'].isin(valid_statuses)
            valid_mask &= ~invalid_status
            rejection_reasons = [r if not ist else 'Invalid order status'
                               for r, ist in zip(rejection_reasons, invalid_status)]

            # Quantity validation
            invalid_qty = df['quantity'] <= 0
            valid_mask &= ~invalid_qty
            rejection_reasons = [r if not iq else 'Invalid quantity (must be positive)'
                               for r, iq in zip(rejection_reasons, invalid_qty)]

            # Price validation
            invalid_price = (df['price'] <= 0) | (df['price'] > 500000)
            valid_mask &= ~invalid_price
            rejection_reasons = [r if not ip else 'Price out of range (0-500000)'
                               for r, ip in zip(rejection_reasons, invalid_price)]

            # Total invoice validation
            invalid_total = df['total_invoice'] <= 0
            valid_mask &= ~invalid_total
            rejection_reasons = [r if not it else 'Invalid total invoice (must be positive)'
                               for r, it in zip(rejection_reasons, invalid_total)]

            # Logical validation: total_invoice should be approximately quantity * price
            calculated_total = df['quantity'] * df['price']
            tolerance = 0.01  # 1% tolerance for rounding
            invalid_calculation = abs(df['total_invoice'] - calculated_total) > (calculated_total * tolerance)
            valid_mask &= ~invalid_calculation
            rejection_reasons = [r if not ic else 'Total invoice does not match quantity × price'
                               for r, ic in zip(rejection_reasons, invalid_calculation)]

            # Date logic validation: shipped_date should be after order_date if present
            shipped_before_order = (df['shipped_date'].notna()) & (df['shipped_date'] < df['order_date'])
            valid_mask &= ~shipped_before_order
            rejection_reasons = [r if not sbo else 'Shipped date before order date'
                               for r, sbo in zip(rejection_reasons, shipped_before_order)]

            # Date logic validation: delivered_date should be after shipped_date if both present
            delivered_before_shipped = (df['delivered_date'].notna()) & (df['shipped_date'].notna()) & (df['delivered_date'] < df['shipped_date'])
            valid_mask &= ~delivered_before_shipped
            rejection_reasons = [r if not dbs else 'Delivered date before shipped date'
                               for r, dbs in zip(rejection_reasons, delivered_before_shipped)]

        valid_df = df[valid_mask].copy()
        invalid_df = df[~valid_mask].copy()
        invalid_reasons = [r for r, v in zip(rejection_reasons, ~valid_mask) if v]

        return valid_df, invalid_df, invalid_reasons

    def _save_to_silver(self, table_name, df):
        """Save DataFrame to silver table."""
        try:
            from sqlalchemy import create_engine
            engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
            df.to_sql(table_name, engine, schema='silver', if_exists='replace', index=False)
            engine.dispose()
        except Exception as e:
            logger.error(f"Error saving to silver.{table_name}: {e}")
            raise

    def _save_rejected_rows(self, table_name, invalid_df, reasons):
        """Save rejected rows to audit.rejected_rows."""
        conn = self.get_connection()
        if not conn:
            return

        try:
            cursor = conn.cursor()

            for idx, (_, row) in enumerate(invalid_df.iterrows()):
                record_json = row.to_json()
                reason = reasons[idx] if idx < len(reasons) else 'Validation failed'

                cursor.execute("""
                    INSERT INTO audit.rejected_rows (table_name, record, reason, run_id)
                    VALUES (%s, %s, %s, %s)
                """, (table_name, record_json, reason, self.run_id))

            conn.commit()

        except psycopg2.Error as e:
            logger.error(f"Error saving rejected rows: {e}")
        finally:
            cursor.close()
            conn.close()

    def process_all_tables(self):
        """Process all tables in the correct order."""
        logger.info("🚀 Starting Silver layer processing...")

        results = {}
        for table_name in self.tables:
            results[table_name] = self.process_table(table_name)

        all_success = all(results.values())
        if all_success:
            logger.info("✅ All tables processed successfully")
        else:
            logger.warning("⚠️  Some tables had processing issues")

        return all_success

    def run_data_quality_checks(self):
        """Run data quality checks on silver tables."""
        logger.info("Running Data Quality checks...")

        checks = {
            'suppliers': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT supplier_id) FROM silver.suppliers"),
                ('email_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT contact_email) FROM silver.suppliers"),
                ('null_check', "SELECT COUNT(*) FROM silver.suppliers WHERE supplier_name IS NULL OR contact_email IS NULL")
            ],
            'products': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM silver.products"),
                ('positive_cost', "SELECT COUNT(*) FROM silver.products WHERE unit_cost <= 0"),
                ('positive_selling_price', "SELECT COUNT(*) FROM silver.products WHERE selling_price <= 0"),
                ('selling_price_gt_cost', "SELECT COUNT(*) FROM silver.products WHERE selling_price <= unit_cost")
            ],
            'warehouses': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT warehouse_id) FROM silver.warehouses"),
                ('positive_capacity', "SELECT COUNT(*) FROM silver.warehouses WHERE storage_capacity <= 0"),
                ('non_empty_region', "SELECT COUNT(*) FROM silver.warehouses WHERE region IS NULL OR TRIM(region) = ''"),
                ('non_empty_city', "SELECT COUNT(*) FROM silver.warehouses WHERE city IS NULL OR TRIM(city) = ''")
            ],
            'inventory': [
                ('fk_product_valid', """SELECT COUNT(*) FROM silver.inventory i
                                       LEFT JOIN silver.products p ON i.product_id = p.product_id
                                       WHERE p.product_id IS NULL"""),
                ('fk_warehouse_valid', """SELECT COUNT(*) FROM silver.inventory i
                                         LEFT JOIN silver.warehouses w ON i.warehouse_id = w.warehouse_id
                                         WHERE w.warehouse_id IS NULL"""),
                ('non_negative_qty', "SELECT COUNT(*) FROM silver.inventory WHERE quantity_on_hand < 0")
            ],
            'retail_stores': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT retail_store_id) FROM silver.retail_stores"),
                ('valid_status', """SELECT COUNT(*) FROM silver.retail_stores
                                   WHERE store_status NOT IN ('active','closed','under renovation')"""),
                ('non_empty_type', "SELECT COUNT(*) FROM silver.retail_stores WHERE store_type IS NULL OR TRIM(store_type) = ''"),
                ('non_empty_region', "SELECT COUNT(*) FROM silver.retail_stores WHERE region IS NULL OR TRIM(region) = ''")
            ],
            'supply_orders': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT supply_order_id) FROM silver.supply_orders"),
                ('fk_product_valid', """SELECT COUNT(*) FROM silver.supply_orders so
                                       LEFT JOIN silver.products p ON so.product_id = p.product_id
                                       WHERE p.product_id IS NULL"""),
                ('fk_warehouse_valid', """SELECT COUNT(*) FROM silver.supply_orders so
                                         LEFT JOIN silver.warehouses w ON so.warehouse_id = w.warehouse_id
                                         WHERE w.warehouse_id IS NULL"""),
                ('fk_retail_store_valid', """SELECT COUNT(*) FROM silver.supply_orders so
                                            LEFT JOIN silver.retail_stores rs ON so.retail_store_id = rs.retail_store_id
                                            WHERE rs.retail_store_id IS NULL"""),
                ('valid_status', """SELECT COUNT(*) FROM silver.supply_orders
                                   WHERE status NOT IN ('pending','shipped','delivered','canceled')"""),
                ('positive_quantity', "SELECT COUNT(*) FROM silver.supply_orders WHERE quantity <= 0"),
                ('positive_price', "SELECT COUNT(*) FROM silver.supply_orders WHERE price <= 0"),
                ('positive_total', "SELECT COUNT(*) FROM silver.supply_orders WHERE total_invoice <= 0")
            ]
        }

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()
            all_passed = True

            for table_name, table_checks in checks.items():
                logger.info(f"Running DQ checks for {table_name}...")

                for check_name, sql_query in table_checks:
                    cursor.execute(sql_query)
                    bad_count = cursor.fetchone()[0]
                    passed = bad_count == 0

                    if not passed:
                        all_passed = False
                        logger.warning(f"❌ {table_name}.{check_name}: {bad_count} bad rows")
                    else:
                        logger.info(f"✅ {table_name}.{check_name}: PASSED")

                    # Save result to audit.dq_results
                    cursor.execute("""
                        INSERT INTO audit.dq_results (table_name, check_name, pass_fail, bad_row_count, run_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (table_name, check_name, passed, bad_count, self.run_id))

            conn.commit()

            if all_passed:
                logger.info("✅ All Data Quality checks passed")
            else:
                logger.warning("⚠️  Some Data Quality checks failed")

            return True

        except psycopg2.Error as e:
            logger.error(f"Error running DQ checks: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def log_etl_step(self, step_name, table_name, input_count, output_count, rejected_count):
        """Log ETL step to audit.etl_log."""
        conn = self.get_connection()
        if not conn:
            return

        try:
            cursor = conn.cursor()

            # Simple checksum based on row count
            checksum = f"{table_name}:{output_count}"
            import hashlib
            checksum = hashlib.md5(checksum.encode()).hexdigest()

            cursor.execute("""
                INSERT INTO audit.etl_log
                (run_id, run_timestamp, step_executed, table_name, input_row_count,
                 output_row_count, rejected_row_count, data_checksum)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (self.run_id, datetime.now(), step_name, table_name,
                  input_count, output_count, rejected_count, checksum))

            conn.commit()

        except psycopg2.Error as e:
            logger.error(f"Error logging ETL step: {e}")
        finally:
            cursor.close()
            conn.close()

    def log_summary(self):
        """Log final summary of Silver layer processing."""
        logger.info("=" * 60)
        logger.info("📊 SILVER LAYER PROCESSING SUMMARY")
        logger.info("=" * 60)

        total_input = sum(stats.get('input_rows', 0) for stats in self.stats.values())
        total_valid = sum(stats.get('valid_rows', 0) for stats in self.stats.values())
        total_invalid = sum(stats.get('invalid_rows', 0) for stats in self.stats.values())

        for table_name in self.tables:
            stats = self.stats.get(table_name, {})
            input_rows = stats.get('input_rows', 0)
            valid_rows = stats.get('valid_rows', 0)
            invalid_rows = stats.get('invalid_rows', 0)

            logger.info(f"  {table_name:<12}: {input_rows:>8,} → {valid_rows:>8,} valid, {invalid_rows:>6,} rejected")

        logger.info("-" * 60)
        logger.info(f"  {'TOTAL':<12}: {total_input:>8,} → {total_valid:>8,} valid, {total_invalid:>6,} rejected")
        logger.info(f"  Run ID: {self.run_id}")
        logger.info("=" * 60)

    def run_full_silver_pipeline(self):
        """Run complete silver layer pipeline."""
        logger.info("🥈 SILVER LAYER BUILDER - FULL PIPELINE")
        logger.info("=" * 60)

        # Step 1: Setup schemas
        if not self.setup_schemas():
            logger.error("❌ Schema setup failed")
            return False

        # Step 2: Process all tables
        if not self.process_all_tables():
            logger.error("❌ Table processing failed")
            return False

        # Step 3: Run DQ checks
        if not self.run_data_quality_checks():
            logger.warning("⚠️  Some DQ checks failed, but continuing...")

        # Step 4: Log summary
        self.log_summary()

        logger.info("✅ Silver layer pipeline completed successfully!")
        return True


def main():
    """Main function for standalone execution."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    sb = SilverBuilder()
    success = sb.run_full_silver_pipeline()

    if success:
        print("✅ Silver Builder completed successfully!")
    else:
        print("❌ Silver Builder failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
