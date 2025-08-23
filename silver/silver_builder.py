"""
Silver Layer Builder for Medallion Data Pipeline
Handles data cleaning, validation, and quality checks
"""

import logging
import pandas as pd
import psycopg2
import hashlib
import json
import re
from datetime import datetime
from psycopg2.extras import RealDictCursor
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
        self.tables = ['suppliers', 'products', 'warehouses', 'inventory', 'shipments']
        self.stats = {}

    def get_connection(self):
        """Get database connection."""
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            return None

    def setup_schemas(self):
        """Step 1: Create Silver and Audit schemas with tables."""
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

    def create_silver_base_tables(self):
        """Step 2: Create Silver base tables with light cleaning using SQL."""
        logger.info("Creating Silver base tables with light cleaning...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # SQL scripts for each table
            sql_scripts = {
                'suppliers': """
                    CREATE TABLE IF NOT EXISTS silver.suppliers_base AS
                    WITH cleaned_data AS (
                        SELECT
                            supplier_id,
                            TRIM(INITCAP(supplier_name)) as supplier_name,
                            LOWER(TRIM(contact_email)) as contact_email,
                            REGEXP_REPLACE(TRIM(phone_number), '[^0-9+()-]', '', 'g') as phone_number,
                            created_at,
                            updated_at,
                            ROW_NUMBER() OVER (PARTITION BY contact_email ORDER BY updated_at DESC) as rn
                        FROM bronze.suppliers
                        WHERE supplier_name IS NOT NULL
                        AND contact_email IS NOT NULL
                        AND phone_number IS NOT NULL
                        AND supplier_id IS NOT NULL
                    )
                    SELECT supplier_id, supplier_name, contact_email, phone_number, created_at, updated_at
                    FROM cleaned_data WHERE rn = 1
                """,

                'products': """
                    CREATE TABLE IF NOT EXISTS silver.products_base AS
                    WITH cleaned_data AS (
                        SELECT
                            product_id,
                            UPPER(TRIM(sku)) as sku,
                            TRIM(INITCAP(product_name)) as product_name,
                            CAST(unit_cost AS NUMERIC(10,2)) as unit_cost,
                            supplier_id,
                            created_at,
                            updated_at,
                            ROW_NUMBER() OVER (PARTITION BY sku ORDER BY updated_at DESC) as rn
                        FROM bronze.products
                        WHERE sku IS NOT NULL
                        AND product_name IS NOT NULL
                        AND unit_cost IS NOT NULL
                        AND unit_cost::NUMERIC > 0
                        AND product_id IS NOT NULL
                    )
                    SELECT product_id, sku, product_name, unit_cost, supplier_id, created_at, updated_at
                    FROM cleaned_data WHERE rn = 1
                """,

                'warehouses': """
                    CREATE TABLE IF NOT EXISTS silver.warehouses_base AS
                    WITH cleaned_data AS (
                        SELECT
                            warehouse_id,
                            TRIM(INITCAP(warehouse_name)) as warehouse_name,
                            TRIM(INITCAP(location_city)) as location_city,
                            CAST(storage_capacity AS INTEGER) as storage_capacity,
                            created_at,
                            updated_at,
                            ROW_NUMBER() OVER (PARTITION BY warehouse_name ORDER BY updated_at DESC) as rn
                        FROM bronze.warehouses
                        WHERE warehouse_name IS NOT NULL
                        AND location_city IS NOT NULL
                        AND storage_capacity IS NOT NULL
                        AND storage_capacity::INTEGER > 0
                        AND warehouse_id IS NOT NULL
                    )
                    SELECT warehouse_id, warehouse_name, location_city, storage_capacity, created_at, updated_at
                    FROM cleaned_data WHERE rn = 1
                """,

                'inventory': """
                    CREATE TABLE IF NOT EXISTS silver.inventory_base AS
                    WITH cleaned_data AS (
                        SELECT
                            inventory_id,
                            product_id,
                            warehouse_id,
                            CAST(quantity_on_hand AS INTEGER) as quantity_on_hand,
                            last_stocked_date,
                            created_at,
                            updated_at,
                            ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id ORDER BY updated_at DESC) as rn
                        FROM bronze.inventory
                        WHERE product_id IS NOT NULL
                        AND warehouse_id IS NOT NULL
                        AND quantity_on_hand IS NOT NULL
                        AND quantity_on_hand::INTEGER >= 0
                        AND inventory_id IS NOT NULL
                    )
                    SELECT inventory_id, product_id, warehouse_id, quantity_on_hand, last_stocked_date, created_at, updated_at
                    FROM cleaned_data WHERE rn = 1
                """,

                'shipments': """
                    CREATE TABLE IF NOT EXISTS silver.shipments_base AS
                    WITH cleaned_data AS (
                        SELECT
                            shipment_id,
                            product_id,
                            warehouse_id,
                            CAST(quantity_shipped AS INTEGER) as quantity_shipped,
                            shipment_date,
                            TRIM(INITCAP(destination)) as destination,
                            CASE
                                WHEN UPPER(TRIM(status)) IN ('IN TRANSIT', 'INTRANSIT') THEN 'In Transit'
                                WHEN UPPER(TRIM(status)) = 'DELIVERED' THEN 'Delivered'
                                WHEN UPPER(TRIM(status)) = 'DELAYED' THEN 'Delayed'
                                WHEN UPPER(TRIM(status)) = 'CANCELLED' THEN 'Cancelled'
                                WHEN UPPER(TRIM(status)) = 'PENDING' THEN 'Pending'
                                WHEN UPPER(TRIM(status)) = 'RETURNED' THEN 'Returned'
                                ELSE TRIM(INITCAP(status))
                            END as status,
                            CAST(weight_kg AS NUMERIC(10,2)) as weight_kg,
                            created_at,
                            updated_at,
                            ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY updated_at DESC) as rn
                        FROM bronze.shipments
                        WHERE product_id IS NOT NULL
                        AND warehouse_id IS NOT NULL
                        AND quantity_shipped IS NOT NULL
                        AND quantity_shipped::INTEGER > 0
                        AND destination IS NOT NULL
                        AND shipment_id IS NOT NULL
                    )
                    SELECT shipment_id, product_id, warehouse_id, quantity_shipped, shipment_date,
                           destination, status, weight_kg, created_at, updated_at
                    FROM cleaned_data WHERE rn = 1
                """
            }

            # Execute SQL scripts for each table
            for table_name, sql_script in sql_scripts.items():
                logger.info(f"Creating silver.{table_name}_base...")

                # Drop table if exists (for rerun capability)
                cursor.execute(f"DROP TABLE IF EXISTS silver.{table_name}_base")

                # Create new table
                cursor.execute(sql_script)

                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM silver.{table_name}_base")
                count = cursor.fetchone()[0]

                logger.info(f"✅ silver.{table_name}_base created with {count:,} rows")

                # Log to audit
                self.log_etl_step(f"create_base_{table_name}", table_name, None, count, 0)

            conn.commit()
            logger.info("✅ All Silver base tables created successfully")
            return True

        except psycopg2.Error as e:
            logger.error(f"Error creating Silver base tables: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def deep_validation(self):
        """Step 3: Deep validation using Python/Pandas."""
        logger.info("Performing deep validation...")

        validation_results = {}

        for table_name in self.tables:
            logger.info(f"Validating {table_name}...")
            result = self._validate_table(table_name)
            validation_results[table_name] = result

        all_success = all(validation_results.values())
        if all_success:
            logger.info("✅ All tables passed deep validation")
        else:
            logger.warning("⚠️  Some tables had validation issues")

        return all_success

    def _validate_table(self, table_name):
        """Validate individual table and separate valid/invalid rows."""
        conn = self.get_connection()
        if not conn:
            return False

        try:
            # Load data into pandas using SQLAlchemy engine
            from sqlalchemy import create_engine
            engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
            df = pd.read_sql(f"SELECT * FROM silver.{table_name}_base", engine)
            logger.info(f"Loaded {len(df):,} rows for validation")

            if df.empty:
                logger.warning(f"No data found in silver.{table_name}_base")
                return True

            # Apply validations based on table type
            valid_df, invalid_df, rejection_reasons = self._apply_table_validations(table_name, df)

            # Save valid rows back to final silver table
            if not valid_df.empty:
                valid_df.to_sql(table_name, engine, schema='silver', if_exists='replace', index=False)
                logger.info(f"✅ {len(valid_df):,} valid rows saved to silver.{table_name}")

            # Save invalid rows to audit.rejected_rows
            if not invalid_df.empty:
                self._save_rejected_rows(table_name, invalid_df, rejection_reasons)
                logger.warning(f"⚠️  {len(invalid_df):,} invalid rows saved to audit.rejected_rows")

            # Log statistics
            self.stats[table_name] = {
                'input_rows': len(df),
                'valid_rows': len(valid_df),
                'invalid_rows': len(invalid_df)
            }

            # Log to audit
            self.log_etl_step(f"deep_validation_{table_name}", table_name,
                            len(df), len(valid_df), len(invalid_df))

            return True

        except Exception as e:
            logger.error(f"Error validating {table_name}: {e}")
            return False
        finally:
            conn.close()
            engine.dispose()

    def _apply_table_validations(self, table_name, df):
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
            invalid_cost = (df['unit_cost'] <= 0) | (df['unit_cost'] > 10000)
            valid_mask &= ~invalid_cost
            rejection_reasons = [r if not ic else 'Unit cost out of range (0-10000)'
                               for r, ic in zip(rejection_reasons, invalid_cost)]

            # SKU format validation (should be alphanumeric)
            invalid_sku = ~df['sku'].str.match(r'^[A-Z0-9]{3,20}$')
            valid_mask &= ~invalid_sku
            rejection_reasons = [r if not isk else 'Invalid SKU format'
                               for r, isk in zip(rejection_reasons, invalid_sku)]

        elif table_name == 'warehouses':
            # Storage capacity validation
            invalid_capacity = (df['storage_capacity'] <= 0) | (df['storage_capacity'] > 1000000)
            valid_mask &= ~invalid_capacity
            rejection_reasons = [r if not ic else 'Storage capacity out of range'
                               for r, ic in zip(rejection_reasons, invalid_capacity)]

        elif table_name == 'inventory':
            # Quantity validation
            invalid_quantity = df['quantity_on_hand'] < 0
            valid_mask &= ~invalid_quantity
            rejection_reasons = [r if not iq else 'Negative quantity'
                               for r, iq in zip(rejection_reasons, invalid_quantity)]

            # Date validation
            today = pd.Timestamp.now()
            future_date = df['last_stocked_date'] > today
            valid_mask &= ~future_date
            rejection_reasons = [r if not fd else 'Future stocking date'
                               for r, fd in zip(rejection_reasons, future_date)]

        elif table_name == 'shipments':
            # Status validation
            valid_statuses = ['In Transit', 'Delivered', 'Delayed', 'Cancelled', 'Pending', 'Returned']
            invalid_status = ~df['status'].isin(valid_statuses)
            valid_mask &= ~invalid_status
            rejection_reasons = [r if not ist else 'Invalid status'
                               for r, ist in zip(rejection_reasons, invalid_status)]

            # Weight validation
            invalid_weight = (df['weight_kg'] <= 0) | (df['weight_kg'] > 50000)
            valid_mask &= ~invalid_weight
            rejection_reasons = [r if not iw else 'Weight out of range'
                               for r, iw in zip(rejection_reasons, invalid_weight)]

            # Quantity validation
            invalid_qty = df['quantity_shipped'] <= 0
            valid_mask &= ~invalid_qty
            rejection_reasons = [r if not iq else 'Invalid quantity shipped'
                               for r, iq in zip(rejection_reasons, invalid_qty)]

        valid_df = df[valid_mask].copy()
        invalid_df = df[~valid_mask].copy()
        invalid_reasons = [r for r, v in zip(rejection_reasons, ~valid_mask) if v]

        return valid_df, invalid_df, invalid_reasons

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
            logger.info(f"Saved {len(invalid_df)} rejected rows for {table_name}")

        except psycopg2.Error as e:
            logger.error(f"Error saving rejected rows: {e}")
        finally:
            cursor.close()
            conn.close()

    def run_data_quality_checks(self):
        """Step 4: Run data quality checks."""
        logger.info("Running Data Quality checks...")

        checks = {
            'suppliers': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT supplier_id) FROM silver.suppliers"),
                ('email_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT contact_email) FROM silver.suppliers"),
                ('null_check', "SELECT COUNT(*) FROM silver.suppliers WHERE supplier_name IS NULL OR contact_email IS NULL")
            ],
            'products': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM silver.products"),
                ('sku_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT sku) FROM silver.products"),
                ('positive_cost', "SELECT COUNT(*) FROM silver.products WHERE unit_cost <= 0")
            ],
            'warehouses': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT warehouse_id) FROM silver.warehouses"),
                ('name_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT warehouse_name) FROM silver.warehouses"),
                ('positive_capacity', "SELECT COUNT(*) FROM silver.warehouses WHERE storage_capacity <= 0")
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
            'shipments': [
                ('fk_product_valid', """SELECT COUNT(*) FROM silver.shipments s
                                       LEFT JOIN silver.products p ON s.product_id = p.product_id
                                       WHERE p.product_id IS NULL"""),
                ('fk_warehouse_valid', """SELECT COUNT(*) FROM silver.shipments s
                                         LEFT JOIN silver.warehouses w ON s.warehouse_id = w.warehouse_id
                                         WHERE w.warehouse_id IS NULL"""),
                ('valid_status', """SELECT COUNT(*) FROM silver.shipments
                                   WHERE status NOT IN ('In Transit','Delivered','Delayed','Cancelled','Pending','Returned')""")
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

            # Calculate checksum if output_count > 0
            checksum = None
            if output_count and output_count > 0:
                checksum = self._calculate_checksum(table_name)

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

    def _calculate_checksum(self, table_name):
        """Calculate MD5 checksum of table data."""
        conn = self.get_connection()
        if not conn:
            return None

        try:
            cursor = conn.cursor()

            # Simple checksum based on row count and key fields
            if table_name in ['suppliers', 'products', 'warehouses', 'inventory', 'shipments']:
                # Check if final table exists, otherwise use base table
                try:
                    cursor.execute(f"SELECT 1 FROM silver.{table_name} LIMIT 1")
                    table_to_use = f"silver.{table_name}"
                except:
                    table_to_use = f"silver.{table_name}_base"

                cursor.execute(f"""
                    SELECT MD5(STRING_AGG(md5_row, '' ORDER BY md5_row))
                    FROM (
                        SELECT MD5(CAST(ROW_TO_JSON(t.*) AS TEXT)) as md5_row
                        FROM (SELECT * FROM {table_to_use} LIMIT 1000) t
                    ) sub
                """)
                result = cursor.fetchone()
                return result[0] if result else None

        except psycopg2.Error as e:
            logger.error(f"Error calculating checksum: {e}")
            return None
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
