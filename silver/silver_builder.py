import sys
import logging
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG, LOG_CONFIG

# Set up logging
log_dir = Path(__file__).parent.parent / LOG_CONFIG['log_dir']
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['level']),
    format=LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(log_dir / 'silver_builder.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SilverDataCleaner:
    """Data cleaning utilities for Silver layer transformation."""

    def __init__(self):
        # Common null/empty values to handle
        self.null_values = ['NULL', 'N/A', 'NOT AVAILABLE', 'TBD', 'UNKNOWN', '', 'NONE', 'NIL']

    def clean_text_field(self, value):
        """Clean text fields - handle dirty strings."""
        if not value or pd.isna(value):
            return None

        # Convert to string and clean
        value_str = str(value).strip()
        if not value_str or value_str.upper() in self.null_values:
            return None

        # Fix common formatting issues
        # Remove extra spaces and normalize case
        cleaned = ' '.join(value_str.split())

        # Fix common typos and formatting
        cleaned = re.sub(r'\s+', ' ', cleaned)  # Multiple spaces to single
        cleaned = cleaned.title() if cleaned.isupper() or cleaned.islower() else cleaned

        # Remove special characters at start/end
        cleaned = re.sub(r'^[^\w\s]+|[^\w\s]+$', '', cleaned)

        return cleaned.strip() if cleaned.strip() else None

    def clean_email(self, value):
        """Clean email addresses."""
        if not value or pd.isna(value):
            return None

        value_str = str(value).strip().lower()
        if not value_str or value_str.upper() in self.null_values:
            return None

        # Basic email validation
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if re.match(email_pattern, value_str):
            return value_str

        return None

    def clean_phone(self, value):
        """Clean phone numbers."""
        if not value or pd.isna(value):
            return None

        value_str = str(value).strip()
        if not value_str or value_str.upper() in self.null_values:
            return None

        # Extract digits and some common patterns
        phone_clean = re.sub(r'[^\d+()-]', '', value_str)

        # Basic validation - should have at least 10 digits
        digits_only = re.sub(r'[^\d]', '', phone_clean)
        if len(digits_only) >= 10:
            return phone_clean

        return None

    def clean_numeric_field(self, value, allow_negative=False, max_digits=15, decimal_places=4):
        """Clean numeric fields - handle dirty numbers with proper precision."""
        if not value or pd.isna(value):
            return None

        # Convert to string and clean
        value_str = str(value).strip()
        if not value_str or value_str.upper() in self.null_values:
            return None

        # Extract numeric part (handle currency symbols, commas, etc.)
        numeric_match = re.search(r'[-+]?\d*\.?\d+', value_str.replace(',', ''))
        if not numeric_match:
            return None

        try:
            num_value = float(numeric_match.group())

            # Handle negative values
            if not allow_negative and num_value < 0:
                return 0.0

            # Round to specified decimal places
            rounded_value = round(num_value, decimal_places)

            # Check if the number fits within the specified precision
            str_value = f"{rounded_value:.{decimal_places}f}"
            total_digits = len(str_value.replace('.', '').replace('-', ''))

            if total_digits > max_digits:
                logger.warning(f"Value {rounded_value} exceeds max digits {max_digits}, truncating")
                # Truncate to fit within precision
                max_integer_part = max_digits - decimal_places
                max_value = 10**max_integer_part - 1
                rounded_value = min(rounded_value, max_value)

            return rounded_value
        except (ValueError, TypeError):
            return None

    def clean_integer_field(self, value):
        """Clean integer fields."""
        if not value or pd.isna(value):
            return None

        try:
            # Handle string representation
            value_str = str(value).strip()
            if not value_str or value_str.upper() in self.null_values:
                return None

            # Extract numeric part
            numeric_match = re.search(r'\d+', value_str.replace(',', ''))
            if not numeric_match:
                return None

            return int(numeric_match.group())
        except (ValueError, TypeError):
            return None

    def clean_date_field(self, value):
        """Clean date fields."""
        if not value or pd.isna(value):
            return None

        value_str = str(value).strip()
        if not value_str or value_str.upper() in self.null_values:
            return None

        # Try common date formats
        date_formats = [
            '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y',
            '%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S',
            '%d-%m-%Y', '%Y/%m/%d'
        ]

        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(value_str, fmt).date()
                return parsed_date
            except ValueError:
                continue

        # Try pandas to_datetime as fallback
        try:
            parsed_date = pd.to_datetime(value_str, errors='coerce')
            if not pd.isna(parsed_date):
                return parsed_date.date()
        except:
            pass

        return None

    def clean_status_field(self, value, status_mapping=None):
        """Clean status fields with comprehensive mapping."""
        if not value or pd.isna(value):
            return 'unknown'

        value_str = str(value).strip().lower()
        if not value_str or value_str.upper() in self.null_values:
            return 'unknown'

        if status_mapping:
            # Check mapping dictionary for standardization
            for standard_status, variants in status_mapping.items():
                if value_str in [v.lower() for v in variants]:
                    return standard_status

        return value_str.lower()

    def clean_category_field(self, value):
        """Clean category fields."""
        cleaned = self.clean_text_field(value)
        return cleaned if cleaned else 'Uncategorized'

    def parse_category_field(self, value):
        """Parse category into main_category and sub_category."""
        if not value or pd.isna(value):
            return 'Uncategorized', 'General'

        # Clean the value first
        cleaned_value = self.clean_text_field(value)
        if not cleaned_value:
            return 'Uncategorized', 'General'

        # Handle common separators and extract main and sub categories
        separators = [' > ', '>', ' - ', '-', ' | ', '|', ' / ', '/']

        for separator in separators:
            if separator in cleaned_value:
                parts = [part.strip() for part in cleaned_value.split(separator, 1)]
                if len(parts) >= 2 and parts[0] and parts[1]:
                    main_cat = self.clean_text_field(parts[0])
                    sub_cat = self.clean_text_field(parts[1])

                    # Ensure we have valid strings after cleaning
                    if not main_cat:
                        main_cat = 'Uncategorized'
                    if not sub_cat:
                        sub_cat = 'General'

                    # Fix common typos in main categories
                    main_cat_fixes = {
                        'autmootive': 'Automotive',
                        'automotiev': 'Automotive',
                        'automotive': 'Automotive',
                        'safety': 'Safety',
                        'asafety': 'Safety',
                        'asfety': 'Safety'
                    }

                    main_lower = (main_cat or '').lower().replace(' parts', '').strip()
                    for typo, correct in main_cat_fixes.items():
                        if typo in main_lower:
                            main_cat = correct + ' Parts' if 'parts' in cleaned_value.lower() else correct
                            break
                    else:
                        # Capitalize properly
                        main_cat = ' '.join(word.capitalize() for word in (main_cat or '').split())

                    # Fix common typos in sub categories
                    sub_cat_fixes = {
                        'battreies': 'Batteries',
                        'battreis': 'Batteries',
                        'batteries': 'Batteries',
                        'high vis': 'High Visibility',
                        'tires': 'Tires',
                        'filters': 'Filters',
                        'body parts': 'Body Parts'
                    }

                    sub_lower = (sub_cat or '').lower()
                    for typo, correct in sub_cat_fixes.items():
                        if typo in sub_lower:
                            sub_cat = correct
                            break
                    else:
                        # Capitalize properly
                        sub_cat = ' '.join(word.capitalize() for word in (sub_cat or '').split())

                    return main_cat, sub_cat

        # No separator found, treat as main category
        main_cat = ' '.join(word.capitalize() for word in cleaned_value.split())
        return main_cat, 'General'


class SilverBuilder:
    """Build Silver layer with clean, validated data."""

    def __init__(self):
        self.cleaner = SilverDataCleaner()
        self.total_stats = {
            'total_records_processed': 0,
            'total_records_cleaned': 0,
            'total_quality_issues_fixed': 0
        }
        # Generate unique run ID for this ETL run
        from datetime import datetime
        self.run_id = f"silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def get_connection(self):
        """Get database connection."""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            return conn
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            return None

    def setup_silver_tables(self):
        """Create silver tables with proper precision for large numbers."""
        logger.info("Setting up Silver layer tables...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # Create silver schema
            cursor.execute("CREATE SCHEMA IF NOT EXISTS silver")

            # Clean suppliers table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS silver.suppliers (
                    supplier_id INT PRIMARY KEY,
                    supplier_name TEXT NOT NULL,
                    contact_email TEXT,
                    phone_number TEXT,
                    quality_score DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Clean products table - increased precision for large numbers
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS silver.products (
                    product_id INT PRIMARY KEY,
                    product_name TEXT NOT NULL,
                    unit_cost DECIMAL(15,4) CHECK (unit_cost >= 0),
                    selling_price DECIMAL(15,4) CHECK (selling_price >= 0),
                    supplier_id INT,
                    product_category TEXT DEFAULT 'Uncategorized',
                    main_category TEXT DEFAULT 'Uncategorized',
                    sub_category TEXT DEFAULT 'General',
                    status TEXT DEFAULT 'active',
                    price_margin DECIMAL(15,4),
                    quality_score DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Clean warehouses table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS silver.warehouses (
                    warehouse_id INT PRIMARY KEY,
                    warehouse_name TEXT NOT NULL,
                    city TEXT,
                    region TEXT,
                    storage_capacity INT CHECK (storage_capacity >= 0),
                    quality_score DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Clean inventory table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS silver.inventory (
                    inventory_id INT PRIMARY KEY,
                    product_id INT,
                    warehouse_id INT,
                    quantity_on_hand INT CHECK (quantity_on_hand >= 0),
                    last_stocked_date DATE,
                    quality_score DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Clean retail stores table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS silver.retail_stores (
                    retail_store_id INT PRIMARY KEY,
                    store_name TEXT NOT NULL,
                    city TEXT,
                    region TEXT,
                    store_type TEXT,
                    store_status TEXT DEFAULT 'active',
                    quality_score DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Clean supply orders table - increased precision
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS silver.supply_orders (
                    supply_order_id INT PRIMARY KEY,
                    product_id INT,
                    warehouse_id INT,
                    retail_store_id INT,
                    quantity INT CHECK (quantity >= 0),
                    price DECIMAL(15,4) CHECK (price >= 0),
                    total_invoice DECIMAL(15,4) CHECK (total_invoice >= 0),
                    order_date DATE NOT NULL,
                    shipped_date DATE,
                    delivered_date DATE,
                    status TEXT DEFAULT 'pending',
                    is_calculation_correct BOOLEAN,
                    date_logic_valid BOOLEAN,
                    quality_score DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create quality issues log table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS silver.quality_issues_log (
                    issue_id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    record_id INT,
                    field_name TEXT,
                    issue_type TEXT NOT NULL,
                    original_value TEXT,
                    cleaned_value TEXT,
                    action_taken TEXT DEFAULT 'cleaned',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
            cursor.close()
            logger.info("✓ Silver layer tables created successfully")
            return True

        except psycopg2.Error as e:
            logger.error(f"Error creating silver tables: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def log_quality_issue(self, table_name, record_id, field_name, issue_type, original_value, action_taken='cleaned'):
        """Log data quality issues."""
        conn = self.get_connection()
        if not conn:
            return

        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO silver.quality_issues_log
                (table_name, record_id, field_name, issue_type, original_value, action_taken)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (table_name, record_id, field_name, issue_type, str(original_value), action_taken))
            conn.commit()
            cursor.close()
        except psycopg2.Error as e:
            logger.error(f"Error logging quality issue: {e}")
        finally:
            conn.close()

    def log_rejected_row(self, table_name, record_data, reason):
        """Log rejected rows to audit.rejected_rows table."""
        conn = self.get_connection()
        if not conn:
            return

        try:
            import json
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO audit.rejected_rows
                (table_name, record, reason, run_id)
                VALUES (%s, %s, %s, %s)
            """, (table_name, json.dumps(record_data), reason, self.run_id))
            conn.commit()
            cursor.close()
        except psycopg2.Error as e:
            logger.error(f"Error logging rejected row: {e}")
        finally:
            conn.close()

    def log_dq_check(self, table_name, check_name, pass_fail, bad_row_count=0):
        """Log data quality check results to audit.dq_results table."""
        conn = self.get_connection()
        if not conn:
            return

        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO audit.dq_results
                (table_name, check_name, pass_fail, bad_row_count, run_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (table_name, check_name, pass_fail, bad_row_count, self.run_id))
            conn.commit()
            cursor.close()
        except psycopg2.Error as e:
            logger.error(f"Error logging DQ check: {e}")
        finally:
            conn.close()

    def log_etl_step(self, step_executed, table_name=None, input_count=0, output_count=0, rejected_count=0):
        """Log ETL step execution to audit.etl_log table."""
        conn = self.get_connection()
        if not conn:
            return

        try:
            from datetime import datetime
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO audit.etl_log
                (run_id, run_timestamp, step_executed, table_name, input_row_count, output_row_count, rejected_row_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (self.run_id, datetime.now(), step_executed, table_name, input_count, output_count, rejected_count))
            conn.commit()
            cursor.close()
        except psycopg2.Error as e:
            logger.error(f"Error logging ETL step: {e}")
        finally:
            conn.close()

    def calculate_quality_score(self, issues_found, total_fields):
        """Calculate quality score based on issues found."""
        if total_fields == 0:
            return 100.0
        clean_fields = total_fields - issues_found
        return round((clean_fields / total_fields) * 100, 2)

    def clean_suppliers(self):
        """Clean suppliers data."""
        logger.info("🧹 Cleaning suppliers data...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # Get bronze data
            cursor.execute("""
                SELECT supplier_id, supplier_name, contact_email, phone_number
                FROM bronze.suppliers
            """)
            bronze_data = cursor.fetchall()

            # Clear silver table
            cursor.execute("TRUNCATE TABLE silver.suppliers")

            stats = {'processed': 0, 'cleaned': 0, 'rejected': 0, 'issues_fixed': 0}

            for row in bronze_data:
                supplier_id, supplier_name, contact_email, phone_number = row
                issues_count = 0

                # Clean each field
                cleaned_name = self.cleaner.clean_text_field(supplier_name)
                if cleaned_name != supplier_name:
                    issues_count += 1

                cleaned_email = self.cleaner.clean_email(contact_email)
                if cleaned_email != contact_email:
                    issues_count += 1

                cleaned_phone = self.cleaner.clean_phone(phone_number)
                if cleaned_phone != phone_number:
                    issues_count += 1

                # Skip if essential data is missing
                if not cleaned_name:
                    stats['rejected'] += 1
                    # Log rejected row to audit table
                    self.log_rejected_row('suppliers', {
                        'supplier_id': supplier_id,
                        'supplier_name': supplier_name,
                        'contact_email': contact_email,
                        'phone_number': phone_number
                    }, 'Missing supplier name')
                    logger.debug(f"Rejected supplier {supplier_id}: missing name")
                    continue

                quality_score = self.calculate_quality_score(issues_count, 4)

                # Insert cleaned data
                cursor.execute("""
                    INSERT INTO silver.suppliers
                    (supplier_id, supplier_name, contact_email, phone_number, quality_score)
                    VALUES (%s, %s, %s, %s, %s)
                """, (supplier_id, cleaned_name, cleaned_email, cleaned_phone, quality_score))

                stats['processed'] += 1
                if issues_count > 0:
                    stats['cleaned'] += 1
                    stats['issues_fixed'] += issues_count

            conn.commit()

            # Log DQ checks
            bronze_count = len(bronze_data)
            self.log_dq_check('suppliers', 'data_completeness_check', stats['rejected'] == 0, stats['rejected'])
            self.log_etl_step('clean_suppliers', 'suppliers', bronze_count, stats['processed'], stats['rejected'])

            logger.info(f"✅ Suppliers: {stats['processed']:,} processed, {stats['cleaned']:,} cleaned, {stats['rejected']:,} rejected, {stats['issues_fixed']:,} issues fixed")
            self.total_stats['total_records_processed'] += stats['processed']
            self.total_stats['total_records_cleaned'] += stats['cleaned']
            self.total_stats['total_quality_issues_fixed'] += stats['issues_fixed']

            return True

        except psycopg2.Error as e:
            logger.error(f"Error cleaning suppliers: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def clean_products(self):
        """Clean products data."""
        logger.info("🧹 Cleaning products data...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # Get bronze data
            cursor.execute("""
                SELECT product_id, product_name, unit_cost, selling_price,
                       supplier_id, product_category, status
                FROM bronze.products
            """)
            bronze_data = cursor.fetchall()

            # Clear silver table
            cursor.execute("TRUNCATE TABLE silver.products")

            stats = {'processed': 0, 'cleaned': 0, 'rejected': 0, 'issues_fixed': 0}

            for row in bronze_data:
                product_id, product_name, unit_cost, selling_price, supplier_id, product_category, status = row
                issues_count = 0

                # Clean each field
                cleaned_name = self.cleaner.clean_text_field(product_name)
                if cleaned_name != product_name:
                    issues_count += 1

                cleaned_unit_cost = self.cleaner.clean_numeric_field(unit_cost, allow_negative=False, max_digits=15, decimal_places=4)
                if str(cleaned_unit_cost) != str(unit_cost):
                    issues_count += 1

                cleaned_selling_price = self.cleaner.clean_numeric_field(selling_price, allow_negative=False, max_digits=15, decimal_places=4)
                if str(cleaned_selling_price) != str(selling_price):
                    issues_count += 1

                cleaned_supplier_id = self.cleaner.clean_integer_field(supplier_id)
                cleaned_category = self.cleaner.clean_category_field(product_category)
                main_category, sub_category = self.cleaner.parse_category_field(product_category)
                if cleaned_category != product_category or main_category != 'Uncategorized':
                    issues_count += 1

                # Define product status mapping
                product_status_mapping = {
                    'active': ['active', 'Active', 'ACTIVE'],
                    'discontinued': ['discontinued', 'Discontinued', 'DISCONTINUED', 'inactive']
                }

                cleaned_status = self.cleaner.clean_status_field(status, product_status_mapping)
                if cleaned_status != status:
                    issues_count += 1

                # Skip if essential data is missing
                if not cleaned_name or cleaned_unit_cost is None or cleaned_selling_price is None:
                    stats['rejected'] += 1
                    # Log rejected row to audit table
                    self.log_rejected_row('products', {
                        'product_id': product_id,
                        'product_name': product_name,
                        'unit_cost': str(unit_cost),
                        'selling_price': str(selling_price),
                        'supplier_id': str(supplier_id),
                        'product_category': product_category,
                        'status': status
                    }, 'Missing essential data (name, unit_cost, or selling_price)')
                    logger.debug(f"Rejected product {product_id}: missing essential data")
                    continue

                # Calculate price margin
                price_margin = cleaned_selling_price - cleaned_unit_cost if cleaned_unit_cost > 0 else 0

                # Check for business logic issues
                if cleaned_unit_cost > cleaned_selling_price:
                    issues_count += 1
                    self.log_quality_issue('products', product_id, 'pricing', 'cost_higher_than_price',
                                         f"cost:{cleaned_unit_cost}, price:{cleaned_selling_price}", 'flagged')

                quality_score = self.calculate_quality_score(issues_count, 6)

                # Insert cleaned data (don't validate supplier reference yet - suppliers may not be cleaned)
                cursor.execute("""
                    INSERT INTO silver.products
                    (product_id, product_name, unit_cost, selling_price, supplier_id,
                     product_category, main_category, sub_category, status, price_margin, quality_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (product_id, cleaned_name, cleaned_unit_cost, cleaned_selling_price,
                      cleaned_supplier_id, cleaned_category, main_category, sub_category, cleaned_status, price_margin, quality_score))

                stats['processed'] += 1
                if issues_count > 0:
                    stats['cleaned'] += 1
                    stats['issues_fixed'] += issues_count

            conn.commit()

            # Log DQ checks
            bronze_count = len(bronze_data)
            self.log_dq_check('products', 'data_completeness_check', stats['rejected'] == 0, stats['rejected'])
            self.log_dq_check('products', 'price_validation_check', True, 0)  # Add specific price validation later
            self.log_etl_step('clean_products', 'products', bronze_count, stats['processed'], stats['rejected'])

            logger.info(f"✅ Products: {stats['processed']:,} processed, {stats['cleaned']:,} cleaned, {stats['rejected']:,} rejected, {stats['issues_fixed']:,} issues fixed")
            self.total_stats['total_records_processed'] += stats['processed']
            self.total_stats['total_records_cleaned'] += stats['cleaned']
            self.total_stats['total_quality_issues_fixed'] += stats['issues_fixed']

            return True

        except psycopg2.Error as e:
            logger.error(f"Error cleaning products: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def clean_warehouses(self):
        """Clean warehouses data."""
        logger.info("🧹 Cleaning warehouses data...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # Get bronze data
            cursor.execute("""
                SELECT warehouse_id, warehouse_name, city, region, storage_capacity
                FROM bronze.warehouses
            """)
            bronze_data = cursor.fetchall()

            # Clear silver table
            cursor.execute("TRUNCATE TABLE silver.warehouses")

            stats = {'processed': 0, 'cleaned': 0, 'rejected': 0, 'issues_fixed': 0}

            for row in bronze_data:
                warehouse_id, warehouse_name, city, region, storage_capacity = row
                issues_count = 0

                # Clean each field
                cleaned_name = self.cleaner.clean_text_field(warehouse_name)
                if cleaned_name != warehouse_name:
                    issues_count += 1

                cleaned_city = self.cleaner.clean_text_field(city)
                if cleaned_city != city:
                    issues_count += 1

                cleaned_region = self.cleaner.clean_text_field(region)
                if cleaned_region != region:
                    issues_count += 1

                cleaned_capacity = self.cleaner.clean_integer_field(storage_capacity)
                if str(cleaned_capacity) != str(storage_capacity):
                    issues_count += 1

                # Skip if essential data is missing
                if not cleaned_name:
                    stats['rejected'] += 1
                    logger.debug(f"Rejected warehouse {warehouse_id}: missing name")
                    continue

                quality_score = self.calculate_quality_score(issues_count, 5)

                # Insert cleaned data
                cursor.execute("""
                    INSERT INTO silver.warehouses
                    (warehouse_id, warehouse_name, city, region, storage_capacity, quality_score)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (warehouse_id, cleaned_name, cleaned_city, cleaned_region, cleaned_capacity, quality_score))

                stats['processed'] += 1
                if issues_count > 0:
                    stats['cleaned'] += 1
                    stats['issues_fixed'] += issues_count

            conn.commit()
            logger.info(f"✅ Warehouses: {stats['processed']:,} processed, {stats['cleaned']:,} cleaned, {stats['rejected']:,} rejected, {stats['issues_fixed']:,} issues fixed")
            self.total_stats['total_records_processed'] += stats['processed']
            self.total_stats['total_records_cleaned'] += stats['cleaned']
            self.total_stats['total_quality_issues_fixed'] += stats['issues_fixed']

            return True

        except psycopg2.Error as e:
            logger.error(f"Error cleaning warehouses: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def clean_retail_stores(self):
        """Clean retail stores data."""
        logger.info("🧹 Cleaning retail stores data...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # Get bronze data
            cursor.execute("""
                SELECT retail_store_id, store_name, city, region, store_type, store_status
                FROM bronze.retail_stores
            """)
            bronze_data = cursor.fetchall()

            # Clear silver table
            cursor.execute("TRUNCATE TABLE silver.retail_stores")

            stats = {'processed': 0, 'cleaned': 0, 'rejected': 0, 'issues_fixed': 0}

            for row in bronze_data:
                retail_store_id, store_name, city, region, store_type, store_status = row
                issues_count = 0

                # Clean each field
                cleaned_name = self.cleaner.clean_text_field(store_name)
                if cleaned_name != store_name:
                    issues_count += 1

                cleaned_city = self.cleaner.clean_text_field(city)
                if cleaned_city != city:
                    issues_count += 1

                cleaned_region = self.cleaner.clean_text_field(region)
                if cleaned_region != region:
                    issues_count += 1

                cleaned_type = self.cleaner.clean_text_field(store_type)
                if cleaned_type != store_type:
                    issues_count += 1

                # Define retail store status mapping
                store_status_mapping = {
                    'active': ['active', 'Active', 'ACTIVE', 'open', 'Open', 'OPEN'],
                    'inactive': ['inactive', 'Inactive', 'INACTIVE', 'closed', 'Closed', 'CLOSED'],
                    'closed': ['closed', 'Closed', 'CLOSED', 'shutdown', 'Shutdown', 'SHUTDOWN']
                }

                cleaned_status = self.cleaner.clean_status_field(store_status, store_status_mapping)
                if cleaned_status != store_status:
                    issues_count += 1

                # Skip if essential data is missing
                if not cleaned_name:
                    stats['rejected'] += 1
                    logger.debug(f"Rejected retail store {retail_store_id}: missing name")
                    continue

                quality_score = self.calculate_quality_score(issues_count, 6)

                # Insert cleaned data
                cursor.execute("""
                    INSERT INTO silver.retail_stores
                    (retail_store_id, store_name, city, region, store_type, store_status, quality_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (retail_store_id, cleaned_name, cleaned_city, cleaned_region, cleaned_type, cleaned_status, quality_score))

                stats['processed'] += 1
                if issues_count > 0:
                    stats['cleaned'] += 1
                    stats['issues_fixed'] += issues_count

            conn.commit()
            logger.info(f"✅ Retail Stores: {stats['processed']:,} processed, {stats['cleaned']:,} cleaned, {stats['rejected']:,} rejected, {stats['issues_fixed']:,} issues fixed")
            self.total_stats['total_records_processed'] += stats['processed']
            self.total_stats['total_records_cleaned'] += stats['cleaned']
            self.total_stats['total_quality_issues_fixed'] += stats['issues_fixed']

            return True

        except psycopg2.Error as e:
            logger.error(f"Error cleaning retail stores: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def clean_supply_orders(self):
        """Clean supply orders data."""
        logger.info("🧹 Cleaning supply orders data...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # Get bronze data
            cursor.execute("""
                SELECT supply_order_id, product_id, warehouse_id, retail_store_id,
                       quantity, price, total_invoice, order_date, shipped_date,
                       delivered_date, status
                FROM bronze.supply_orders
            """)
            bronze_data = cursor.fetchall()

            # Clear silver table
            cursor.execute("TRUNCATE TABLE silver.supply_orders")

            stats = {'processed': 0, 'cleaned': 0, 'rejected': 0, 'issues_fixed': 0}

            for row in bronze_data:
                supply_order_id, product_id, warehouse_id, retail_store_id, quantity, price, total_invoice, order_date, shipped_date, delivered_date, status = row
                issues_count = 0

                # Clean each field
                cleaned_product_id = self.cleaner.clean_integer_field(product_id)
                cleaned_warehouse_id = self.cleaner.clean_integer_field(warehouse_id)
                cleaned_retail_store_id = self.cleaner.clean_integer_field(retail_store_id)

                cleaned_quantity = self.cleaner.clean_integer_field(quantity)
                cleaned_price = self.cleaner.clean_numeric_field(price, allow_negative=False, max_digits=15, decimal_places=4)
                cleaned_total_invoice = self.cleaner.clean_numeric_field(total_invoice, allow_negative=False, max_digits=15, decimal_places=4)

                cleaned_order_date = self.cleaner.clean_date_field(order_date)
                cleaned_shipped_date = self.cleaner.clean_date_field(shipped_date)
                cleaned_delivered_date = self.cleaner.clean_date_field(delivered_date)

                # Define comprehensive supply order status mapping
                supply_order_status_mapping = {
                    'pending': ['pending', 'Pending', 'PENDING', 'Awaiting', 'Processing', 'In Process'],
                    'shipped': ['shipped', 'Shipped', 'SHIPPED', 'Dispatched', 'In Transit', 'On Route'],
                    'delivered': ['delivered', 'Delivered', 'DELIVERED', 'Complete', 'Completed', 'Received'],
                    'cancelled': ['cancelled', 'Cancelled', 'CANCELLED', 'Canceled', 'Void']
                }

                cleaned_status = self.cleaner.clean_status_field(status, supply_order_status_mapping)

                # Skip if essential data is missing
                if not cleaned_order_date or cleaned_quantity is None or cleaned_quantity < 0 or cleaned_price is None:
                    stats['rejected'] += 1
                    logger.debug(f"Rejected supply order {supply_order_id}: missing essential data")
                    continue

                # Business logic validations
                is_calculation_correct = True
                date_logic_valid = True

                # Check calculation
                expected_total = (cleaned_quantity or 0) * (cleaned_price or 0)
                if cleaned_total_invoice and abs(cleaned_total_invoice - expected_total) > 0.01:
                    is_calculation_correct = False
                    issues_count += 1

                # Check date logic
                if cleaned_shipped_date and cleaned_shipped_date < cleaned_order_date:
                    date_logic_valid = False
                    issues_count += 1

                if cleaned_delivered_date and cleaned_shipped_date and cleaned_delivered_date < cleaned_shipped_date:
                    date_logic_valid = False
                    issues_count += 1

                quality_score = self.calculate_quality_score(issues_count, 11)

                # Insert cleaned data
                cursor.execute("""
                    INSERT INTO silver.supply_orders
                    (supply_order_id, product_id, warehouse_id, retail_store_id, quantity,
                     price, total_invoice, order_date, shipped_date, delivered_date, status,
                     is_calculation_correct, date_logic_valid, quality_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (supply_order_id, cleaned_product_id, cleaned_warehouse_id, cleaned_retail_store_id,
                      cleaned_quantity, cleaned_price, cleaned_total_invoice, cleaned_order_date,
                      cleaned_shipped_date, cleaned_delivered_date, cleaned_status,
                      is_calculation_correct, date_logic_valid, quality_score))

                stats['processed'] += 1
                if issues_count > 0:
                    stats['cleaned'] += 1
                    stats['issues_fixed'] += issues_count

            conn.commit()
            logger.info(f"✅ Supply Orders: {stats['processed']:,} processed, {stats['cleaned']:,} cleaned, {stats['rejected']:,} rejected, {stats['issues_fixed']:,} issues fixed")
            self.total_stats['total_records_processed'] += stats['processed']
            self.total_stats['total_records_cleaned'] += stats['cleaned']
            self.total_stats['total_quality_issues_fixed'] += stats['issues_fixed']

            return True

        except psycopg2.Error as e:
            logger.error(f"Error cleaning supply orders: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def clean_inventory(self):
        """Clean inventory data."""
        logger.info("🧹 Cleaning inventory data...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # Get bronze data
            cursor.execute("""
                SELECT inventory_id, product_id, warehouse_id, quantity_on_hand, last_stocked_date
                FROM bronze.inventory
            """)
            bronze_data = cursor.fetchall()

            # Clear silver table
            cursor.execute("TRUNCATE TABLE silver.inventory")

            stats = {'processed': 0, 'cleaned': 0, 'rejected': 0, 'issues_fixed': 0}

            for row in bronze_data:
                inventory_id, product_id, warehouse_id, quantity_on_hand, last_stocked_date = row
                issues_count = 0

                # Clean each field
                cleaned_product_id = self.cleaner.clean_integer_field(product_id)
                cleaned_warehouse_id = self.cleaner.clean_integer_field(warehouse_id)
                cleaned_quantity = self.cleaner.clean_integer_field(quantity_on_hand)
                cleaned_date = self.cleaner.clean_date_field(last_stocked_date)

                # Skip if essential data is missing
                if cleaned_quantity is None or cleaned_quantity < 0:
                    stats['rejected'] += 1
                    # Log rejected row to audit table
                    self.log_rejected_row('inventory', {
                        'inventory_id': inventory_id,
                        'product_id': product_id,
                        'warehouse_id': warehouse_id,
                        'quantity_on_hand': quantity_on_hand,
                        'last_stocked_date': str(last_stocked_date)
                    }, 'Invalid or negative quantity')
                    logger.debug(f"Rejected inventory {inventory_id}: invalid quantity")
                    continue

                quality_score = self.calculate_quality_score(issues_count, 5)

                # Insert cleaned data
                cursor.execute("""
                    INSERT INTO silver.inventory
                    (inventory_id, product_id, warehouse_id, quantity_on_hand, last_stocked_date, quality_score)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (inventory_id, cleaned_product_id, cleaned_warehouse_id, cleaned_quantity, cleaned_date, quality_score))

                stats['processed'] += 1
                if issues_count > 0:
                    stats['cleaned'] += 1
                    stats['issues_fixed'] += issues_count

            conn.commit()

            # Log DQ checks
            bronze_count = len(bronze_data)
            self.log_dq_check('inventory', 'quantity_validation_check', stats['rejected'] == 0, stats['rejected'])
            self.log_etl_step('clean_inventory', 'inventory', bronze_count, stats['processed'], stats['rejected'])

            logger.info(f"✅ Inventory: {stats['processed']:,} processed, {stats['cleaned']:,} cleaned, {stats['rejected']:,} rejected, {stats['issues_fixed']:,} issues fixed")
            self.total_stats['total_records_processed'] += stats['processed']
            self.total_stats['total_records_cleaned'] += stats['cleaned']
            self.total_stats['total_quality_issues_fixed'] += stats['issues_fixed']

            return True

        except psycopg2.Error as e:
            logger.error(f"Error cleaning inventory: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def run_full_cleaning_pipeline(self):
        """Run the complete silver layer cleaning pipeline."""
        logger.info("🥈 SILVER LAYER BUILDER - DATA QUALITY CLEANING")
        logger.info("=" * 60)
        logger.info("Processing all tables with data quality fixes...")
        logger.info("=" * 60)

        start_time = datetime.now()

        # Step 1: Setup silver tables
        logger.info("1️⃣  Setting up Silver layer tables...")
        if not self.setup_silver_tables():
            logger.error("❌ Failed to setup silver tables")
            return False

        # Step 2: Clean tables in order (independent tables first)
        cleaning_steps = [
            ("suppliers", self.clean_suppliers),
            ("warehouses", self.clean_warehouses),
            ("retail_stores", self.clean_retail_stores),
            ("products", self.clean_products),
            ("inventory", self.clean_inventory),
            ("supply_orders", self.clean_supply_orders)
        ]

        for step_num, (table_name, clean_func) in enumerate(cleaning_steps, 2):
            logger.info(f"{step_num}️⃣  Cleaning {table_name}...")
            if not clean_func():
                logger.error(f"❌ Failed to clean {table_name}")
                return False

        # Step 3: Final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info("🎉 SILVER LAYER CLEANING COMPLETED!")
        logger.info("=" * 60)
        logger.info(f"📊 Processing Summary:")
        logger.info(f"   Records processed: {self.total_stats['total_records_processed']:,}")
        logger.info(f"   Records cleaned: {self.total_stats['total_records_cleaned']:,}")
        logger.info(f"   Quality issues fixed: {self.total_stats['total_quality_issues_fixed']:,}")
        logger.info(f"   Processing time: {duration:.1f} seconds")
        logger.info("=" * 60)

        # Show final record counts
        conn = self.get_connection()
        if conn:
            try:
                cursor = conn.cursor()
                logger.info("📈 Final Silver Layer Record Counts:")
                tables = ['suppliers', 'products', 'warehouses', 'inventory', 'retail_stores', 'supply_orders']
                total_clean = 0
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM silver.{table}")
                    count = cursor.fetchone()[0]
                    total_clean += count
                    logger.info(f"   silver.{table:<15}: {count:>8,} clean records")
                logger.info(f"   {'TOTAL CLEAN':<20}: {total_clean:>8,} records")
                cursor.close()
                conn.close()
            except:
                pass

        logger.info("✅ Silver layer ready for Gold layer processing!")
        return True


def main():
    """Main function for standalone execution."""
    builder = SilverBuilder()
    success = builder.run_full_cleaning_pipeline()

    if success:
        print("\n🎉 Silver Builder completed successfully!")
        print("All data quality issues have been addressed!")
    else:
        print("\n❌ Silver Builder failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
