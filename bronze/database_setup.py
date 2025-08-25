import psycopg2
import sys
import logging
from pathlib import Path

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
        logging.FileHandler(log_dir / 'database_setup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def create_database():
    """Create the supply_chain database if it doesn't exist."""
    # Connect to default postgres database to create our database
    default_config = DB_CONFIG.copy()
    default_config['database'] = 'postgres'

    try:
        conn = psycopg2.connect(**default_config)
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'supply_chain'")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute("CREATE DATABASE supply_chain")
            logger.info("✓ Database 'supply_chain' created successfully")
        else:
            logger.info("✓ Database 'supply_chain' already exists")

        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error creating database: {e}")
        return False


def create_bronze_schema():
    """Create bronze schema and tables."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create bronze schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        logger.info("✓ Bronze schema created/verified")

        # Create suppliers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.suppliers (
                supplier_id INT PRIMARY KEY,
                supplier_name TEXT,
                contact_email TEXT,
                phone_number TEXT
            )
        """)
        logger.info("✓ Table 'bronze.suppliers' created/verified")

        # Create products table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.products (
                product_id INT PRIMARY KEY,
                product_name TEXT,
                unit_cost NUMERIC(15,6),
                selling_price NUMERIC(15,6),
                supplier_id INT,
                product_category TEXT,
                status TEXT
            )
        """)
        logger.info("✓ Table 'bronze.products' created/verified")

        # Create warehouses table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.warehouses (
                warehouse_id INT PRIMARY KEY,
                warehouse_name TEXT,
                city TEXT,
                region TEXT,
                storage_capacity INT
            )
        """)
        logger.info("✓ Table 'bronze.warehouses' created/verified")

        # Create inventory table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.inventory (
                inventory_id INT PRIMARY KEY,
                product_id INT,
                warehouse_id INT,
                quantity_on_hand INT,
                last_stocked_date DATE
            )
        """)
        logger.info("✓ Table 'bronze.inventory' created/verified")

        # Create retail_stores table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.retail_stores (
                retail_store_id INT PRIMARY KEY,
                store_name TEXT,
                city TEXT,
                region TEXT,
                store_type TEXT,
                store_status TEXT
            )
        """)
        logger.info("✓ Table 'bronze.retail_stores' created/verified")

        # Create supply_orders table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.supply_orders (
                supply_order_id INT PRIMARY KEY,
                product_id TEXT,
                warehouse_id TEXT,
                retail_store_id TEXT,
                quantity TEXT,
                price TEXT,
                total_invoice TEXT,
                order_date TEXT,
                shipped_date TEXT,
                delivered_date TEXT,
                status TEXT
            )
        """)
        logger.info("✓ Table 'bronze.supply_orders' created/verified")

        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_supplier_id ON bronze.products(supplier_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inventory_product_id ON bronze.inventory(product_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inventory_warehouse_id ON bronze.inventory(warehouse_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_supply_orders_product_id ON bronze.supply_orders(product_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_supply_orders_warehouse_id ON bronze.supply_orders(warehouse_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_supply_orders_retail_store_id ON bronze.supply_orders(retail_store_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_supply_orders_order_date ON bronze.supply_orders(order_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_supply_orders_status ON bronze.supply_orders(status)")
        logger.info("✓ Database indexes created/verified")

        conn.commit()
        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error creating bronze schema/tables: {e}")
        return False


def create_silver_gold_views():
    """Create views and tables for Silver and Gold layers."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create silver and gold schemas
        cursor.execute("CREATE SCHEMA IF NOT EXISTS silver")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        logger.info("✓ Silver and Gold schemas created/verified")

        # Silver layer - cleaned and validated data
        cursor.execute("""
            CREATE OR REPLACE VIEW silver.suppliers_clean AS
            SELECT
                supplier_id,
                TRIM(supplier_name) as supplier_name,
                LOWER(TRIM(contact_email)) as contact_email,
                TRIM(phone_number) as phone_number,
                created_at,
                updated_at
            FROM bronze.suppliers
            WHERE supplier_name IS NOT NULL
            AND contact_email IS NOT NULL
            AND phone_number IS NOT NULL
        """)

        cursor.execute("""
            CREATE OR REPLACE VIEW silver.products_clean AS
            SELECT
                p.product_id,
                TRIM(p.product_name) as product_name,
                p.unit_cost,
                p.selling_price,
                p.supplier_id,
                TRIM(p.product_category) as product_category,
                p.status,
                p.created_at,
                p.updated_at
            FROM bronze.products p
            WHERE p.unit_cost > 0 AND p.selling_price > 0
        """)

        # Gold layer - business metrics and aggregations
        cursor.execute("""
            CREATE OR REPLACE VIEW gold.inventory_summary AS
            SELECT
                w.warehouse_name,
                w.city,
                w.region,
                COUNT(DISTINCT i.product_id) as total_products,
                SUM(i.quantity_on_hand) as total_inventory,
                SUM(i.quantity_on_hand * COALESCE(p.unit_cost, 0)) as total_inventory_value,
                AVG(p.unit_cost) as avg_product_cost
            FROM bronze.inventory i
            JOIN bronze.warehouses w ON i.warehouse_id = w.warehouse_id
            LEFT JOIN bronze.products p ON i.product_id = p.product_id
            GROUP BY w.warehouse_id, w.warehouse_name, w.city, w.region
        """)

        cursor.execute("""
            CREATE OR REPLACE VIEW gold.supply_order_metrics AS
            SELECT
                so.order_date,
                so.status,
                COUNT(*) as order_count,
                SUM(so.quantity) as total_quantity,
                SUM(so.total_invoice) as total_revenue,
                AVG(so.total_invoice) as avg_order_value,
                COUNT(DISTINCT so.retail_store_id) as unique_stores
            FROM bronze.supply_orders so
            GROUP BY so.order_date, so.status
            ORDER BY so.order_date DESC
        """)

        cursor.execute("""
            CREATE OR REPLACE VIEW gold.retail_store_performance AS
            SELECT
                rs.store_name,
                rs.city,
                rs.region,
                rs.store_type,
                COUNT(so.supply_order_id) as total_orders,
                SUM(so.total_invoice) as total_revenue,
                AVG(so.total_invoice) as avg_order_value,
                SUM(so.quantity) as total_quantity_ordered
            FROM bronze.retail_stores rs
            LEFT JOIN bronze.supply_orders so ON rs.retail_store_id = so.retail_store_id
            WHERE rs.store_status = 'active'
            GROUP BY rs.retail_store_id, rs.store_name, rs.city, rs.region, rs.store_type
            ORDER BY total_revenue DESC NULLS LAST
        """)

        logger.info("✓ Silver and Gold layer views created")

        conn.commit()
        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error creating Silver/Gold views: {e}")
        return False


def test_connection():
    """Test the database connection and verify table structure."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Test basic connection
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        logger.info("✓ Successfully connected to PostgreSQL")

        # Test table access and show record counts
        tables = ['suppliers', 'products', 'warehouses', 'inventory', 'retail_stores', 'supply_orders']
        logger.info("\n📊 Bronze Layer Tables:")
        logger.info("-" * 40)

        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM bronze.{table}")
                count = cursor.fetchone()[0]
                logger.info(f"  ✓ {table:<15}: {count:>8,} records")
            except psycopg2.Error as e:
                logger.error(f"  ❌ Error accessing bronze.{table}: {e}")

        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Connection test failed: {e}")
        logger.info("\n🔧 Troubleshooting tips:")
        logger.info("1. Make sure PostgreSQL is running")
        logger.info("2. Check your database credentials in config.py")
        logger.info("3. Ensure the user has necessary permissions")
        return False


def drop_all_tables():
    """Drop all tables and schemas - USE WITH CAUTION!"""
    logger.warning("⚠️  WARNING: This will delete ALL data!")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Drop tables in reverse order to avoid foreign key conflicts
        tables = ['supply_orders', 'inventory', 'retail_stores', 'products', 'warehouses', 'suppliers']

        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS bronze.{table} CASCADE")
            logger.info(f"✓ Dropped table bronze.{table}")

        # Drop schemas
        schemas = ['gold', 'silver', 'bronze']
        for schema in schemas:
            cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            logger.info(f"✓ Dropped schema {schema}")

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("✅ All tables and schemas dropped successfully")
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error dropping tables: {e}")
        return False


def main():
    """Main setup function."""
    logger.info("🚀 Setting up Medallion Database - Bronze Layer")
    logger.info("=" * 60)

    logger.info("1. Creating database...")
    if not create_database():
        logger.error("❌ Failed to create database")
        sys.exit(1)

    logger.info("2. Creating bronze schema and tables...")
    if not create_bronze_schema():
        logger.error("❌ Failed to create bronze schema")
        sys.exit(1)

    logger.info("3. Creating silver and gold views...")
    if not create_silver_gold_views():
        logger.warning("⚠️  Failed to create views, but continuing...")

    logger.info("4. Testing connection...")
    if not test_connection():
        logger.error("❌ Connection test failed")
        sys.exit(1)

    logger.info("\n🎉 Bronze Layer Database Setup Completed Successfully!")
    logger.info("=" * 60)
    logger.info("Next step: Run bronze/data_loader.py to load data")
    logger.info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")


if __name__ == "__main__":
    main()
