"""
Gold Layer Builder for Medallion Data Pipeline
Handles business analytics, aggregations, and BI-ready data marts
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


class GoldBuilder:
    """Handles Gold layer business analytics and aggregations."""

    def __init__(self):
        self.run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.stats = {}

    def get_connection(self):
        """Get database connection."""
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            return None

    def setup_gold_schema(self):
        """Create Gold schema and audit tables."""
        logger.info("Setting up Gold schema...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # Create gold schema
            cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")

            # Create gold metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gold.table_metadata (
                    table_name VARCHAR(100) PRIMARY KEY,
                    description TEXT,
                    source_tables TEXT[],
                    refresh_frequency VARCHAR(50),
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    row_count BIGINT,
                    created_by VARCHAR(100) DEFAULT 'gold_builder'
                )
            """)

            conn.commit()
            logger.info("✅ Gold schema created successfully")
            return True

        except psycopg2.Error as e:
            logger.error(f"Error setting up Gold schema: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def create_aggregate_tables(self):
        """Create the two required aggregate tables."""
        logger.info("🔄 Creating Gold aggregate tables...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # Aggregate Table 1: Monthly Sales Performance
            logger.info("Creating monthly_sales_performance aggregate...")
            cursor.execute("DROP TABLE IF EXISTS gold.monthly_sales_performance CASCADE")
            cursor.execute("""
                CREATE TABLE gold.monthly_sales_performance AS
                WITH monthly_orders AS (
                    SELECT
                        DATE_TRUNC('month', so.order_date) as sales_month,
                        rs.region,
                        rs.store_type,
                        p.product_category,
                        COUNT(DISTINCT so.supply_order_id) as total_orders,
                        SUM(so.quantity) as total_quantity_sold,
                        SUM(so.total_invoice) as total_revenue,
                        AVG(so.total_invoice) as avg_order_value,
                        COUNT(DISTINCT so.retail_store_id) as active_stores,
                        COUNT(DISTINCT so.product_id) as unique_products
                    FROM silver.supply_orders so
                    JOIN silver.retail_stores rs ON so.retail_store_id = rs.retail_store_id
                    JOIN silver.products p ON so.product_id = p.product_id
                    WHERE so.status IN ('delivered', 'shipped')
                    GROUP BY sales_month, rs.region, rs.store_type, p.product_category
                )
                SELECT
                    sales_month,
                    region,
                    store_type,
                    product_category,
                    total_orders,
                    total_quantity_sold,
                    total_revenue,
                    avg_order_value,
                    active_stores,
                    unique_products,
                    ROUND((total_revenue / NULLIF(total_quantity_sold, 0))::NUMERIC, 2) as avg_revenue_per_unit,
                    ROUND((total_revenue / NULLIF(active_stores, 0))::NUMERIC, 2) as revenue_per_store,
                    CURRENT_TIMESTAMP as created_at
                FROM monthly_orders
                ORDER BY sales_month DESC, total_revenue DESC
            """)

            cursor.execute("SELECT COUNT(*) FROM gold.monthly_sales_performance")
            count1 = cursor.fetchone()[0]
            logger.info(f"✅ monthly_sales_performance created with {count1:,} rows")

            # Aggregate Table 2: Inventory Health Metrics
            logger.info("Creating inventory_health_metrics aggregate...")
            cursor.execute("DROP TABLE IF EXISTS gold.inventory_health_metrics CASCADE")
            cursor.execute("""
                CREATE TABLE gold.inventory_health_metrics AS
                WITH inventory_analysis AS (
                    SELECT
                        w.region,
                        w.city,
                        w.warehouse_name,
                        p.product_category,
                        COUNT(DISTINCT i.product_id) as product_count,
                        SUM(i.quantity_on_hand) as total_stock_quantity,
                        SUM(i.quantity_on_hand * p.unit_cost) as total_stock_value,
                        AVG(i.quantity_on_hand) as avg_stock_per_product,
                        MIN(i.quantity_on_hand) as min_stock_level,
                        MAX(i.quantity_on_hand) as max_stock_level,
                        COUNT(CASE WHEN i.quantity_on_hand = 0 THEN 1 END) as out_of_stock_items,
                        COUNT(CASE WHEN i.quantity_on_hand < 100 THEN 1 END) as low_stock_items,
                        w.storage_capacity,
                        ROUND((SUM(i.quantity_on_hand)::NUMERIC / w.storage_capacity * 100), 2) as capacity_utilization_pct
                    FROM silver.inventory i
                    JOIN silver.warehouses w ON i.warehouse_id = w.warehouse_id
                    JOIN silver.products p ON i.product_id = p.product_id
                    GROUP BY w.warehouse_id, w.warehouse_name, w.region, w.city, p.product_category, w.storage_capacity
                )
                SELECT
                    region,
                    city,
                    warehouse_name,
                    product_category,
                    product_count,
                    total_stock_quantity,
                    total_stock_value,
                    avg_stock_per_product,
                    min_stock_level,
                    max_stock_level,
                    out_of_stock_items,
                    low_stock_items,
                    storage_capacity,
                    capacity_utilization_pct,
                    CASE
                        WHEN capacity_utilization_pct >= 90 THEN 'Over Capacity'
                        WHEN capacity_utilization_pct >= 75 THEN 'High Utilization'
                        WHEN capacity_utilization_pct >= 50 THEN 'Optimal'
                        WHEN capacity_utilization_pct >= 25 THEN 'Under Utilized'
                        ELSE 'Very Low Utilization'
                    END as utilization_status,
                    CASE
                        WHEN out_of_stock_items > 5 THEN 'Critical'
                        WHEN low_stock_items > 10 THEN 'Warning'
                        ELSE 'Healthy'
                    END as stock_health_status,
                    CURRENT_TIMESTAMP as created_at
                FROM inventory_analysis
                ORDER BY total_stock_value DESC, capacity_utilization_pct DESC
            """)

            cursor.execute("SELECT COUNT(*) FROM gold.inventory_health_metrics")
            count2 = cursor.fetchone()[0]
            logger.info(f"✅ inventory_health_metrics created with {count2:,} rows")

            # Log metadata
            self._log_table_metadata(cursor, 'monthly_sales_performance',
                                   'Monthly sales performance by region, store type, and product category',
                                   ['silver.supply_orders', 'silver.retail_stores', 'silver.products'],
                                   'Monthly', count1)

            self._log_table_metadata(cursor, 'inventory_health_metrics',
                                   'Inventory health and capacity utilization metrics by warehouse and category',
                                   ['silver.inventory', 'silver.warehouses', 'silver.products'],
                                   'Daily', count2)

            conn.commit()
            logger.info("✅ Aggregate tables created successfully")
            return True

        except psycopg2.Error as e:
            logger.error(f"Error creating aggregate tables: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def create_dashboard_table(self):
        """Create the wide dashboard table for BI tools."""
        logger.info("🔄 Creating dashboard table...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            logger.info("Creating supply_chain_dashboard wide table...")
            cursor.execute("DROP TABLE IF EXISTS gold.supply_chain_dashboard CASCADE")
            cursor.execute("""
                CREATE TABLE gold.supply_chain_dashboard AS
                WITH order_details AS (
                    SELECT
                        so.supply_order_id,
                        so.order_date,
                        so.shipped_date,
                        so.delivered_date,
                        so.status as order_status,
                        so.quantity,
                        so.price,
                        so.total_invoice,

                        -- Product details
                        p.product_name,
                        p.product_category,
                        p.unit_cost,
                        p.selling_price,
                        p.selling_price - p.unit_cost as profit_margin,
                        ROUND(((p.selling_price - p.unit_cost) / p.unit_cost * 100)::NUMERIC, 2) as profit_margin_pct,

                        -- Supplier details
                        s.supplier_name,
                        s.contact_email as supplier_email,

                        -- Warehouse details
                        w.warehouse_name,
                        w.city as warehouse_city,
                        w.region as warehouse_region,
                        w.storage_capacity,

                        -- Retail store details
                        rs.store_name,
                        rs.city as store_city,
                        rs.region as store_region,
                        rs.store_type,
                        rs.store_status,

                        -- Calculated fields
                        CASE
                            WHEN so.shipped_date IS NOT NULL AND so.delivered_date IS NOT NULL
                            THEN so.delivered_date - so.shipped_date
                            ELSE NULL
                        END as delivery_days,

                        CASE
                            WHEN so.shipped_date IS NOT NULL
                            THEN so.shipped_date - so.order_date
                            ELSE NULL
                        END as processing_days,

                        EXTRACT(YEAR FROM so.order_date) as order_year,
                        EXTRACT(MONTH FROM so.order_date) as order_month,
                        EXTRACT(QUARTER FROM so.order_date) as order_quarter,
                        TO_CHAR(so.order_date, 'YYYY-MM') as order_year_month,
                        TO_CHAR(so.order_date, 'Day') as order_day_of_week,

                        -- Business metrics
                        so.quantity * p.unit_cost as total_cost,
                        so.total_invoice - (so.quantity * p.unit_cost) as total_profit

                    FROM silver.supply_orders so
                    JOIN silver.products p ON so.product_id = p.product_id
                    JOIN silver.suppliers s ON p.supplier_id = s.supplier_id
                    JOIN silver.warehouses w ON so.warehouse_id = w.warehouse_id
                    JOIN silver.retail_stores rs ON so.retail_store_id = rs.retail_store_id
                ),
                enhanced_details AS (
                    SELECT
                        *,
                        -- Performance indicators
                        CASE
                            WHEN order_status = 'delivered' AND delivery_days <= 3 THEN 'Excellent'
                            WHEN order_status = 'delivered' AND delivery_days <= 7 THEN 'Good'
                            WHEN order_status = 'delivered' AND delivery_days <= 14 THEN 'Average'
                            WHEN order_status = 'delivered' THEN 'Poor'
                            WHEN order_status = 'shipped' THEN 'In Transit'
                            WHEN order_status = 'pending' THEN 'Processing'
                            ELSE 'Canceled'
                        END as delivery_performance,

                        CASE
                            WHEN total_invoice >= 100000 THEN 'High Value'
                            WHEN total_invoice >= 50000 THEN 'Medium Value'
                            WHEN total_invoice >= 10000 THEN 'Standard Value'
                            ELSE 'Low Value'
                        END as order_value_category,

                        CASE
                            WHEN profit_margin_pct >= 50 THEN 'High Margin'
                            WHEN profit_margin_pct >= 25 THEN 'Medium Margin'
                            WHEN profit_margin_pct >= 10 THEN 'Low Margin'
                            ELSE 'Very Low Margin'
                        END as profit_category,

                        -- Regional performance
                        CASE
                            WHEN warehouse_region = store_region THEN 'Same Region'
                            ELSE 'Cross Region'
                        END as distribution_type,

                        CURRENT_TIMESTAMP as created_at,
                        CURRENT_DATE as snapshot_date

                    FROM order_details
                )
                SELECT * FROM enhanced_details
                ORDER BY order_date DESC, total_invoice DESC
            """)

            cursor.execute("SELECT COUNT(*) FROM gold.supply_chain_dashboard")
            count = cursor.fetchone()[0]
            logger.info(f"✅ supply_chain_dashboard created with {count:,} rows")

            # Log metadata
            self._log_table_metadata(cursor, 'supply_chain_dashboard',
                                   'Comprehensive dashboard table with all supply chain metrics and KPIs',
                                   ['silver.supply_orders', 'silver.products', 'silver.suppliers',
                                    'silver.warehouses', 'silver.retail_stores'],
                                   'Daily', count)

            conn.commit()
            logger.info("✅ Dashboard table created successfully")
            return True

        except psycopg2.Error as e:
            logger.error(f"Error creating dashboard table: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def _log_table_metadata(self, cursor, table_name, description, source_tables, frequency, row_count):
        """Log metadata for created tables."""
        cursor.execute("""
            INSERT INTO gold.table_metadata
            (table_name, description, source_tables, refresh_frequency, row_count)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE SET
                description = EXCLUDED.description,
                source_tables = EXCLUDED.source_tables,
                refresh_frequency = EXCLUDED.refresh_frequency,
                row_count = EXCLUDED.row_count,
                last_updated = CURRENT_TIMESTAMP
        """, (table_name, description, source_tables, frequency, row_count))

    def create_business_views(self):
        """Create additional business views for analytics."""
        logger.info("🔄 Creating business views...")

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()

            # Top Performing Products View
            cursor.execute("""
                CREATE OR REPLACE VIEW gold.top_performing_products AS
                SELECT
                    p.product_name,
                    p.product_category,
                    s.supplier_name,
                    COUNT(DISTINCT so.supply_order_id) as total_orders,
                    SUM(so.quantity) as total_quantity_sold,
                    SUM(so.total_invoice) as total_revenue,
                    AVG(so.total_invoice) as avg_order_value,
                    SUM(so.total_invoice - (so.quantity * p.unit_cost)) as total_profit,
                    ROUND(AVG((p.selling_price - p.unit_cost) / p.unit_cost * 100)::NUMERIC, 2) as avg_profit_margin_pct,
                    COUNT(DISTINCT so.retail_store_id) as store_reach
                FROM silver.supply_orders so
                JOIN silver.products p ON so.product_id = p.product_id
                JOIN silver.suppliers s ON p.supplier_id = s.supplier_id
                WHERE so.status IN ('delivered', 'shipped')
                GROUP BY p.product_id, p.product_name, p.product_category, s.supplier_name
                ORDER BY total_revenue DESC
            """)

            # Regional Performance View
            cursor.execute("""
                CREATE OR REPLACE VIEW gold.regional_performance AS
                SELECT
                    rs.region,
                    COUNT(DISTINCT rs.retail_store_id) as total_stores,
                    COUNT(DISTINCT so.supply_order_id) as total_orders,
                    SUM(so.total_invoice) as total_revenue,
                    AVG(so.total_invoice) as avg_order_value,
                    COUNT(DISTINCT so.product_id) as unique_products_sold,
                    SUM(CASE WHEN so.status = 'delivered' THEN 1 ELSE 0 END) as delivered_orders,
                    ROUND((SUM(CASE WHEN so.status = 'delivered' THEN 1 ELSE 0 END)::NUMERIC /
                           NULLIF(COUNT(so.supply_order_id), 0) * 100), 2) as delivery_success_rate,
                    ROUND((SUM(so.total_invoice) / NULLIF(COUNT(DISTINCT rs.retail_store_id), 0))::NUMERIC, 2) as revenue_per_store
                FROM silver.retail_stores rs
                LEFT JOIN silver.supply_orders so ON rs.retail_store_id = so.retail_store_id
                WHERE rs.store_status = 'active'
                GROUP BY rs.region
                ORDER BY total_revenue DESC
            """)

            # Supplier Performance View
            cursor.execute("""
                CREATE OR REPLACE VIEW gold.supplier_performance AS
                SELECT
                    s.supplier_name,
                    s.contact_email,
                    COUNT(DISTINCT p.product_id) as products_supplied,
                    COUNT(DISTINCT so.supply_order_id) as total_orders,
                    SUM(so.quantity) as total_units_sold,
                    SUM(so.total_invoice) as total_revenue_generated,
                    AVG(p.selling_price - p.unit_cost) as avg_profit_per_unit,
                    COUNT(DISTINCT so.retail_store_id) as store_coverage,
                    SUM(CASE WHEN so.status = 'delivered' THEN 1 ELSE 0 END) as successful_deliveries,
                    ROUND((SUM(CASE WHEN so.status = 'delivered' THEN 1 ELSE 0 END)::NUMERIC /
                           NULLIF(COUNT(so.supply_order_id), 0) * 100), 2) as delivery_success_rate
                FROM silver.suppliers s
                JOIN silver.products p ON s.supplier_id = p.supplier_id
                LEFT JOIN silver.supply_orders so ON p.product_id = so.product_id
                GROUP BY s.supplier_id, s.supplier_name, s.contact_email
                ORDER BY total_revenue_generated DESC
            """)

            logger.info("✅ Business views created successfully")
            return True

        except psycopg2.Error as e:
            logger.error(f"Error creating business views: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def run_gold_quality_checks(self):
        """Run quality checks on Gold layer tables."""
        logger.info("🔍 Running Gold layer quality checks...")

        checks = {
            'monthly_sales_performance': [
                ('no_negative_revenue', "SELECT COUNT(*) FROM gold.monthly_sales_performance WHERE total_revenue < 0"),
                ('valid_dates', "SELECT COUNT(*) FROM gold.monthly_sales_performance WHERE sales_month > CURRENT_DATE"),
                ('consistent_calculations', """SELECT COUNT(*) FROM gold.monthly_sales_performance
                                              WHERE ABS(avg_revenue_per_unit * total_quantity_sold - total_revenue) > 1""")
            ],
            'inventory_health_metrics': [
                ('capacity_not_exceeded', "SELECT COUNT(*) FROM gold.inventory_health_metrics WHERE capacity_utilization_pct > 100"),
                ('non_negative_stock', "SELECT COUNT(*) FROM gold.inventory_health_metrics WHERE total_stock_quantity < 0"),
                ('valid_warehouse_data', "SELECT COUNT(*) FROM gold.inventory_health_metrics WHERE warehouse_name IS NULL")
            ],
            'supply_chain_dashboard': [
                ('valid_profit_margins', "SELECT COUNT(*) FROM gold.supply_chain_dashboard WHERE profit_margin IS NULL AND order_status = 'delivered'"),
                ('logical_dates', "SELECT COUNT(*) FROM gold.supply_chain_dashboard WHERE delivered_date < shipped_date"),
                ('positive_quantities', "SELECT COUNT(*) FROM gold.supply_chain_dashboard WHERE quantity <= 0")
            ]
        }

        conn = self.get_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()
            all_passed = True

            for table_name, table_checks in checks.items():
                logger.info(f"Running quality checks for {table_name}...")

                for check_name, sql_query in table_checks:
                    cursor.execute(sql_query)
                    bad_count = cursor.fetchone()[0]
                    passed = bad_count == 0

                    if not passed:
                        all_passed = False
                        logger.warning(f"❌ {table_name}.{check_name}: {bad_count} issues found")
                    else:
                        logger.info(f"✅ {table_name}.{check_name}: PASSED")

            if all_passed:
                logger.info("✅ All Gold layer quality checks passed")
            else:
                logger.warning("⚠️  Some Gold layer quality checks failed")

            return True

        except psycopg2.Error as e:
            logger.error(f"Error running quality checks: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def log_summary(self):
        """Log summary of Gold layer processing."""
        logger.info("=" * 60)
        logger.info("📊 GOLD LAYER PROCESSING SUMMARY")
        logger.info("=" * 60)

        conn = self.get_connection()
        if not conn:
            return

        try:
            cursor = conn.cursor()

            # Get counts for all gold tables
            gold_tables = ['monthly_sales_performance', 'inventory_health_metrics', 'supply_chain_dashboard']
            total_records = 0

            for table in gold_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM gold.{table}")
                    count = cursor.fetchone()[0]
                    total_records += count
                    logger.info(f"  {table:<25}: {count:>8,} records")
                except:
                    logger.info(f"  {table:<25}: Not created")

            # Get view counts
            views = ['top_performing_products', 'regional_performance', 'supplier_performance']
            logger.info("\n📈 Business Views:")
            for view in views:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM gold.{view}")
                    count = cursor.fetchone()[0]
                    logger.info(f"  {view:<25}: {count:>8,} records")
                except:
                    logger.info(f"  {view:<25}: Not created")

            logger.info("-" * 60)
            logger.info(f"  {'TOTAL GOLD RECORDS':<25}: {total_records:>8,}")
            logger.info(f"  Run ID: {self.run_id}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Error in summary: {e}")
        finally:
            cursor.close()
            conn.close()

    def run_full_gold_pipeline(self):
        """Run complete Gold layer pipeline."""
        logger.info("🥇 GOLD LAYER BUILDER - FULL PIPELINE")
        logger.info("=" * 60)

        # Step 1: Setup schema
        if not self.setup_gold_schema():
            logger.error("❌ Gold schema setup failed")
            return False

        # Step 2: Create aggregate tables
        if not self.create_aggregate_tables():
            logger.error("❌ Aggregate tables creation failed")
            return False

        # Step 3: Create dashboard table
        if not self.create_dashboard_table():
            logger.error("❌ Dashboard table creation failed")
            return False

        # Step 4: Create business views
        if not self.create_business_views():
            logger.warning("⚠️  Business views creation had issues, but continuing...")

        # Step 5: Run quality checks
        if not self.run_gold_quality_checks():
            logger.warning("⚠️  Some quality checks failed, but continuing...")

        # Step 6: Log summary
        self.log_summary()

        logger.info("✅ Gold layer pipeline completed successfully!")
        return True


def main():
    """Main function for standalone execution."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    gb = GoldBuilder()
    success = gb.run_full_gold_pipeline()

    if success:
        print("✅ Gold Builder completed successfully!")
    else:
        print("❌ Gold Builder failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
