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
        """Create the three required aggregate tables."""
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

            # Aggregate Table 3: Supplier Performance Monthly
            logger.info("Creating supplier_performance_monthly aggregate...")
            cursor.execute("DROP TABLE IF EXISTS gold.supplier_performance_monthly CASCADE")
            cursor.execute("""
                CREATE TABLE gold.supplier_performance_monthly AS
                WITH base AS (
                    SELECT
                        DATE_TRUNC('month', so.order_date) AS month,
                        s.supplier_id,
                        s.supplier_name,
                        COUNT(DISTINCT so.supply_order_id) AS total_orders,
                        SUM(so.quantity) AS total_units,
                        SUM(so.total_invoice) AS total_revenue,
                        AVG(EXTRACT(EPOCH FROM (so.delivery_date - so.order_date)) / 86400.0) AS avg_lead_time_days,
                        SUM(CASE WHEN so.status IN ('delivered', 'shipped') THEN 1 ELSE 0 END) AS fulfilled_orders,
                        SUM(CASE WHEN so.status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
                        SUM(CASE WHEN so.on_time = TRUE THEN 1 ELSE 0 END) AS on_time_orders,
                        SUM(CASE WHEN so.in_full = TRUE THEN 1 ELSE 0 END) AS in_full_orders
                    FROM silver.supply_orders so
                    JOIN silver.suppliers s ON so.supplier_id = s.supplier_id
                    GROUP BY month, s.supplier_id, s.supplier_name
                )
                SELECT
                    month,
                    supplier_id,
                    supplier_name,
                    total_orders,
                    total_units,
                    total_revenue,
                    ROUND(avg_lead_time_days::NUMERIC, 2) AS avg_lead_time_days,
                    fulfilled_orders,
                    delivered_orders,
                    ROUND((on_time_orders::NUMERIC / NULLIF(total_orders, 0)) * 100, 2) AS on_time_rate_pct,
                    ROUND((in_full_orders::NUMERIC / NULLIF(total_orders, 0)) * 100, 2) AS in_full_rate_pct,
                    ROUND(((on_time_orders::NUMERIC > 0)::INT + (in_full_orders::NUMERIC > 0)::INT) / 2.0 * 100, 2) AS otif_proxy_pct,
                    CURRENT_TIMESTAMP AS created_at
                FROM base
                ORDER BY month DESC, total_revenue DESC
            """)

            cursor.execute("SELECT COUNT(*) FROM gold.supplier_performance_monthly")
            count3 = cursor.fetchone()[0]
            logger.info(f"✅ supplier_performance_monthly created with {count3:,} rows")

            # Log metadata
            self._log_table_metadata(cursor, 'monthly_sales_performance',
                                   'Monthly sales performance by region, store type, and product category',
                                   ['silver.supply_orders', 'silver.retail_stores', 'silver.products'],
                                   'Monthly', count1)

            self._log_table_metadata(cursor, 'inventory_health_metrics',
                                   'Inventory health and capacity utilization metrics by warehouse and category',
                                   ['silver.inventory', 'silver.warehouses', 'silver.products'],
                                   'Daily', count2)

            self._log_table_metadata(cursor, 'supplier_performance_monthly',
                                   'Monthly supplier performance metrics including lead time and OTIF proxies',
                                   ['silver.supply_orders', 'silver.suppliers'],
                                   'Monthly', count3)

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

                        -- Natural keys for BI joins
                        so.product_id,
                        so.warehouse_id,
                        so.retail_store_id,

                        -- Product details (COALESCE to handle missing dims)
                        COALESCE(p.product_name, 'Unknown') as product_name,
                        COALESCE(p.product_category, 'Unknown') as product_category,
                        p.unit_cost,
                        p.selling_price,
                        (p.selling_price - p.unit_cost) as profit_margin,
                        ROUND(((p.selling_price - p.unit_cost) / NULLIF(p.unit_cost, 0) * 100)::NUMERIC, 2) as profit_margin_pct,

                        -- Supplier details
                        COALESCE(s.supplier_id, -1) as supplier_id,
                        COALESCE(s.supplier_name, 'Unknown') as supplier_name,
                        s.contact_email as supplier_email,

                        -- Warehouse details
                        COALESCE(w.warehouse_name, 'Unknown') as warehouse_name,
                        COALESCE(w.city, 'Unknown') as warehouse_city,
                        COALESCE(w.region, 'Unknown') as warehouse_region,
                        w.storage_capacity,

                        -- Retail store details
                        COALESCE(rs.store_name, 'Unknown') as store_name,
                        COALESCE(rs.city, 'Unknown') as store_city,
                        COALESCE(rs.region, 'Unknown') as store_region,
                        COALESCE(rs.store_type, 'Unknown') as store_type,
                        COALESCE(rs.store_status, 'unknown') as store_status,

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
                        EXTRACT(ISODOW FROM so.order_date) as order_isodow,
                        CAST(TO_CHAR(so.order_date, 'YYYYMMDD') AS INTEGER) as order_date_key,

                        -- Business metrics (typed)
                        ROUND((so.quantity * p.unit_cost)::NUMERIC, 2) as total_cost,
                        ROUND((so.total_invoice - (so.quantity * p.unit_cost))::NUMERIC, 2) as total_profit,

                        -- Status flags
                        (so.status = 'shipped')::BOOLEAN as is_shipped,
                        (so.status = 'delivered')::BOOLEAN as is_delivered,
                        (so.status = 'canceled')::BOOLEAN as is_canceled,

                        -- DQ flags based on LEFT JOIN nulls and value checks
                        (p.product_id IS NULL OR s.supplier_id IS NULL OR w.warehouse_id IS NULL OR rs.retail_store_id IS NULL) as dq_missing_dim,
                        (so.quantity < 0 OR so.price < 0 OR so.total_invoice < 0) as dq_negative_amount,
                        ((so.shipped_date IS NOT NULL AND so.shipped_date < so.order_date) OR (so.delivered_date IS NOT NULL AND so.delivered_date < so.order_date)) as dq_invalid_dates,

                        -- Order age and backlog helpers
                        (CURRENT_DATE - so.order_date) as order_age_days

                    FROM silver.supply_orders so
                    LEFT JOIN silver.products p ON so.product_id = p.product_id
                    LEFT JOIN silver.suppliers s ON p.supplier_id = s.supplier_id
                    LEFT JOIN silver.warehouses w ON so.warehouse_id = w.warehouse_id
                    LEFT JOIN silver.retail_stores rs ON so.retail_store_id = rs.retail_store_id
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

                        -- Backlog flag: undelivered orders older than 7 days
                        (order_status <> 'delivered' AND order_age_days > 7) as is_backlog,

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

            # Helpful indexes for BI slicing
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_gscd_order_date ON gold.supply_chain_dashboard(order_date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_gscd_order_year_month ON gold.supply_chain_dashboard(order_year_month)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_gscd_product_category ON gold.supply_chain_dashboard(product_category)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_gscd_supplier_name ON gold.supply_chain_dashboard(supplier_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_gscd_warehouse_region ON gold.supply_chain_dashboard(warehouse_region)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_gscd_store_region ON gold.supply_chain_dashboard(store_region)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_gscd_order_status ON gold.supply_chain_dashboard(order_status)")

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
        """Skip creating extra business views to keep only 3 aggregates + 1 dashboard."""
        logger.info("ℹ️ Skipping creation of business views as per requirements (3 aggregates + 1 dashboard only)")
        return True

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
            'supplier_performance_monthly': [
                ('non_negative_revenue', "SELECT COUNT(*) FROM gold.supplier_performance_monthly WHERE total_revenue < 0"),
                ('valid_rates', "SELECT COUNT(*) FROM gold.supplier_performance_monthly WHERE on_time_rate_pct < 0 OR on_time_rate_pct > 100 OR in_full_rate_pct < 0 OR in_full_rate_pct > 100"),
                ('lead_time_not_negative', "SELECT COUNT(*) FROM gold.supplier_performance_monthly WHERE avg_lead_time_days < 0")
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

            # Get counts for all gold tables (3 aggregates + 1 dashboard)
            gold_tables = ['monthly_sales_performance', 'inventory_health_metrics', 'supplier_performance_monthly', 'supply_chain_dashboard']
            total_records = 0

            for table in gold_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM gold.{table}")
                    count = cursor.fetchone()[0]
                    total_records += count
                    logger.info(f"  {table:<25}: {count:>8,} records")
                except:
                    logger.info(f"  {table:<25}: Not created")

            # No extra views are created by design
            logger.info("\n📈 Business Views: Skipped by design")

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
