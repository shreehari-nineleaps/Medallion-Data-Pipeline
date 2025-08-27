"""
Streamlit UI for Medallion Data Pipeline
A comprehensive interface for managing and monitoring the supply chain data pipeline
"""

import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
import os
import subprocess
import logging
from pathlib import Path
import json
import random

# Add project root to path
sys.path.append(str(Path(__file__).parent))

# Configuration (built-in)
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'supply_chain'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password123'),
    'port': int(os.getenv('DB_PORT', '5432'))
}

# Page configuration
st.set_page_config(
    page_title="Medallion Data Pipeline",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS
st.markdown("""
<style>
    /* Formal, clean styling */
    :root {
        --brand-primary: #0f4c81; /* corporate blue */
        --brand-secondary: #2f4858; /* slate */
        --brand-accent: #0ea5e9; /* accent blue */
    }
    .main-header {
        background: linear-gradient(90deg, var(--brand-primary) 0%, var(--brand-secondary) 100%);
        padding: 1rem 1.25rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        color: #ffffff;
        text-align: left;
    }
    .main-header h1 {
        font-size: 1.4rem;
        margin: 0;
        font-weight: 600;
        letter-spacing: 0.3px;
    }
    .metric-card {
        background: #ffffff;
        padding: 0.9rem 1rem;
        border-radius: 8px;
        border: 1px solid #e6e8eb;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
        margin: 0.5rem 0;
    }
    .stButton > button {
        width: 100%;
        border: 1px solid #e6e8eb;
        background-color: #f8fafc;
        color: #111827;
    }
    .stButton > button:hover {
        border-color: var(--brand-accent);
    }
    .success-status { color: #198754; }
    .error-status { color: #dc3545; }
    .warning-status { color: #fd7e14; }
    .small-muted { color: #6b7280; font-size: 0.85rem; }
    /* Hide Streamlit default header/menu/footer so navbar is visible */
    [data-testid="stHeader"] { height: 0px; visibility: hidden; }
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# Compact table theme for dataframes
st.markdown("""
<style>
  /* Compact tables for Database Explorer and Analytics */
  [data-testid="stDataFrame"] table {
    font-size: 12px;
  }
  [data-testid="stDataFrame"] th, [data-testid="stDataFrame"] td {
    padding: 4px 6px !important;
    line-height: 1.2 !important;
  }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'pipeline_status' not in st.session_state:
    st.session_state.pipeline_status = {
        'bronze': 'Not Run',
        'silver': 'Not Run',
        'gold': 'Not Run'
    }



# Database connection helper
@st.cache_resource
def get_database_connection():
    """Get database connection with caching"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

def execute_query(query, fetch_all=True):
    """Execute SQL query and return results"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(query)

        if query.strip().upper().startswith('SELECT'):
            if fetch_all:
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(results, columns=columns)
                cursor.close()
                conn.close()
                return df
            else:
                results = cursor.fetchall()
                cursor.close()
                conn.close()
                return results
        else:
            conn.commit()
            cursor.close()
            conn.close()
            return True
    except Exception as e:
        st.error(f"Query execution failed: {e}")
        return None



def run_pipeline_stage(stage):
    """Run specific pipeline stage"""
    try:
        if stage == 'setup':
            # Database setup
            try:
                from bronze.database_setup import create_database, create_bronze_schema, create_silver_gold_views
                success1 = create_database()
                success2 = create_bronze_schema()
                success3 = create_silver_gold_views()
                success = success1 and success2 and success3
            except Exception as e:
                st.error(f"Database setup error: {e}")
                success = False
        elif stage == 'bronze':
            from etl import build_bronze
            success = build_bronze()
        elif stage == 'silver':
            from etl import build_silver
            success = build_silver()
        elif stage == 'gold':
            from etl import build_gold
            success = build_gold()
        elif stage == 'full':
            from etl import run_full_pipeline
            success = run_full_pipeline()
        else:
            success = False

        if success:
            st.session_state.pipeline_status[stage] = 'Success'
            return True
        else:
            st.session_state.pipeline_status[stage] = 'Failed'
            return False
    except Exception as e:
        st.session_state.pipeline_status[stage] = f'Error: {str(e)}'
        return False

# --- Top Navigation Bar Setup ---
pages = {
    "Dashboard Home": "home",
    "Pipeline Control": "pipeline",
    "Database Explorer": "database",
    "Data Analytics": "analytics",
    "Forecasting": "forecasting",
    "Query Runner": "query",
    "BI Dashboard": "dashboard"
}
nav_pages = list(pages.keys())
if 'current_page' not in st.session_state:
    st.session_state.current_page = nav_pages[0]
# --- Top Navbar HTML/CSS/JS ---
nav_html = """
<div class=\"top-navbar\">
  <div class=\"nav-logo\">Medallion Data Pipeline</div>
</div>
<div class=\"nav-links-bar\"></div>
<style>
.top-navbar {
  width: 100%;
  position: fixed;
  top: 0; left: 0; right: 0;
  background: linear-gradient(90deg, #1a237e 0%, #3949ab 100%);
  color: #fff;
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 56px;
  z-index: 10000;
  box-shadow: 0 2px 8px rgba(30,40,80,0.1);
}
.nav-logo {
  font-weight: 700;
  font-size: 1.25rem;
  margin-left: 2rem;
  letter-spacing: 0.04em;
}
.nav-links-bar { height: 0; }
.stApp {
  padding-top: 64px !important;
}
</style>
"""
st.markdown(nav_html, unsafe_allow_html=True)

# Streamlit-based navbar buttons (no page reload)
btn_cols = st.columns(len(nav_pages))
for i, page in enumerate(nav_pages):
    with btn_cols[i]:
        is_active = (st.session_state.current_page == page)
        if st.button(page, key=f"navbtn_{i}", type=("primary" if is_active else "secondary")):
            st.session_state.current_page = page
            st.experimental_set_query_params(page=page)
            st.experimental_rerun()

# --- Assign current_page variable for routing ---
import urllib.parse
params = st.experimental_get_query_params()
page_value = params.get('page', [st.session_state.current_page])[0]
if page_value in nav_pages:
    st.session_state.current_page = page_value
current_page = pages[st.session_state.current_page]
# Main header
st.markdown('<div class="main-header"><h1>Medallion Data Pipeline — Control Center</h1></div>', unsafe_allow_html=True)

# HOME PAGE
if current_page == "home":
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Pipeline Status", "Active", "Running")

    with col2:
        st.metric("Total Tables", "15", "3 layers")

    with col3:
        st.metric("Last Run", "12:34", "2 hours ago")

    with col4:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            status = "Connected"
        except:
            status = "Disconnected"
        st.metric("Database", status)

        if st.button("Test Connection", use_container_width=True):
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                conn.close()
                st.success("Database connection successful.")
            except Exception as e:
                st.error(f"Connection failed: {e}")


    # Pipeline Status Overview
    st.subheader("Pipeline Layers Status")
    status_cols = st.columns(3)

    with status_cols[0]:
        bronze_status = st.session_state.pipeline_status.get('bronze', 'Not Run')
        st.markdown(f"### Bronze Layer")
        st.write(f"Status: {bronze_status}")

    with status_cols[1]:
        silver_status = st.session_state.pipeline_status.get('silver', 'Not Run')
        st.markdown(f"### Silver Layer")
        st.write(f"Status: {silver_status}")

    with status_cols[2]:
        gold_status = st.session_state.pipeline_status.get('gold', 'Not Run')
        st.markdown(f"### Gold Layer")
        st.write(f"Status: {gold_status}")

    # Quick Actions
        st.subheader("Quick Actions")
    action_cols = st.columns(4)

    with action_cols[0]:
        if st.button("Setup Database", use_container_width=True):
            with st.spinner("Setting up database schemas..."):
                success = run_pipeline_stage('setup')
                if success:
                    st.success("Database schemas created successfully.")
                else:
                    st.error("Database setup failed.")

    with action_cols[1]:
        if st.button("Run Bronze", use_container_width=True):
            with st.spinner("Running Bronze layer..."):
                success = run_pipeline_stage('bronze')
                if success:
                    st.success("Bronze layer completed!")
                else:
                    st.error("Bronze layer failed!")

    with action_cols[2]:
        if st.button("Run Silver", use_container_width=True):
            with st.spinner("Running Silver layer..."):
                success = run_pipeline_stage('silver')
                if success:
                    st.success("Silver layer completed!")
                else:
                    st.error("Silver layer failed!")

    with action_cols[3]:
        if st.button("Run Gold", use_container_width=True):
            with st.spinner("Running Gold layer..."):
                success = run_pipeline_stage('gold')
                if success:
                    st.success("Gold layer completed!")
                else:
                    st.error("Gold layer failed!")

# PIPELINE CONTROL PAGE
elif current_page == "pipeline":
    st.header("Pipeline Control Center")

    tab1, tab2 = st.tabs(["Manual Control", "Status"])

    with tab1:
        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("Pipeline Execution")

            # Full pipeline run
            st.markdown("### Full Pipeline")
            if st.button("Run Complete ETL Pipeline", type="primary", use_container_width=True):
                with st.spinner("Running full pipeline..."):
                    progress_bar = st.progress(0)

                    # Setup
                    progress_bar.progress(20)
                    setup_success = run_pipeline_stage('setup')
                    st.session_state.demo_data_loaded = setup_success

                    # Bronze
                    progress_bar.progress(40)
                    bronze_success = run_pipeline_stage('bronze')

                    # Silver
                    progress_bar.progress(70)
                    silver_success = run_pipeline_stage('silver')

                    # Gold
                    progress_bar.progress(90)
                    gold_success = run_pipeline_stage('gold')

                    progress_bar.progress(100)

                    if all([setup_success, bronze_success, silver_success, gold_success]):
                        st.success("Full pipeline completed successfully.")
                    else:
                        st.error("Pipeline completed with errors.")


            # Individual stages
            st.markdown("### Individual Stages")
            stage_cols = st.columns(4)

            with stage_cols[0]:
                if st.button("Database Setup", use_container_width=True):
                    with st.spinner("Setting up database schemas..."):
                        success = run_pipeline_stage('setup')
                        if success:
                            st.success("✅ Database setup completed!")
                        else:
                            st.error("❌ Database setup failed!")

            with stage_cols[1]:
                if st.button("Bronze Layer", use_container_width=True):
                    with st.spinner("Building Bronze layer..."):
                        success = run_pipeline_stage('bronze')
                        if success:
                            st.success("Bronze completed!")
                        else:
                            st.error("Bronze failed!")

            with stage_cols[2]:
                if st.button("Silver Layer", use_container_width=True):
                    with st.spinner("Building Silver layer..."):
                        success = run_pipeline_stage('silver')
                        if success:
                            st.success("Silver completed!")
                        else:
                            st.error("Silver failed!")

            with stage_cols[3]:
                if st.button("Gold Layer", use_container_width=True):
                    with st.spinner("Building Gold layer..."):
                        success = run_pipeline_stage('gold')
                        if success:
                            st.success("Gold completed!")
                        else:
                            st.error("Gold failed!")

        with col2:
            st.subheader("Current Status")
            for layer, status in st.session_state.pipeline_status.items():
                if status == 'Success':
                    st.success(f"{layer.title()}: {status}")
                elif status == 'Failed' or 'Error' in status:
                    st.error(f"{layer.title()}: {status}")
                else:
                    st.info(f"{layer.title()}: {status}")

    with tab2:
        st.subheader("📊 Pipeline Statistics")

        # Show some mock statistics
        stats_cols = st.columns(3)

        with st.container():
            st.markdown('<div class="card">', unsafe_allow_html=True)
            with stats_cols[0]:
                st.metric("Records Processed", "1,250,000", "↑ 15%")
            with stats_cols[1]:
                st.metric("Data Quality", "97.8%", "↑ 2.1%")
            with stats_cols[2]:
                st.metric("Processing Time", "4.2 min", "↓ 30s")
            st.markdown('</div>', unsafe_allow_html=True)

# DATABASE EXPLORER PAGE
elif current_page == "database":
    st.header("Database Explorer")

    conn = get_database_connection()
    if not conn:
        st.error("Cannot connect to database. Please check your configuration.")
        st.stop()

    # Get all schemas and tables
    schema_query = """
    SELECT schemaname, tablename
    FROM pg_tables
    WHERE schemaname IN ('bronze', 'silver', 'gold', 'audit')
    ORDER BY schemaname, tablename;
    """

    tables_df = execute_query(schema_query)

    if tables_df is not None and not tables_df.empty:
        # Schema selector
        schemas = tables_df['schemaname'].unique()
        selected_schema = st.selectbox("Select Schema:", schemas)

        # Filter tables by schema
        schema_tables = tables_df[tables_df['schemaname'] == selected_schema]['tablename'].tolist()
        selected_table = st.selectbox("Select Table:", schema_tables)

        if selected_table:
            full_table_name = f"{selected_schema}.{selected_table}"
        else:
            st.warning("No tables found in selected schema.")
            st.stop()
    else:
        st.warning("No tables found. Please run the pipeline first.")
        st.stop()

    col1, col2 = st.columns([3, 1])
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        with col2:
            st.subheader("Table Info")
            # Get table info
            info_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{selected_schema}'
            AND table_name = '{selected_table}'
            ORDER BY ordinal_position;
            """
            info_df = execute_query(info_query)
            if info_df is not None:
                st.dataframe(info_df, use_container_width=True)
            # Get row count
            count_query = f"SELECT COUNT(*) as row_count FROM {full_table_name}"
            count_result = execute_query(count_query)
            if count_result is not None:
                row_count = count_result.iloc[0]['row_count']
                st.metric("Total Rows", row_count)
        with col1:
            st.subheader(f"{selected_schema}.{selected_table}")
            # Pagination
            rows_per_page = st.slider("Rows per page:", 10, 100, 50)
            page_number = st.number_input("Page:", min_value=1, value=1)
            offset = (page_number - 1) * rows_per_page
            # Data query with pagination
            data_query = f"""
            SELECT * FROM {full_table_name}
            LIMIT {rows_per_page} OFFSET {offset}
            """
            data_df = execute_query(data_query)
            if data_df is not None and not data_df.empty:
                st.dataframe(data_df, use_container_width=True)
                # Export options
                csv_data = data_df.to_csv(index=False)
                st.download_button(
                    label="Download as CSV",
                    data=csv_data,
                    file_name=f"{selected_schema}_{selected_table}.csv",
                    mime='text/csv'
                )
            else:
                st.info("No data found in this table.")
        st.markdown('</div>', unsafe_allow_html=True)

# DATA ANALYTICS PAGE
elif current_page == "analytics":
    st.header("📊 Data Analytics")

    conn = get_database_connection()
    if not conn:
        st.error("Cannot connect to database.")
        st.stop()

    tab1, tab2, tab3 = st.tabs(["📈 Overview", "🛒 Orders", "📦 Inventory"])

    with tab1:
        st.subheader("📈 Business Overview")

        # Key metrics
        metrics_cols = st.columns(4)

        try:
            # Get metrics from database
            orders_query = "SELECT COUNT(*) as total, SUM(total_amount) as revenue FROM silver.supply_orders"
            orders_result = execute_query(orders_query)

            products_query = "SELECT COUNT(*) as total FROM silver.products"
            products_result = execute_query(products_query)

            suppliers_query = "SELECT COUNT(*) as total FROM silver.suppliers"
            suppliers_result = execute_query(suppliers_query)

            if orders_result is not None and not orders_result.empty:
                total_orders = orders_result.iloc[0]['total']
                total_revenue = orders_result.iloc[0]['revenue'] or 0
            else:
                total_orders, total_revenue = 0, 0

            total_products = products_result.iloc[0]['total'] if products_result is not None else 0
            total_suppliers = suppliers_result.iloc[0]['total'] if suppliers_result is not None else 0

            with metrics_cols[0]:
                st.metric("Total Orders", f"{total_orders:,}")
            with metrics_cols[1]:
                st.metric("Total Revenue", f"${total_revenue:,.2f}")
            with metrics_cols[2]:
                st.metric("Products", f"{total_products}")
            with metrics_cols[3]:
                st.metric("Suppliers", f"{total_suppliers}")

        except Exception as e:
            st.error(f"Error loading metrics: {e}")

        # Charts
        chart_cols = st.columns(2)

        with chart_cols[0]:
            try:
                # Orders by status
                status_query = "SELECT order_status, COUNT(*) as count FROM silver.supply_orders GROUP BY order_status"
                status_df = execute_query(status_query)

                if status_df is not None and not status_df.empty:
                    fig = px.pie(status_df, values='count', names='order_status',
                               title='Orders by Status')
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error loading orders chart: {e}")

        with chart_cols[1]:
            try:
                # Products by category
                category_query = "SELECT category, COUNT(*) as count FROM silver.products GROUP BY category"
                category_df = execute_query(category_query)

                if category_df is not None and not category_df.empty:
                    fig = px.bar(category_df, x='category', y='count',
                               title='Products by Category')
                    st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error loading products chart: {e}")

    with tab2:
        st.subheader("🛒 Order Analysis")

        try:
            # Recent orders
            recent_query = """
            SELECT order_date, COUNT(*) as orders, SUM(total_amount) as revenue
            FROM silver.supply_orders
            WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY order_date
            ORDER BY order_date DESC
            LIMIT 30
            """
            recent_df = execute_query(recent_query)

            if recent_df is not None and not recent_df.empty:
                fig = px.line(recent_df, x='order_date', y='orders',
                             title='Daily Orders (Last 30 Days)')
                st.plotly_chart(fig, use_container_width=True)

                # Orders table
                st.subheader("Recent Orders")
                orders_query = """
                SELECT order_id, order_date, product_id, quantity,
                       total_amount, order_status
                FROM silver.supply_orders
                ORDER BY order_date DESC
                LIMIT 20
                """
                orders_df = execute_query(orders_query)
                if orders_df is not None:
                    st.dataframe(orders_df, use_container_width=True)

        except Exception as e:
            st.error(f"Error loading order analysis: {e}")

    with tab3:
        st.subheader("📦 Inventory Analysis")

        try:
            # Inventory levels
            inventory_query = """
            SELECT p.product_name, i.warehouse_name, i.stock_quantity, i.reorder_level,
                   CASE
                       WHEN i.stock_quantity <= i.reorder_level THEN 'Low Stock'
                       WHEN i.stock_quantity <= i.reorder_level * 1.5 THEN 'Medium Stock'
                       ELSE 'High Stock'
                   END as stock_status
            FROM silver.inventory i
            JOIN silver.products p ON i.product_id = p.product_id
            ORDER BY i.stock_quantity ASC
            """
            inventory_df = execute_query(inventory_query)

            if inventory_df is not None and not inventory_df.empty:
                # Stock status distribution
                status_counts = inventory_df['stock_status'].value_counts()
                fig = px.pie(values=status_counts.values, names=status_counts.index,
                           title='Inventory Stock Status')
                st.plotly_chart(fig, use_container_width=True)

                # Low stock alerts
                low_stock = inventory_df[inventory_df['stock_status'] == 'Low Stock']
                if not low_stock.empty:
                    st.warning(f"⚠️ {len(low_stock)} items are low in stock!")
                    st.dataframe(low_stock, use_container_width=True)
                else:
                    st.success("✅ All items are adequately stocked")

        except Exception as e:
            st.error(f"Error loading inventory analysis: {e}")

# QUERY RUNNER PAGE
elif current_page == "query":
    st.header("💻 SQL Query Runner")

    conn = get_database_connection()
    if not conn:
        st.error("Cannot connect to database.")
        st.stop()

    col1, col2 = st.columns([3, 1])

    with col1:
        st.subheader("✏️ Write Your Query")

        # Sample queries
        sample_queries = {
            "Select all orders": "SELECT * FROM silver.supply_orders LIMIT 10;",
            "Orders by status": "SELECT order_status, COUNT(*) FROM silver.supply_orders GROUP BY order_status;",
            "Revenue by product": """SELECT p.product_name, SUM(so.total_amount) as revenue
FROM silver.products p
JOIN silver.supply_orders so ON p.product_id = so.product_id
GROUP BY p.product_name ORDER BY revenue DESC;""",
            "Low stock items": """SELECT p.product_name, i.warehouse_name, i.stock_quantity, i.reorder_level
FROM silver.inventory i
JOIN silver.products p ON i.product_id = p.product_id
WHERE i.stock_quantity <= i.reorder_level;"""
        }

        selected_sample = st.selectbox("Choose a sample query:", ["Custom"] + list(sample_queries.keys()))

        if selected_sample != "Custom":
            default_query = sample_queries[selected_sample]
        else:
            default_query = ""

        query = st.text_area(
            "SQL Query:",
            value=default_query,
            height=200,
            help="Enter your SQL query here."
        )

        # Execute button
        if st.button("🚀 Execute Query", type="primary"):
            if query.strip():
                try:
                    with st.spinner("Executing query..."):
                        start_time = datetime.now()
                        result = execute_query(query)
                        end_time = datetime.now()
                        execution_time = (end_time - start_time).total_seconds()

                    if result is not None:
                        if isinstance(result, pd.DataFrame):
                            st.success(f"✅ Query executed in {execution_time:.3f} seconds")
                            st.subheader("📊 Results")

                            # Show metrics
                            if not result.empty:
                                result_cols = st.columns(3)
                                with result_cols[0]:
                                    st.metric("Rows", len(result))
                                with result_cols[1]:
                                    st.metric("Columns", len(result.columns))
                                with result_cols[2]:
                                    st.metric("Time", f"{execution_time:.3f}s")

                                # Display results
                                st.dataframe(result, use_container_width=True)

                                # Export option
                                csv_data = result.to_csv(index=False)
                                st.download_button(
                                    label="📥 Download as CSV",
                                    data=csv_data,
                                    file_name=f"query_results.csv",
                                    mime='text/csv'
                                )
                            else:
                                st.info("Query returned no results.")
                        else:
                            st.success("✅ Query executed successfully")
                    else:
                        st.error("❌ Query execution failed")

                except Exception as e:
                    st.error(f"❌ Query error: {str(e)}")
            else:
                st.warning("Please enter a query to execute")

    with col2:
        st.subheader("📚 Available Tables")

        st.markdown("### silver.suppliers")
        st.markdown("- supplier_id, supplier_name, email, phone, address")

        st.markdown("### silver.products")
        st.markdown("- product_id, sku, product_name, category, cost, status")

        st.markdown("### silver.supply_orders")
        st.markdown("- order_id, order_date, product_id, supplier_id")
        st.markdown("- quantity, unit_price, total_amount, order_status")

        st.markdown("### silver.inventory")
        st.markdown("- inventory_id, product_id, warehouse_name")
        st.markdown("- stock_quantity, reorder_level")

elif current_page == "forecasting":
    st.header("🔮 Demand Forecasting")

    conn = get_database_connection()
    if not conn:
        st.error("❌ Cannot connect to database.")
        st.stop()

    st.markdown("""
    ### 🎯 Flexible Forecasting
    Use filters below to generate and view demand forecasts:
    - **Product Level** → product_id based forecast  
    - **Warehouse Level** → warehouse_id based forecast  
    - **Duration** → select forecast horizon  
    """)

    # 🔹 Fetch filter values safely
    try:
        product_ids = execute_query("SELECT DISTINCT product_id FROM silver.supply_orders ORDER BY product_id LIMIT 200")
    except Exception:
        product_ids = pd.DataFrame(columns=["product_id"])

    try:
        warehouse_ids = execute_query("SELECT DISTINCT warehouse_id FROM silver.supply_orders ORDER BY warehouse_id LIMIT 200")
    except Exception:
        warehouse_ids = pd.DataFrame(columns=["warehouse_id"])

    col1, col2, col3 = st.columns(3)

    with col1:
        selected_product = st.selectbox(
            "📦 Product ID",
            ["All"] + product_ids['product_id'].astype(str).tolist() if not product_ids.empty else ["All"]
        )

    with col2:
        selected_warehouse = st.selectbox(
            "🏪 Warehouse ID",
            ["All"] + warehouse_ids['warehouse_id'].astype(str).tolist() if not warehouse_ids.empty else ["All"]
        )

    with col3:
        duration = st.selectbox("📅 Duration", ["4 weeks", "8 weeks", "12 weeks", "6 months", "12 months"])

    # 🔹 Button to trigger simple_forecasting.py
    if st.button("🚀 Run Forecasting", type="primary", use_container_width=True):
        with st.spinner("Running forecasting pipeline... ⏳"):
            try:
                import subprocess, sys
                result = subprocess.run(
                    [sys.executable, "simple_forecasting.py"],
                    capture_output=True, text=True
                )
                if result.returncode == 0:
                    st.success("🎉 Forecasting pipeline completed successfully!")
                else:
                    st.error("❌ Forecasting pipeline failed.")
                    st.code(result.stderr)
            except Exception as e:
                st.error(f"❌ Error running forecasting: {str(e)}")
                st.exception(e)

    # 🔹 Show Forecast Results
    st.subheader("📊 Forecast Results")

    forecast_query = """
        SELECT ds, yhat, yhat_lower, yhat_upper, level, entity_id, model, granularity
        FROM gold.forecasts
        WHERE ds >= CURRENT_DATE
    """

    if selected_product != "All":
        forecast_query += f" AND level = 'product' AND entity_id = '{selected_product}'"
    if selected_warehouse != "All":
        forecast_query += f" AND level = 'warehouse' AND entity_id = '{selected_warehouse}'"

    forecast_query += " ORDER BY ds ASC LIMIT 200"

    df_forecasts = execute_query(forecast_query)

    if df_forecasts is not None and not df_forecasts.empty:
        st.dataframe(df_forecasts, use_container_width=True)

        import plotly.express as px
        fig = px.line(df_forecasts, x="ds", y="yhat",
                      title="📈 Forecasted Demand",
                      markers=True)
        fig.add_scatter(x=df_forecasts["ds"], y=df_forecasts["yhat_lower"],
                        mode="lines", name="Lower Bound", line=dict(dash="dot"))
        fig.add_scatter(x=df_forecasts["ds"], y=df_forecasts["yhat_upper"],
                        mode="lines", name="Upper Bound", line=dict(dash="dot"))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("ℹ️ No forecasts found for the selected filters. Try running the pipeline again.")


elif current_page == "dashboard":
    st.header("📈 Business Intelligence Dashboard")

    conn = get_database_connection()
    if not conn:
        st.error("Cannot connect to database.")
        st.stop()

    # Executive Dashboard
    st.subheader("📊 Executive Summary")

    # Key metrics
    metrics_cols = st.columns(4)

    try:
        # Get key business metrics
        total_orders_query = "SELECT COUNT(*) as total FROM silver.supply_orders"
        total_revenue_query = "SELECT COALESCE(SUM(total_amount), 0) as revenue FROM silver.supply_orders"
        avg_order_query = "SELECT COALESCE(AVG(total_amount), 0) as avg_order FROM silver.supply_orders"
        delivered_orders_query = "SELECT COUNT(*) as delivered FROM silver.supply_orders WHERE order_status = 'Delivered'"

        total_orders_result = execute_query(total_orders_query)
        total_revenue_result = execute_query(total_revenue_query)
        avg_order_result = execute_query(avg_order_query)
        delivered_orders_result = execute_query(delivered_orders_query)

        total_orders = total_orders_result.iloc[0]['total'] if total_orders_result is not None else 0
        total_revenue = total_revenue_result.iloc[0]['revenue'] if total_revenue_result is not None else 0
        avg_order = avg_order_result.iloc[0]['avg_order'] if avg_order_result is not None else 0
        delivered_orders = delivered_orders_result.iloc[0]['delivered'] if delivered_orders_result is not None else 0

        fulfillment_rate = (delivered_orders / total_orders * 100) if total_orders > 0 else 0

        with metrics_cols[0]:
            st.metric("Total Orders", f"{total_orders:,}", "↑ 12%")
        with metrics_cols[1]:
            st.metric("Total Revenue", f"${total_revenue:,.2f}", "↑ 18%")
        with metrics_cols[2]:
            st.metric("Avg Order Value", f"${avg_order:.2f}", "↑ 5%")
        with metrics_cols[3]:
            st.metric("Fulfillment Rate", f"{fulfillment_rate:.1f}%", "↑ 3%")

    except Exception as e:
        st.error(f"Error loading metrics: {e}")

    # Charts section
    chart_cols = st.columns(2)

    with chart_cols[0]:
        st.subheader("Revenue Trend")
        try:
            # Monthly revenue trend
            trend_query = """
            SELECT
                DATE_TRUNC('month', order_date) as month,
                SUM(total_amount) as revenue
            FROM silver.supply_orders
            GROUP BY DATE_TRUNC('month', order_date)
            ORDER BY month
            """
            trend_df = execute_query(trend_query)

            if trend_df is not None and not trend_df.empty:
                fig = px.line(trend_df, x='month', y='revenue',
                             title='Monthly Revenue Trend')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No trend data available")

        except Exception as e:
            st.error(f"Error loading revenue trend: {e}")

    with chart_cols[1]:
        st.subheader("Top Products")
        try:
            # Top products by revenue
            top_products_query = """
            SELECT p.product_name, SUM(so.total_amount) as revenue
            FROM silver.products p
            JOIN silver.supply_orders so ON p.product_id = so.product_id
            GROUP BY p.product_name
            ORDER BY revenue DESC
            LIMIT 10
            """
            top_products_df = execute_query(top_products_query)

            if top_products_df is not None and not top_products_df.empty:
                fig = px.bar(top_products_df, x='revenue', y='product_name',
                           orientation='h', title='Top 10 Products by Revenue')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No product data available")

        except Exception as e:
            st.error(f"Error loading top products: {e}")

    # External BI Tools section
    st.subheader("🔗 External BI Tools Integration")

    tool = st.selectbox("Select BI Tool:", ["None", "Power BI", "Tableau", "Looker Studio"])

    if tool != "None":
        st.info(f"Connect {tool} using these database credentials:")
        st.json({
            "Host": DB_CONFIG['host'],
            "Database": DB_CONFIG['database'],
            "Port": DB_CONFIG['port'],
            "Username": DB_CONFIG['user'],
            "Password": "••••••••••"
        })




# Database status
try:
    conn = get_database_connection()
    if conn:
        st.success("Database connection successful.")
        conn.close()
    else:
        st.error("Database connection failed.")
except Exception as e:
    st.error(f"Database connection error: {e}")


# Pipeline status

for layer, status in st.session_state.pipeline_status.items():
    if status == 'Success':
        st.success(f"{layer.title()}: {status}")
    elif status == 'Failed' or 'Error' in status:
        st.error(f"{layer.title()}: {status}")
    else:
        st.info(f"{layer.title()}: {status}")
