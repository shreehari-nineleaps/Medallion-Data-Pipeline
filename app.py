"""
Streamlit UI for Medallion Data Pipeline
A comprehensive interface for managing and monitoring the supply chain data pipeline
"""

import streamlit as st
import streamlit.components.v1 as components
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
import schedule
import threading
import time
from scheduler_manager import get_scheduler_manager

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
        padding: 0.5rem 1rem;
        border-radius: 6px;
        margin-bottom: 1rem;
        color: #ffffff;
        text-align: left;
    }
    .main-header h1 {
        font-size: 1.1rem;
        margin: 0;
        font-weight: 500;
        letter-spacing: 0.2px;
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

# Initialize scheduler manager
@st.cache_resource
def get_scheduler():
    return get_scheduler_manager()



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

def run_scheduled_pipeline():
    """Run pipeline for scheduled execution"""
    try:
        from etl import run_full_pipeline
        success = run_full_pipeline()
        if success:
            st.session_state.pipeline_status['scheduled'] = f'Success at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        else:
            st.session_state.pipeline_status['scheduled'] = f'Failed at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
    except Exception as e:
        st.session_state.pipeline_status['scheduled'] = f'Error at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: {str(e)}'

# --- Top Navigation Bar Setup ---
pages = {
    "Dashboard Home": "home",
    "Pipeline Control": "pipeline",
    "Database Explorer": "database",
    "Forecasting": "forecasting",
    "Query Runner": "query",
    "BI Dashboard": "dashboard",
    "EDA Reports": "eda"
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
            st.query_params.page = page
            st.rerun()

# --- Assign current_page variable for routing ---
import urllib.parse
page_value = st.query_params.get('page', st.session_state.current_page)
if page_value in nav_pages:
    st.session_state.current_page = page_value
current_page = pages[st.session_state.current_page]
# Main header
st.markdown('<div class="main-header"><h1>Medallion Data Pipeline</h1></div>', unsafe_allow_html=True)

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

    tab1, tab2, tab3 = st.tabs(["Manual Control", "Status", "Scheduler"])

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

    with tab3:
        st.subheader("🕒 Pipeline Scheduler")

        col1, col2 = st.columns([2, 1])

        with col1:
            st.markdown("### Schedule Configuration")

            # Get scheduler manager
            scheduler_manager = get_scheduler()

            # Schedule type selection
            schedule_type = st.selectbox(
                "Schedule Type:",
                ["Daily", "Weekly", "Hourly", "Custom Cron"]
            )

            # Pipeline stage selection
            stage = st.selectbox(
                "Pipeline Stage:",
                ["full", "bronze", "silver", "gold"],
                help="Select which part of the pipeline to run on schedule"
            )

            # Time configuration based on schedule type
            if schedule_type == "Daily":
                schedule_time = st.time_input("Daily run time:", value=datetime.now().time())
                cron_expression = f"{schedule_time.minute} {schedule_time.hour} * * *"

            elif schedule_type == "Weekly":
                col_day, col_time = st.columns(2)
                with col_day:
                    weekday = st.selectbox("Day of week:",
                        ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
                with col_time:
                    schedule_time = st.time_input("Weekly run time:", value=datetime.now().time())

                weekday_map = {"Monday": 1, "Tuesday": 2, "Wednesday": 3, "Thursday": 4,
                             "Friday": 5, "Saturday": 6, "Sunday": 0}
                cron_expression = f"{schedule_time.minute} {schedule_time.hour} * * {weekday_map[weekday]}"

            elif schedule_type == "Hourly":
                minute = st.slider("Minute of hour:", 0, 59, 0)
                cron_expression = f"{minute} * * * *"

            else:  # Custom Cron
                cron_expression = st.text_input(
                    "Cron Expression:",
                    placeholder="0 8 * * 1-5  (8 AM on weekdays)",
                    help="Format: minute hour day month weekday"
                )

            st.info(f"**Cron Expression:** `{cron_expression}`")

            # Schedule actions
            col_add, col_remove = st.columns(2)

            with col_add:
                if st.button("➕ Add Schedule", use_container_width=True, type="primary"):
                    try:
                        job_config = scheduler_manager.add_schedule(
                            schedule_type=schedule_type,
                            cron_expression=cron_expression,
                            stage=stage,
                            name=f"Pipeline {schedule_type} Schedule ({stage.title()})"
                        )

                        if job_config:
                            st.success(f"✅ {schedule_type} schedule added successfully!")
                            st.rerun()
                        else:
                            st.error("❌ Failed to add schedule")

                    except Exception as e:
                        st.error(f"❌ Failed to add schedule: {str(e)}")

            with col_remove:
                if st.button("🗑️ Clear All Schedules", use_container_width=True, type="secondary"):
                    try:
                        success = scheduler_manager.clear_all_schedules()
                        if success:
                            st.success("🗑️ All schedules cleared!")
                            st.rerun()
                        else:
                            st.error("❌ Failed to clear schedules")

                    except Exception as e:
                        st.error(f"❌ Failed to clear schedules: {str(e)}")

        with col2:
            st.markdown("### Scheduler Status")

            # Get active jobs from scheduler manager
            active_jobs = scheduler_manager.get_active_jobs()
            scheduler_info = scheduler_manager.get_scheduler_info()

            # Scheduler status
            if scheduler_info['running'] and active_jobs:
                st.success("✅ Scheduler Active")
                st.metric("Active Jobs", len(active_jobs))
            else:
                st.info("⏸️ No Active Schedules")

            # Execution history
            history = scheduler_manager.get_execution_history(3)
            if history:
                st.markdown("**Recent Executions:**")
                for entry in reversed(history):
                    status_icon = "✅" if entry['status'] == 'success' else "❌" if entry['status'] == 'failed' else "⚠️"
                    st.write(f"{status_icon} {entry['stage']} - {entry['timestamp'][:19]}")

        # Active schedules table
        if active_jobs:
            st.markdown("---")
            st.markdown("### 📋 Active Schedules")

            # Display with custom formatting
            for job in active_jobs:
                with st.expander(f"📅 {job['type']} Schedule - {job['name']}", expanded=False):
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.write(f"**Type:** {job['type']}")
                        st.write(f"**Stage:** {job.get('stage', 'full').title()}")
                        st.write(f"**Cron:** `{job['cron_expression']}`")

                    with col2:
                        st.write(f"**Created:** {job['created'][:19]}")
                        st.write(f"**Next Run:** {job.get('next_run', 'N/A')[:19] if job.get('next_run') else 'N/A'}")

                    with col3:
                        if st.button(f"🗑️ Remove", key=f"remove_{job['id']}", use_container_width=True):
                            try:
                                success = scheduler_manager.remove_schedule(job['id'])
                                if success:
                                    st.success(f"Schedule removed!")
                                    st.rerun()
                                else:
                                    st.error("Failed to remove schedule")
                            except Exception as e:
                                st.error(f"Failed to remove schedule: {str(e)}")

        # Help section
        st.markdown("---")
        st.markdown("### ℹ️ Scheduling Help")

        with st.expander("Cron Expression Examples", expanded=False):
            st.markdown("""
            **Common Patterns:**
            - `0 8 * * *` - Daily at 8:00 AM
            - `0 8 * * 1-5` - Weekdays at 8:00 AM
            - `0 */6 * * *` - Every 6 hours
            - `30 2 * * 0` - Sundays at 2:30 AM
            - `0 9 1 * *` - First day of month at 9:00 AM

            **Format:** `minute hour day month weekday`
            - minute: 0-59
            - hour: 0-23
            - day: 1-31
            - month: 1-12
            - weekday: 0-7 (0 and 7 = Sunday)
            """)

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
# DATABASE EXPLORER PAGE
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
            "Orders by status": "SELECT status, COUNT(*) FROM silver.supply_orders GROUP BY status;",
            "Revenue by product": """SELECT p.product_name, SUM(so.total_invoice) as revenue
FROM silver.products p
JOIN silver.supply_orders so ON p.product_id = so.product_id
GROUP BY p.product_name ORDER BY revenue DESC;""",
            "Low stock items": """SELECT p.product_name, w.warehouse_name, i.quantity_on_hand
FROM silver.inventory i
JOIN silver.products p ON i.product_id = p.product_id
JOIN silver.warehouses w ON i.warehouse_id = w.warehouse_id
WHERE i.quantity_on_hand <= 50;"""
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
        st.markdown("- supplier_id, supplier_name, contact_email, phone_number")

        st.markdown("### silver.products")
        st.markdown("- product_id, product_name, unit_cost, selling_price, supplier_id, product_category, status")

        st.markdown("### silver.supply_orders")
        st.markdown("- supply_order_id, order_date, product_id, warehouse_id")
        st.markdown("- quantity, price, total_invoice, status")

        st.markdown("### silver.inventory")
        st.markdown("- inventory_id, product_id, warehouse_id")
        st.markdown("- quantity_on_hand, last_stocked_date")

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

    # Embed Looker Studio Dashboard
    looker_url = "https://lookerstudio.google.com/embed/reporting/6402e1b6-caac-4c6c-bad1-2ef421eb6a47/page/zXiVF"

    iframe_html = f"""
    <iframe
        width="100%"
        height="800"
        src="{looker_url}"
        frameborder="0"
        allowfullscreen>
    </iframe>
    """

    components.html(iframe_html, height=800)

elif current_page == "eda":
    st.header("📊 Exploratory Data Analysis (EDA)")

    st.markdown("""
    ### 🔍 Comprehensive Data Analysis
    Generate detailed insights, visualizations, and reports from your supply chain data pipeline.
    """)

    # EDA Control Panel
    col1, col2, col3 = st.columns(3)

    with col1:
        st.info("**📈 Analysis Scope**")
        st.write("• Data Quality Assessment")
        st.write("• Financial Analysis")
        st.write("• Inventory Insights")
        st.write("• Correlation Analysis")
        st.write("• Reconciliation Reports")

    with col2:
        st.info("**📊 Generated Outputs**")
        st.write("• Interactive Charts")
        st.write("• Statistical Reports")
        st.write("• Data Exports (CSV)")
        st.write("• Executive Summary")
        st.write("• Quality Metrics")

    with col3:
        st.info("**🎯 Business Value**")
        st.write("• Performance Insights")
        st.write("• Trend Analysis")
        st.write("• Risk Assessment")
        st.write("• Optimization Opportunities")
        st.write("• Data-Driven Decisions")

    st.markdown("---")

    # EDA Execution
    st.subheader("🚀 Run EDA Analysis")

    # Check if EDA has been run before
    eda_output_dir = Path("eda/outputs")
    eda_exists = eda_output_dir.exists() and any(eda_output_dir.iterdir())

    # EDA Status Display
    col_status, col_metrics = st.columns([2, 1])

    with col_status:
        if eda_exists:
            last_run = max(eda_output_dir.glob("*"), key=lambda x: x.stat().st_mtime)
            last_run_time = datetime.fromtimestamp(last_run.stat().st_mtime)
            st.success(f"✅ EDA last run: {last_run_time.strftime('%Y-%m-%d %H:%M:%S')}")

            # Show file counts
            charts_count = len(list((eda_output_dir / "charts").glob("*"))) if (eda_output_dir / "charts").exists() else 0
            reports_count = len(list((eda_output_dir / "reports").glob("*"))) if (eda_output_dir / "reports").exists() else 0
            csv_count = len(list((eda_output_dir / "csv").glob("*"))) if (eda_output_dir / "csv").exists() else 0

            st.info(f"📊 Generated: {charts_count} charts, {reports_count} reports, {csv_count} data files")
        else:
            st.info("🔍 No previous EDA analysis found. Run analysis to generate insights.")

    with col_metrics:
        if eda_exists:
            # Show quick metrics
            try:
                total_size = sum(f.stat().st_size for f in eda_output_dir.rglob('*') if f.is_file())
                size_mb = total_size / (1024 * 1024)
                st.metric("Output Size", f"{size_mb:.1f} MB")
                st.metric("Last Run", "Today" if last_run_time.date() == datetime.now().date() else "Earlier")
            except:
                pass

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        if st.button("🔬 Generate EDA Report", use_container_width=True, type="primary"):
            # Create progress indicators
            progress_placeholder = st.empty()
            status_placeholder = st.empty()

            with progress_placeholder:
                progress_bar = st.progress(0)

            with status_placeholder:
                st.info("🔄 Initializing EDA analysis...")

            try:
                # Update progress
                progress_bar.progress(10)
                status_placeholder.info("📊 Loading data from database...")
                time.sleep(0.5)

                progress_bar.progress(30)
                status_placeholder.info("🔍 Performing data quality analysis...")
                time.sleep(0.5)

                # Run EDA script
                result = subprocess.run(
                    [sys.executable, "eda/supply_chain_eda.py"],
                    capture_output=True,
                    text=True,
                    cwd=Path.cwd()
                )

                progress_bar.progress(80)
                status_placeholder.info("📈 Generating visualizations and reports...")
                time.sleep(0.5)

                progress_bar.progress(100)

                if result.returncode == 0:
                    status_placeholder.success("🎉 EDA Analysis completed successfully!")
                    st.balloons()
                    time.sleep(1)
                    progress_placeholder.empty()
                    status_placeholder.empty()
                    st.rerun()
                else:
                    progress_placeholder.empty()
                    status_placeholder.error("❌ EDA Analysis failed")
                    with st.expander("Error Details", expanded=True):
                        st.code(result.stderr)
                        if result.stdout:
                            st.text("Standard Output:")
                            st.code(result.stdout)

            except Exception as e:
                progress_placeholder.empty()
                status_placeholder.error(f"❌ Error running EDA: {str(e)}")

    with col2:
        if st.button("🔄 Refresh Results", use_container_width=True, type="secondary"):
            st.rerun()

    with col3:
        # Quick database connection test
        if st.button("🔗 Test Database", use_container_width=True, type="secondary"):
            try:
                conn = get_database_connection()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM bronze.suppliers")
                    count = cursor.fetchone()[0]
                    cursor.close()
                    conn.close()
                    st.success(f"✅ Connected! {count} suppliers found")
                else:
                    st.error("❌ Database connection failed")
            except Exception as e:
                st.error(f"❌ Connection error: {str(e)}")

    # Display EDA Results if available
    if eda_exists:
        st.markdown("---")
        st.subheader("📈 EDA Results")

        # Create tabs for different sections
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Charts", "📋 Data Quality", "📈 Statistics", "💾 Data Exports", "⚙️ Settings"])

        with tab1:
            st.markdown("### 📊 Generated Visualizations")

            charts_dir = eda_output_dir / "charts"
            if charts_dir.exists():
                chart_files = list(charts_dir.glob("*.png")) + list(charts_dir.glob("*.html"))

                if chart_files:
                    # Display PNG charts in organized columns
                    png_files = [f for f in chart_files if f.suffix == '.png']

                    if png_files:
                        st.markdown("#### 📈 Statistical Charts")
                        # Display PNG charts in 2-column layout
                        for i in range(0, len(png_files), 2):
                            cols = st.columns(2)

                            for j, col in enumerate(cols):
                                if i + j < len(png_files):
                                    chart_file = png_files[i + j]
                                    with col:
                                        st.markdown(f"**{chart_file.stem.replace('_', ' ').title()}**")
                                        st.image(str(chart_file), use_container_width=True)

                    # Display HTML charts
                    html_files = [f for f in chart_files if f.suffix == '.html']
                    if html_files:
                        st.markdown("---")
                        st.markdown("#### 🔄 Interactive Charts")

                        for html_file in html_files:
                            with st.expander(f"📊 {html_file.stem.replace('_', ' ').title()}", expanded=True):
                                with open(html_file, 'r') as f:
                                    html_content = f.read()
                                components.html(html_content, height=500, scrolling=True)
                else:
                    st.info("No chart files found. Run EDA analysis to generate visualizations.")
            else:
                st.info("Charts directory not found.")

        with tab2:
            st.markdown("### 📋 Data Quality Assessment")

            # Load and display data quality metrics
            csv_dir = eda_output_dir / "csv"
            if csv_dir.exists():
                # Data quality summary
                quality_files = {
                    'data_quality_summary.csv': '🔍 Data Quality Overview',
                    'missing_values_analysis.csv': '❌ Missing Values Analysis',
                    'duplicate_analysis.csv': '📝 Duplicate Records Analysis',
                    'data_types_info.csv': '🏷️ Data Types Information'
                }

                for filename, title in quality_files.items():
                    file_path = csv_dir / filename
                    if file_path.exists():
                        st.markdown(f"#### {title}")
                        try:
                            df = pd.read_csv(file_path)

                            if 'summary' in filename.lower():
                                # Display as metrics for summary
                                cols = st.columns(min(4, len(df.columns)))
                                for i, col in enumerate(df.columns):
                                    if i < len(cols):
                                        with cols[i]:
                                            if len(df) > 0:
                                                st.metric(col.replace('_', ' ').title(), df[col].iloc[0])
                            else:
                                # Display as table for detailed data
                                st.dataframe(df, use_container_width=True, height=300)

                            # Add download button
                            csv_data = df.to_csv(index=False)
                            st.download_button(
                                label=f"📄 Download {filename}",
                                data=csv_data,
                                file_name=filename,
                                mime="text/csv",
                                key=f"download_quality_{filename}"
                            )
                            st.markdown("---")

                        except Exception as e:
                            st.error(f"Error loading {filename}: {e}")

                # Quick data quality insights
                st.markdown("#### 🎯 Key Data Quality Insights")

                quality_insights = [
                    "💡 **Completeness**: Check missing value percentages across all tables",
                    "🔄 **Consistency**: Verify data format consistency across layers",
                    "🎯 **Accuracy**: Review data ranges and outlier detection",
                    "📊 **Uniqueness**: Analyze duplicate records and key constraints",
                    "⚡ **Timeliness**: Validate data freshness and update patterns"
                ]

                for insight in quality_insights:
                    st.markdown(insight)
            else:
                st.info("Data quality reports not available. Run EDA analysis to generate quality metrics.")

        with tab3:
            st.markdown("### 📈 Statistical Analysis")

            csv_dir = eda_output_dir / "csv"
            if csv_dir.exists():
                # Statistical summary files
                stat_files = {
                    'statistical_summary.csv': '📊 Descriptive Statistics',
                    'correlation_matrix.csv': '🔗 Correlation Analysis',
                    'financial_summary.csv': '💰 Financial Metrics',
                    'inventory_metrics.csv': '📦 Inventory Analysis'
                }

                for filename, title in stat_files.items():
                    file_path = csv_dir / filename
                    if file_path.exists():
                        with st.expander(f"{title}", expanded=False):
                            try:
                                df = pd.read_csv(file_path)

                                if 'correlation' in filename.lower():
                                    # Display correlation matrix as heatmap
                                    st.write("**Correlation Matrix:**")
                                    if len(df.columns) > 1:
                                        # Create a simple correlation display
                                        numeric_cols = df.select_dtypes(include=[np.number]).columns
                                        if len(numeric_cols) > 1:
                                            corr_data = df[numeric_cols].corr()
                                            st.dataframe(corr_data.round(3), use_container_width=True)
                                        else:
                                            st.dataframe(df, use_container_width=True)
                                    else:
                                        st.dataframe(df, use_container_width=True)
                                else:
                                    # Regular table display
                                    st.dataframe(df, use_container_width=True, height=300)

                                # Download option
                                csv_data = df.to_csv(index=False)
                                st.download_button(
                                    label=f"📄 Download {filename}",
                                    data=csv_data,
                                    file_name=filename,
                                    mime="text/csv",
                                    key=f"download_stats_{filename}"
                                )
                            except Exception as e:
                                st.error(f"Error loading {filename}: {e}")

                # Key statistical insights
                st.markdown("---")
                st.markdown("#### 🔢 Statistical Highlights")

                try:
                    insights_file = csv_dir / "key_insights.csv"
                    if insights_file.exists():
                        insights_df = pd.read_csv(insights_file)

                        col1, col2 = st.columns(2)

                        with col1:
                            st.markdown("**📈 Key Findings:**")
                            for i, insight in enumerate(insights_df['Insight'][:5], 1):
                                st.write(f"{i}. {insight}")

                        with col2:
                            st.markdown("**🎯 Recommendations:**")
                            if len(insights_df) > 5:
                                for i, insight in enumerate(insights_df['Insight'][5:10], 1):
                                    st.write(f"{i}. {insight}")
                            else:
                                st.info("Additional insights will appear after running comprehensive analysis.")

                except Exception as e:
                    st.info("Statistical insights will be available after running EDA analysis.")
            else:
                st.info("Statistical analysis not available. Run EDA analysis to generate statistics.")

        with tab4:
            st.markdown("### 💾 Data Exports & Downloads")

            csv_dir = eda_output_dir / "csv"
            if csv_dir.exists():
                csv_files = list(csv_dir.glob("*.csv"))

                if csv_files:
                    st.write(f"**📁 Available Data Files ({len(csv_files)} files):**")

                    # Organize files by category
                    file_categories = {
                        'Quality Reports': [f for f in csv_files if any(x in f.name.lower() for x in ['quality', 'missing', 'duplicate', 'types'])],
                        'Statistical Data': [f for f in csv_files if any(x in f.name.lower() for x in ['statistical', 'correlation', 'summary'])],
                        'Business Reports': [f for f in csv_files if any(x in f.name.lower() for x in ['financial', 'inventory', 'insights'])],
                        'Raw Data': [f for f in csv_files if not any(x in f.name.lower() for x in ['quality', 'missing', 'duplicate', 'types', 'statistical', 'correlation', 'summary', 'financial', 'inventory', 'insights'])]
                    }

                    for category, files in file_categories.items():
                        if files:
                            with st.expander(f"📂 {category} ({len(files)} files)", expanded=False):
                                for csv_file in files:
                                    col1, col2, col3, col4 = st.columns([3, 1, 1, 1])

                                    with col1:
                                        st.write(f"📄 {csv_file.name}")

                                    with col2:
                                        # File size
                                        size_mb = csv_file.stat().st_size / (1024 * 1024)
                                        if size_mb < 1:
                                            st.write(f"{csv_file.stat().st_size / 1024:.1f} KB")
                                        else:
                                            st.write(f"{size_mb:.1f} MB")

                                    with col3:
                                        # Preview button
                                        if st.button("👁️ Preview", key=f"preview_{category}_{csv_file.name}"):
                                            st.session_state[f"preview_file_{category}_{csv_file.name}"] = True

                                    with col4:
                                        # Download button
                                        with open(csv_file, 'rb') as f:
                                            csv_data = f.read()

                                        st.download_button(
                                            label="⬇️ Download",
                                            data=csv_data,
                                            file_name=csv_file.name,
                                            mime="text/csv",
                                            key=f"download_export_{category}_{csv_file.name}"
                                        )

                                    # Show preview if requested
                                    if st.session_state.get(f"preview_file_{category}_{csv_file.name}", False):
                                        try:
                                            df_preview = pd.read_csv(csv_file)
                                            st.write(f"**Preview of {csv_file.name}** ({len(df_preview)} rows, {len(df_preview.columns)} columns)")
                                            st.dataframe(df_preview.head(10), use_container_width=True)
                                            if st.button("❌ Close Preview", key=f"close_preview_{category}_{csv_file.name}"):
                                                st.session_state[f"preview_file_{category}_{csv_file.name}"] = False
                                                st.rerun()
                                        except Exception as e:
                                            st.error(f"Error loading preview: {e}")

                    # Summary statistics
                    st.markdown("---")
                    st.markdown("### 📈 Export Summary")

                    summary_stats = {
                        "Total Files": len(csv_files),
                        "Total Charts": len(list((eda_output_dir / "charts").glob("*"))) if (eda_output_dir / "charts").exists() else 0,
                        "Reports Available": len(list((eda_output_dir / "reports").glob("*"))) if (eda_output_dir / "reports").exists() else 0,
                        "Last Updated": datetime.fromtimestamp(last_run.stat().st_mtime).strftime('%m/%d %H:%M') if eda_exists else "Never"
                    }

                    col1, col2, col3, col4 = st.columns(4)
                    metrics = list(summary_stats.items())

                    with col1:
                        st.metric("Data Files", metrics[0][1])
                    with col2:
                        st.metric("Charts", metrics[1][1])
                    with col3:
                        st.metric("Reports", metrics[2][1])
                    with col4:
                        st.metric("Last Updated", metrics[3][1] if isinstance(metrics[3][1], int) else "")

                else:
                    st.info("No data export files found.")
            else:
                st.info("Export directory not found.")

        with tab5:
            st.markdown("### ⚙️ EDA Configuration")

            # EDA settings
            st.markdown("#### Analysis Options")

            col1, col2 = st.columns(2)

            with col1:
                st.checkbox("📊 Generate Interactive Charts", value=True, help="Create interactive Plotly visualizations")
                st.checkbox("📈 Statistical Analysis", value=True, help="Include correlation and statistical tests")
                st.checkbox("💾 Export Raw Data", value=True, help="Export processed data as CSV files")

            with col2:
                st.checkbox("🔍 Data Quality Checks", value=True, help="Comprehensive data quality assessment")
                st.checkbox("💰 Financial Analysis", value=True, help="Revenue and cost analysis")
                st.checkbox("📦 Inventory Insights", value=True, help="Stock level and turnover analysis")

            st.markdown("---")
            st.markdown("#### Output Management")

            col1, col2 = st.columns(2)

            with col1:
                if st.button("🗂️ Open Output Folder", use_container_width=True):
                    try:
                        import subprocess
                        subprocess.run(['xdg-open', str(eda_output_dir)])
                    except:
                        st.info(f"📁 Output directory: {eda_output_dir}")

            with col2:
                if st.button("🧹 Clear Output Folder", use_container_width=True, type="secondary"):
                    if st.session_state.get('confirm_clear', False):
                        try:
                            import shutil
                            if eda_output_dir.exists():
                                shutil.rmtree(eda_output_dir)
                                eda_output_dir.mkdir(exist_ok=True)
                                (eda_output_dir / 'charts').mkdir(exist_ok=True)
                                (eda_output_dir / 'csv').mkdir(exist_ok=True)
                                (eda_output_dir / 'reports').mkdir(exist_ok=True)
                            st.success("🧹 Output folder cleared!")
                            st.session_state.confirm_clear = False
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error clearing folder: {e}")
                    else:
                        st.session_state.confirm_clear = True
                        st.warning("⚠️ Click again to confirm clearing all EDA outputs")

            if not st.session_state.get('confirm_clear', False):
                pass
            else:
                if st.button("❌ Cancel Clear", use_container_width=True):
                    st.session_state.confirm_clear = False
                    st.rerun()

    else:
        st.markdown("---")
        st.info("🔍 **No EDA results available.** Click 'Generate EDA Report' to create comprehensive analysis of your supply chain data.")

# Show database status and pipeline status only on home and pipeline pages
if current_page in ["home", "pipeline"]:
    st.markdown("---")

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
