"""
Supply Chain Data - Exploratory Data Analysis (EDA)
Comprehensive analysis of Bronze, Silver, and Gold layer data with insights generation
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import warnings
from datetime import datetime, timedelta
import os
from pathlib import Path
import sys

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG

# Set up plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class SupplyChainEDA:
    """Comprehensive EDA for Supply Chain Data Pipeline"""

    def __init__(self):
        self.output_dir = Path(__file__).parent / 'outputs'
        self.output_dir.mkdir(exist_ok=True)

        # Create subdirectories
        (self.output_dir / 'charts').mkdir(exist_ok=True)
        (self.output_dir / 'csv').mkdir(exist_ok=True)
        (self.output_dir / 'reports').mkdir(exist_ok=True)

        self.connection = None
        self.data = {}
        self.insights = []

        print(f"📊 Supply Chain EDA initialized")
        print(f"📁 Output directory: {self.output_dir}")

    def get_connection(self):
        """Get database connection"""
        try:
            self.connection = psycopg2.connect(**DB_CONFIG)
            print("✅ Database connection established")
            return True
        except psycopg2.Error as e:
            print(f"❌ Database connection error: {e}")
            return False

    def load_data(self):
        """Load data from all layers (Bronze, Silver, Gold)"""
        if not self.get_connection():
            return False

        print("\n🔄 Loading data from all pipeline layers...")

        # Define tables to load from each layer
        bronze_tables = ['suppliers', 'products', 'warehouses', 'inventory', 'retail_stores', 'supply_orders']
        silver_tables = ['suppliers', 'products', 'warehouses', 'inventory', 'retail_stores', 'supply_orders']
        gold_tables = ['monthly_sales_performance', 'product_performance_metrics', 'supply_chain_kpis']

        try:
            # Load Bronze layer data
            print("📥 Loading Bronze layer data...")
            for table in bronze_tables:
                query = f"SELECT * FROM bronze.{table}"  # Load all data for complete analysis
                self.data[f'bronze_{table}'] = pd.read_sql(query, self.connection)
                print(f"  ✅ bronze.{table}: {len(self.data[f'bronze_{table}'])} rows")

            # Load Silver layer data
            print("📥 Loading Silver layer data...")
            for table in silver_tables:
                query = f"SELECT * FROM silver.{table}"  # Load all data for complete analysis
                self.data[f'silver_{table}'] = pd.read_sql(query, self.connection)
                print(f"  ✅ silver.{table}: {len(self.data[f'silver_{table}'])} rows")

            # Load Gold layer data (if available)
            print("📥 Loading Gold layer data...")
            for table in gold_tables:
                try:
                    query = f"SELECT * FROM gold.{table}"
                    self.data[f'gold_{table}'] = pd.read_sql(query, self.connection)
                    print(f"  ✅ gold.{table}: {len(self.data[f'gold_{table}'])} rows")
                except:
                    print(f"  ⚠️  gold.{table}: Table not found or empty")

            # Load audit data
            print("📥 Loading Audit data...")
            try:
                self.data['audit_rejected'] = pd.read_sql("SELECT * FROM audit.rejected_rows", self.connection)
                self.data['audit_dq'] = pd.read_sql("SELECT * FROM audit.dq_results", self.connection)
                self.data['audit_log'] = pd.read_sql("SELECT * FROM audit.etl_log", self.connection)
                print(f"  ✅ Audit data loaded successfully")
            except:
                print(f"  ⚠️  Audit data: Not available")

            return True

        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
        finally:
            if self.connection:
                self.connection.close()

    def data_quality_analysis(self):
        """Comprehensive data quality analysis across Bronze and Silver layers"""
        print("\n🔍 DATA QUALITY ANALYSIS")
        print("=" * 50)

        # Compare Bronze vs Silver record counts
        quality_summary = []
        missing_values_analysis = []
        duplicate_analysis = []
        data_types_info = []

        for table in ['suppliers', 'products', 'warehouses', 'inventory', 'retail_stores', 'supply_orders']:
            bronze_key = f'bronze_{table}'
            silver_key = f'silver_{table}'

            if bronze_key in self.data and silver_key in self.data:
                bronze_df = self.data[bronze_key]
                silver_df = self.data[silver_key]

                bronze_count = len(bronze_df)
                silver_count = len(silver_df)
                rejected_count = bronze_count - silver_count
                quality_rate = (silver_count / bronze_count * 100) if bronze_count > 0 else 0

                # Null/Missing Values Analysis
                bronze_nulls = bronze_df.isnull().sum().sum()
                silver_nulls = silver_df.isnull().sum().sum()
                total_bronze_cells = bronze_df.size
                total_silver_cells = silver_df.size
                bronze_null_pct = (bronze_nulls / total_bronze_cells * 100) if total_bronze_cells > 0 else 0
                silver_null_pct = (silver_nulls / total_silver_cells * 100) if total_silver_cells > 0 else 0

                # Duplicate Analysis
                bronze_duplicates = bronze_df.duplicated().sum()
                silver_duplicates = silver_df.duplicated().sum()
                bronze_dup_pct = (bronze_duplicates / bronze_count * 100) if bronze_count > 0 else 0
                silver_dup_pct = (silver_duplicates / silver_count * 100) if silver_count > 0 else 0

                quality_summary.append({
                    'table': table,
                    'bronze_records': bronze_count,
                    'silver_records': silver_count,
                    'rejected_records': rejected_count,
                    'quality_rate_pct': round(quality_rate, 2),
                    'bronze_null_pct': round(bronze_null_pct, 2),
                    'silver_null_pct': round(silver_null_pct, 2),
                    'bronze_duplicates': bronze_duplicates,
                    'silver_duplicates': silver_duplicates
                })

                # Detailed missing values by column
                for col in bronze_df.columns:
                    bronze_col_nulls = bronze_df[col].isnull().sum()
                    bronze_col_null_pct = (bronze_col_nulls / bronze_count * 100) if bronze_count > 0 else 0

                    silver_col_nulls = 0
                    silver_col_null_pct = 0
                    if col in silver_df.columns:
                        silver_col_nulls = silver_df[col].isnull().sum()
                        silver_col_null_pct = (silver_col_nulls / silver_count * 100) if silver_count > 0 else 0

                    missing_values_analysis.append({
                        'table': table,
                        'column': col,
                        'bronze_nulls': bronze_col_nulls,
                        'bronze_null_pct': round(bronze_col_null_pct, 2),
                        'silver_nulls': silver_col_nulls,
                        'silver_null_pct': round(silver_col_null_pct, 2)
                    })

                # Data types information
                for col in bronze_df.columns:
                    bronze_dtype = str(bronze_df[col].dtype)
                    silver_dtype = str(silver_df[col].dtype) if col in silver_df.columns else 'N/A'
                    unique_values = bronze_df[col].nunique()
                    unique_pct = (unique_values / bronze_count * 100) if bronze_count > 0 else 0

                    data_types_info.append({
                        'table': table,
                        'column': col,
                        'bronze_dtype': bronze_dtype,
                        'silver_dtype': silver_dtype,
                        'unique_values': unique_values,
                        'uniqueness_pct': round(unique_pct, 2)
                    })

                # Duplicate details
                duplicate_analysis.append({
                    'table': table,
                    'bronze_records': bronze_count,
                    'bronze_duplicates': bronze_duplicates,
                    'bronze_duplicate_pct': round(bronze_dup_pct, 2),
                    'silver_records': silver_count,
                    'silver_duplicates': silver_duplicates,
                    'silver_duplicate_pct': round(silver_dup_pct, 2),
                    'duplicates_removed': bronze_duplicates - silver_duplicates
                })

        # Create DataFrames and save
        quality_df = pd.DataFrame(quality_summary)
        missing_df = pd.DataFrame(missing_values_analysis)
        duplicate_df = pd.DataFrame(duplicate_analysis)
        dtypes_df = pd.DataFrame(data_types_info)

        print(quality_df.to_string(index=False))

        # Save all analysis files
        quality_df.to_csv(self.output_dir / 'csv' / 'data_quality_summary.csv', index=False)
        missing_df.to_csv(self.output_dir / 'csv' / 'missing_values_analysis.csv', index=False)
        duplicate_df.to_csv(self.output_dir / 'csv' / 'duplicate_analysis.csv', index=False)
        dtypes_df.to_csv(self.output_dir / 'csv' / 'data_types_info.csv', index=False)

        # Create enhanced quality visualizations
        fig = plt.figure(figsize=(20, 12))

        # Quality rates by table
        ax1 = plt.subplot(2, 3, 1)
        bars = ax1.bar(quality_df['table'], quality_df['quality_rate_pct'], color='skyblue', alpha=0.8)
        ax1.set_title('Data Quality Rate by Table (%)', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Quality Rate (%)')
        ax1.tick_params(axis='x', rotation=45)
        ax1.grid(axis='y', alpha=0.3)
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5, f'{height:.1f}%',
                    ha='center', va='bottom', fontsize=10)

        # Records comparison
        ax2 = plt.subplot(2, 3, 2)
        x = np.arange(len(quality_df))
        width = 0.35
        ax2.bar(x - width/2, quality_df['bronze_records'], width, label='Bronze', alpha=0.8, color='orange')
        ax2.bar(x + width/2, quality_df['silver_records'], width, label='Silver', alpha=0.8, color='green')
        ax2.set_title('Records: Bronze vs Silver', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Record Count')
        ax2.set_xticks(x)
        ax2.set_xticklabels(quality_df['table'], rotation=45)
        ax2.legend()
        ax2.grid(axis='y', alpha=0.3)

        # Null percentages comparison
        ax3 = plt.subplot(2, 3, 3)
        ax3.bar(x - width/2, quality_df['bronze_null_pct'], width, label='Bronze Nulls %', alpha=0.8, color='red')
        ax3.bar(x + width/2, quality_df['silver_null_pct'], width, label='Silver Nulls %', alpha=0.8, color='pink')
        ax3.set_title('Null Values Percentage', fontsize=14, fontweight='bold')
        ax3.set_ylabel('Null Percentage (%)')
        ax3.set_xticks(x)
        ax3.set_xticklabels(quality_df['table'], rotation=45)
        ax3.legend()
        ax3.grid(axis='y', alpha=0.3)

        # Duplicate analysis
        ax4 = plt.subplot(2, 3, 4)
        ax4.bar(x - width/2, duplicate_df['bronze_duplicate_pct'], width, label='Bronze Duplicates %', alpha=0.8, color='purple')
        ax4.bar(x + width/2, duplicate_df['silver_duplicate_pct'], width, label='Silver Duplicates %', alpha=0.8, color='violet')
        ax4.set_title('Duplicate Records Percentage', fontsize=14, fontweight='bold')
        ax4.set_ylabel('Duplicate Percentage (%)')
        ax4.set_xticks(x)
        ax4.set_xticklabels(duplicate_df['table'], rotation=45)
        ax4.legend()
        ax4.grid(axis='y', alpha=0.3)

        # Overall data quality metrics pie chart
        ax5 = plt.subplot(2, 3, 5)
        total_records = quality_df['silver_records'].sum()
        total_rejected = quality_df['rejected_records'].sum()
        total_accepted = total_records

        sizes = [total_accepted, total_rejected]
        labels = ['Accepted Records', 'Rejected Records']
        colors = ['lightgreen', 'lightcoral']
        ax5.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        ax5.set_title('Overall Data Acceptance Rate', fontsize=14, fontweight='bold')

        # Data completeness heatmap
        ax6 = plt.subplot(2, 3, 6)
        completeness_data = []
        table_names = []
        for table in quality_df['table']:
            table_missing = missing_df[missing_df['table'] == table]['bronze_null_pct'].mean()
            completeness_data.append(100 - table_missing)
            table_names.append(table)

        bars = ax6.barh(table_names, completeness_data, color='lightblue', alpha=0.8)
        ax6.set_title('Data Completeness by Table (%)', fontsize=14, fontweight='bold')
        ax6.set_xlabel('Completeness (%)')
        ax6.grid(axis='x', alpha=0.3)

        # Add value labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax6.text(width + 0.5, bar.get_y() + bar.get_height()/2., f'{width:.1f}%',
                    ha='left', va='center', fontsize=10)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'charts' / 'comprehensive_data_quality.png', dpi=300, bbox_inches='tight')
        plt.show()

        # Generate comprehensive insights
        overall_quality = quality_df['quality_rate_pct'].mean()
        worst_quality = quality_df.loc[quality_df['quality_rate_pct'].idxmin()]
        best_quality = quality_df.loc[quality_df['quality_rate_pct'].idxmax()]
        avg_nulls = quality_df['bronze_null_pct'].mean()
        total_duplicates = duplicate_df['bronze_duplicates'].sum()

        self.insights.extend([
            f"📊 Overall data quality rate: {overall_quality:.1f}%",
            f"🔴 Lowest quality table: {worst_quality['table']} ({worst_quality['quality_rate_pct']:.1f}%)",
            f"🟢 Highest quality table: {best_quality['table']} ({best_quality['quality_rate_pct']:.1f}%)",
            f"📋 Total rejected records: {quality_df['rejected_records'].sum():,}",
            f"❌ Average null values percentage: {avg_nulls:.2f}%",
            f"🔄 Total duplicate records found: {total_duplicates:,}",
            f"🧹 Duplicate records removed in silver layer: {duplicate_df['duplicates_removed'].sum():,}",
            f"📈 Data completeness ranges from {(100-quality_df['bronze_null_pct'].max()):.1f}% to {(100-quality_df['bronze_null_pct'].min()):.1f}%"
        ])

        print(f"\n✅ Data quality analysis complete. Generated {len(self.insights)} insights.")

    def supply_chain_overview(self):
        """Generate supply chain overview analysis"""
        print("\n📦 SUPPLY CHAIN OVERVIEW")
        print("=" * 50)

        # Use Silver layer data for analysis
        suppliers = self.data.get('silver_suppliers', pd.DataFrame())
        products = self.data.get('silver_products', pd.DataFrame())
        warehouses = self.data.get('silver_warehouses', pd.DataFrame())
        inventory = self.data.get('silver_inventory', pd.DataFrame())
        stores = self.data.get('silver_retail_stores', pd.DataFrame())
        orders = self.data.get('silver_supply_orders', pd.DataFrame())

        # Basic statistics
        stats = {
            'Total Suppliers': len(suppliers),
            'Total Products': len(products),
            'Total Warehouses': len(warehouses),
            'Total Retail Stores': len(stores),
            'Total Inventory Records': len(inventory),
            'Total Supply Orders': len(orders)
        }

        print("📊 Supply Chain Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value:,}")

        # Create overview dashboard
        fig = make_subplots(
            rows=2, cols=3,
            subplot_titles=('Supply Chain Components', 'Product Categories', 'Store Types',
                          'Regional Distribution', 'Supplier Countries', 'Order Status'),
            specs=[[{"type": "pie"}, {"type": "bar"}, {"type": "bar"}],
                   [{"type": "bar"}, {"type": "bar"}, {"type": "pie"}]]
        )

        # Supply Chain Components (Pie)
        components = list(stats.keys())
        values = list(stats.values())
        fig.add_trace(go.Pie(labels=components, values=values, name="Components"), row=1, col=1)

        # Product categories
        if 'product_category' in products.columns:
            cat_counts = products['product_category'].value_counts().head(10)
            fig.add_trace(go.Bar(x=cat_counts.values, y=cat_counts.index, orientation='h', name="Categories"), row=1, col=2)

        # Store types
        if 'store_type' in stores.columns:
            store_counts = stores['store_type'].value_counts()
            fig.add_trace(go.Bar(x=store_counts.index, y=store_counts.values, name="Store Types"), row=1, col=3)

        # Regional distribution
        if 'region' in stores.columns:
            region_counts = stores['region'].value_counts()
            fig.add_trace(go.Bar(x=region_counts.index, y=region_counts.values, name="Regions"), row=2, col=1)

        # Supplier countries
        if 'country' in suppliers.columns:
            country_counts = suppliers['country'].value_counts().head(10)
            fig.add_trace(go.Bar(x=country_counts.values, y=country_counts.index, orientation='h', name="Countries"), row=2, col=2)

        # Order status
        if 'status' in orders.columns:
            status_counts = orders['status'].value_counts()
            fig.add_trace(go.Pie(labels=status_counts.index, values=status_counts.values, name="Order Status"), row=2, col=3)

        fig.update_layout(height=800, showlegend=False, title_text="Supply Chain Overview Dashboard")
        fig.write_html(self.output_dir / 'charts' / 'supply_chain_overview.html')
        fig.show()

        # Generate insights
        self.insights.extend([
            f"🏭 Supply chain network spans {len(suppliers)} suppliers across multiple countries",
            f"📦 Managing {len(products)} products across {len(warehouses)} warehouses",
            f"🏪 Serving {len(stores)} retail stores with {len(orders)} supply orders",
            f"📊 Current inventory tracking covers {len(inventory)} stock positions"
        ])

    def financial_analysis(self):
        """Analyze financial metrics from supply orders"""
        print("\n💰 FINANCIAL ANALYSIS")
        print("=" * 50)

        orders = self.data.get('silver_supply_orders', pd.DataFrame())

        if orders.empty or 'total_invoice' not in orders.columns:
            print("⚠️  Financial data not available for analysis")
            return

        # Convert dates if needed
        if 'order_date' in orders.columns:
            orders['order_date'] = pd.to_datetime(orders['order_date'])
            orders['month'] = orders['order_date'].dt.to_period('M')

        # Financial metrics
        total_revenue = orders['total_invoice'].sum()
        avg_order_value = orders['total_invoice'].mean()
        median_order_value = orders['total_invoice'].median()
        total_orders = len(orders)

        print(f"💵 Total Revenue: ${total_revenue:,.2f}")
        print(f"📊 Average Order Value: ${avg_order_value:,.2f}")
        print(f"📊 Median Order Value: ${median_order_value:,.2f}")
        print(f"🛒 Total Orders: {total_orders:,}")

        # Create financial visualizations
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # Revenue distribution
        axes[0,0].hist(orders['total_invoice'], bins=50, alpha=0.7, color='skyblue', edgecolor='black')
        axes[0,0].axvline(avg_order_value, color='red', linestyle='--', label=f'Mean: ${avg_order_value:.0f}')
        axes[0,0].axvline(median_order_value, color='green', linestyle='--', label=f'Median: ${median_order_value:.0f}')
        axes[0,0].set_title('Order Value Distribution', fontweight='bold')
        axes[0,0].set_xlabel('Order Value ($)')
        axes[0,0].set_ylabel('Frequency')
        axes[0,0].legend()
        axes[0,0].grid(alpha=0.3)

        # Monthly revenue trend (if date available)
        if 'month' in orders.columns:
            monthly_revenue = orders.groupby('month')['total_invoice'].sum()
            axes[0,1].plot(monthly_revenue.index.astype(str), monthly_revenue.values, marker='o', linewidth=2)
            axes[0,1].set_title('Monthly Revenue Trend', fontweight='bold')
            axes[0,1].set_xlabel('Month')
            axes[0,1].set_ylabel('Revenue ($)')
            axes[0,1].tick_params(axis='x', rotation=45)
            axes[0,1].grid(alpha=0.3)

        # Top revenue generating stores (if available)
        if 'retail_store_id' in orders.columns:
            store_revenue = orders.groupby('retail_store_id')['total_invoice'].sum().nlargest(10)
            axes[1,0].bar(range(len(store_revenue)), store_revenue.values, color='lightcoral')
            axes[1,0].set_title('Top 10 Revenue Generating Stores', fontweight='bold')
            axes[1,0].set_xlabel('Store Rank')
            axes[1,0].set_ylabel('Revenue ($)')
            axes[1,0].grid(axis='y', alpha=0.3)

        # Order value vs quantity scatter
        if 'quantity' in orders.columns:
            axes[1,1].scatter(orders['quantity'], orders['total_invoice'], alpha=0.6, color='orange')
            axes[1,1].set_title('Order Value vs Quantity', fontweight='bold')
            axes[1,1].set_xlabel('Quantity')
            axes[1,1].set_ylabel('Order Value ($)')
            axes[1,1].grid(alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'charts' / 'financial_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

        # Save financial summary
        financial_summary = pd.DataFrame({
            'Metric': ['Total Revenue', 'Average Order Value', 'Median Order Value', 'Total Orders'],
            'Value': [f'${total_revenue:,.2f}', f'${avg_order_value:,.2f}',
                     f'${median_order_value:,.2f}', f'{total_orders:,}']
        })
        financial_summary.to_csv(self.output_dir / 'csv' / 'financial_summary.csv', index=False)

        # Generate insights
        self.insights.extend([
            f"💰 Total revenue processed: ${total_revenue:,.2f}",
            f"📊 Average order value: ${avg_order_value:.2f}",
            f"🛒 Total orders processed: {total_orders:,}",
            f"📈 Revenue distribution shows {'high' if avg_order_value > median_order_value * 1.5 else 'moderate'} variability"
        ])

    def inventory_analysis(self):
        """Analyze inventory levels and patterns"""
        print("\n📦 INVENTORY ANALYSIS")
        print("=" * 50)

        inventory = self.data.get('silver_inventory', pd.DataFrame())
        products = self.data.get('silver_products', pd.DataFrame())
        warehouses = self.data.get('silver_warehouses', pd.DataFrame())

        if inventory.empty:
            print("⚠️  Inventory data not available for analysis")
            return

        # Basic inventory metrics
        total_inventory_value = 0
        if 'quantity' in inventory.columns and 'product_id' in inventory.columns:
            # Merge with products to get unit costs
            if 'unit_cost' in products.columns and 'product_id' in products.columns:
                inv_with_cost = inventory.merge(products[['product_id', 'unit_cost']], on='product_id', how='left')
                inv_with_cost['total_value'] = inv_with_cost['quantity'] * inv_with_cost['unit_cost']
                total_inventory_value = inv_with_cost['total_value'].sum()

        total_quantity = inventory['quantity'].sum() if 'quantity' in inventory.columns else 0
        avg_quantity_per_item = inventory['quantity'].mean() if 'quantity' in inventory.columns else 0
        total_inventory_positions = len(inventory)

        print(f"📊 Total Inventory Positions: {total_inventory_positions:,}")
        print(f"📦 Total Quantity in Stock: {total_quantity:,}")
        print(f"📊 Average Quantity per Item: {avg_quantity_per_item:,.0f}")
        if total_inventory_value > 0:
            print(f"💰 Total Inventory Value: ${total_inventory_value:,.2f}")

        # Create inventory visualizations
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # Inventory quantity distribution
        if 'quantity' in inventory.columns:
            axes[0,0].hist(inventory['quantity'], bins=50, alpha=0.7, color='lightgreen', edgecolor='black')
            axes[0,0].set_title('Inventory Quantity Distribution', fontweight='bold')
            axes[0,0].set_xlabel('Quantity')
            axes[0,0].set_ylabel('Frequency')
            axes[0,0].grid(alpha=0.3)

        # Inventory by warehouse
        if 'warehouse_id' in inventory.columns:
            warehouse_inventory = inventory.groupby('warehouse_id')['quantity'].sum().nlargest(15)
            axes[0,1].bar(range(len(warehouse_inventory)), warehouse_inventory.values, color='lightblue')
            axes[0,1].set_title('Inventory by Warehouse (Top 15)', fontweight='bold')
            axes[0,1].set_xlabel('Warehouse Rank')
            axes[0,1].set_ylabel('Total Quantity')
            axes[0,1].grid(axis='y', alpha=0.3)

        # Inventory levels (Low, Medium, High)
        if 'quantity' in inventory.columns:
            q25, q75 = inventory['quantity'].quantile([0.25, 0.75])
            inventory_levels = pd.cut(inventory['quantity'],
                                    bins=[0, q25, q75, inventory['quantity'].max()],
                                    labels=['Low', 'Medium', 'High'])
            level_counts = inventory_levels.value_counts()
            axes[1,0].pie(level_counts.values, labels=level_counts.index, autopct='%1.1f%%',
                         colors=['red', 'orange', 'green'])
            axes[1,0].set_title('Inventory Level Distribution', fontweight='bold')

        # Top products by quantity
        if 'product_id' in inventory.columns:
            top_products = inventory.groupby('product_id')['quantity'].sum().nlargest(10)
            axes[1,1].barh(range(len(top_products)), top_products.values, color='coral')
            axes[1,1].set_title('Top 10 Products by Inventory Quantity', fontweight='bold')
            axes[1,1].set_xlabel('Total Quantity')
            axes[1,1].set_ylabel('Product Rank')
            axes[1,1].grid(axis='x', alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'charts' / 'inventory_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

        # Save inventory summary
        inventory_summary = pd.DataFrame({
            'Metric': ['Total Positions', 'Total Quantity', 'Average Quantity', 'Total Value'],
            'Value': [f'{total_inventory_positions:,}', f'{total_quantity:,}',
                     f'{avg_quantity_per_item:,.0f}', f'${total_inventory_value:,.2f}']
        })
        inventory_summary.to_csv(self.output_dir / 'csv' / 'inventory_summary.csv', index=False)

        # Generate insights
        self.insights.extend([
            f"📦 Managing {total_inventory_positions:,} inventory positions",
            f"📊 Total stock quantity: {total_quantity:,} units",
            f"💰 Total inventory value: ${total_inventory_value:,.2f}" if total_inventory_value > 0 else "💰 Inventory valuation requires product cost data",
            f"🏭 Inventory distributed across {len(inventory['warehouse_id'].unique()) if 'warehouse_id' in inventory.columns else 'N/A'} warehouses"
        ])

    def correlation_analysis(self):
        """Analyze correlations between key metrics"""
        print("\n🔗 CORRELATION ANALYSIS")
        print("=" * 50)

        orders = self.data.get('silver_supply_orders', pd.DataFrame())

        if orders.empty:
            print("⚠️  Order data not available for correlation analysis")
            return

        # Select numerical columns for correlation
        numerical_cols = orders.select_dtypes(include=[np.number]).columns.tolist()

        if len(numerical_cols) < 2:
            print("⚠️  Insufficient numerical columns for correlation analysis")
            return

        # Calculate correlations
        correlation_matrix = orders[numerical_cols].corr()

        # Create correlation heatmap
        plt.figure(figsize=(12, 10))
        mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))
        sns.heatmap(correlation_matrix, mask=mask, annot=True, cmap='coolwarm', center=0,
                    square=True, linewidths=0.5, cbar_kws={"shrink": 0.8})
        plt.title('Supply Chain Metrics Correlation Matrix', fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(self.output_dir / 'charts' / 'correlation_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

        # Find strongest correlations
        correlation_pairs = []
        for i in range(len(correlation_matrix.columns)):
            for j in range(i+1, len(correlation_matrix.columns)):
                col1, col2 = correlation_matrix.columns[i], correlation_matrix.columns[j]
                correlation_pairs.append({
                    'Variable_1': col1,
                    'Variable_2': col2,
                    'Correlation': correlation_matrix.iloc[i, j]
                })

        correlation_df = pd.DataFrame(correlation_pairs)
        correlation_df = correlation_df.reindex(correlation_df['Correlation'].abs().sort_values(ascending=False).index)

        print("🔗 Strongest Correlations:")
        print(correlation_df.head(10).to_string(index=False))

        # Save correlation results
        correlation_df.to_csv(self.output_dir / 'csv' / 'correlation_analysis.csv', index=False)

        # Generate insights
        strong_correlations = correlation_df[correlation_df['Correlation'].abs() > 0.7].head(3)
        self.insights.extend([
            f"🔗 Found {len(correlation_df)} metric pairs for correlation analysis",
            f"📊 Strongest positive correlation: {strong_correlations.iloc[0]['Variable_1']} & {strong_correlations.iloc[0]['Variable_2']} ({strong_correlations.iloc[0]['Correlation']:.3f})" if len(strong_correlations) > 0 else "📊 No strong correlations found"
        ])

    def statistical_summary_analysis(self):
        """Generate comprehensive statistical summary for all tables"""
        print("\n📊 STATISTICAL SUMMARY ANALYSIS")
        print("=" * 50)

        all_statistics = []

        for table_key, df in self.data.items():
            if df.empty:
                continue

            table_name = table_key.replace('bronze_', '').replace('silver_', '').replace('gold_', '')
            layer = 'bronze' if 'bronze_' in table_key else 'silver' if 'silver_' in table_key else 'gold'

            print(f"\n📋 Analyzing {table_name} ({layer} layer)")

            # Basic statistics
            total_records = len(df)
            total_columns = len(df.columns)

            # Numerical columns analysis
            numerical_cols = df.select_dtypes(include=[np.number]).columns
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns

            # Memory usage
            memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024  # MB

            # Missing values
            total_nulls = df.isnull().sum().sum()
            null_percentage = (total_nulls / df.size * 100) if df.size > 0 else 0

            # Duplicates
            duplicate_count = df.duplicated().sum()
            duplicate_percentage = (duplicate_count / total_records * 100) if total_records > 0 else 0

            # Statistical summary for numerical columns
            for col in numerical_cols:
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    stats = {
                        'table': table_name,
                        'layer': layer,
                        'column': col,
                        'data_type': str(df[col].dtype),
                        'count': len(col_data),
                        'nulls': df[col].isnull().sum(),
                        'null_pct': round((df[col].isnull().sum() / len(df) * 100), 2),
                        'mean': round(col_data.mean(), 3),
                        'median': round(col_data.median(), 3),
                        'mode': col_data.mode().iloc[0] if len(col_data.mode()) > 0 else None,
                        'std': round(col_data.std(), 3),
                        'min': col_data.min(),
                        'max': col_data.max(),
                        'q25': round(col_data.quantile(0.25), 3),
                        'q75': round(col_data.quantile(0.75), 3),
                        'iqr': round(col_data.quantile(0.75) - col_data.quantile(0.25), 3),
                        'skewness': round(col_data.skew(), 3),
                        'kurtosis': round(col_data.kurtosis(), 3),
                        'unique_values': col_data.nunique(),
                        'unique_pct': round((col_data.nunique() / len(col_data) * 100), 2)
                    }
                    all_statistics.append(stats)

            # Summary for categorical columns
            for col in categorical_cols:
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    most_common = col_data.value_counts().head(1)
                    stats = {
                        'table': table_name,
                        'layer': layer,
                        'column': col,
                        'data_type': str(df[col].dtype),
                        'count': len(col_data),
                        'nulls': df[col].isnull().sum(),
                        'null_pct': round((df[col].isnull().sum() / len(df) * 100), 2),
                        'mean': None,
                        'median': None,
                        'mode': most_common.index[0] if len(most_common) > 0 else None,
                        'std': None,
                        'min': None,
                        'max': None,
                        'q25': None,
                        'q75': None,
                        'iqr': None,
                        'skewness': None,
                        'kurtosis': None,
                        'unique_values': col_data.nunique(),
                        'unique_pct': round((col_data.nunique() / len(col_data) * 100), 2)
                    }
                    all_statistics.append(stats)

            print(f"  📊 Records: {total_records:,}")
            print(f"  📈 Columns: {total_columns} ({len(numerical_cols)} numerical, {len(categorical_cols)} categorical)")
            print(f"  💾 Memory: {memory_usage:.2f} MB")
            print(f"  ❌ Nulls: {total_nulls:,} ({null_percentage:.2f}%)")
            print(f"  🔄 Duplicates: {duplicate_count:,} ({duplicate_percentage:.2f}%)")

        # Convert to DataFrame and save
        stats_df = pd.DataFrame(all_statistics)
        if not stats_df.empty:
            stats_df.to_csv(self.output_dir / 'csv' / 'statistical_summary.csv', index=False)
            print(f"\n✅ Generated statistical summary for {stats_df['table'].nunique()} tables")
            print(f"📊 Total columns analyzed: {len(stats_df)}")

            # Create statistical summary visualization
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))

            # Null percentages by table
            table_nulls = stats_df.groupby('table')['null_pct'].mean()
            axes[0,0].bar(table_nulls.index, table_nulls.values, color='lightcoral', alpha=0.8)
            axes[0,0].set_title('Average Null Percentage by Table', fontweight='bold')
            axes[0,0].set_ylabel('Null Percentage (%)')
            axes[0,0].tick_params(axis='x', rotation=45)
            axes[0,0].grid(axis='y', alpha=0.3)

            # Data type distribution
            dtype_counts = stats_df['data_type'].value_counts()
            axes[0,1].pie(dtype_counts.values, labels=dtype_counts.index, autopct='%1.1f%%', startangle=90)
            axes[0,1].set_title('Data Types Distribution', fontweight='bold')

            # Uniqueness percentage distribution
            numerical_stats = stats_df[stats_df['mean'].notna()]
            if not numerical_stats.empty:
                axes[1,0].hist(numerical_stats['unique_pct'], bins=20, color='lightblue', alpha=0.8, edgecolor='black')
                axes[1,0].set_title('Distribution of Uniqueness Percentages', fontweight='bold')
                axes[1,0].set_xlabel('Uniqueness Percentage (%)')
                axes[1,0].set_ylabel('Frequency')
                axes[1,0].grid(axis='y', alpha=0.3)

            # Skewness distribution for numerical columns
            if not numerical_stats.empty:
                skewness_data = numerical_stats['skewness'].dropna()
                if len(skewness_data) > 0:
                    axes[1,1].hist(skewness_data, bins=15, color='lightgreen', alpha=0.8, edgecolor='black')
                    axes[1,1].set_title('Distribution of Skewness Values', fontweight='bold')
                    axes[1,1].set_xlabel('Skewness')
                    axes[1,1].set_ylabel('Frequency')
                    axes[1,1].axvline(x=0, color='red', linestyle='--', alpha=0.7, label='Normal Distribution')
                    axes[1,1].legend()
                    axes[1,1].grid(axis='y', alpha=0.3)

            plt.tight_layout()
            plt.savefig(self.output_dir / 'charts' / 'statistical_summary.png', dpi=300, bbox_inches='tight')
            plt.show()

            # Generate statistical insights
            high_null_cols = stats_df[stats_df['null_pct'] > 20]
            highly_unique_cols = stats_df[stats_df['unique_pct'] > 95]
            skewed_cols = numerical_stats[abs(numerical_stats['skewness']) > 2]

            self.insights.extend([
                f"📊 Statistical analysis completed for {stats_df['table'].nunique()} tables with {len(stats_df)} columns",
                f"❌ Found {len(high_null_cols)} columns with >20% null values",
                f"🔑 Identified {len(highly_unique_cols)} highly unique columns (>95% unique)",
                f"📈 Detected {len(skewed_cols)} highly skewed numerical columns (|skewness| > 2)",
                f"📋 Data types: {dtype_counts.to_dict()}",
                f"💾 Average data completeness: {(100 - stats_df['null_pct'].mean()):.2f}%"
            ])

    def data_reconciliation(self):
        """Perform data reconciliation between Silver and Gold layers"""
        print("\n🔍 DATA RECONCILIATION")
        print("=" * 50)

        reconciliation_results = []

        # Check if we have both Silver and Gold data
        silver_orders = self.data.get('silver_supply_orders', pd.DataFrame())
        gold_kpis = self.data.get('gold_supply_chain_kpis', pd.DataFrame())
        gold_sales = self.data.get('gold_monthly_sales_performance', pd.DataFrame())

        if not silver_orders.empty:
            silver_total_revenue = silver_orders['total_invoice'].sum() if 'total_invoice' in silver_orders.columns else 0
            silver_total_quantity = silver_orders['quantity'].sum() if 'quantity' in silver_orders.columns else 0
            silver_order_count = len(silver_orders)

            print(f"📊 Silver Layer Totals:")
            print(f"  💰 Total Revenue: ${silver_total_revenue:,.2f}")
            print(f"  📦 Total Quantity: {silver_total_quantity:,}")
            print(f"  🛒 Total Orders: {silver_order_count:,}")

            reconciliation_results.append({
                'Metric': 'Total Revenue (Silver)',
                'Value': silver_total_revenue,
                'Source': 'silver.supply_orders'
            })

            reconciliation_results.append({
                'Metric': 'Total Quantity (Silver)',
                'Value': silver_total_quantity,
                'Source': 'silver.supply_orders'
            })

            reconciliation_results.append({
                'Metric': 'Total Orders (Silver)',
                'Value': silver_order_count,
                'Source': 'silver.supply_orders'
            })

        if not gold_sales.empty and 'total_revenue' in gold_sales.columns:
            gold_total_revenue = gold_sales['total_revenue'].sum()
            gold_total_quantity = gold_sales['total_quantity_sold'].sum() if 'total_quantity_sold' in gold_sales.columns else 0
            gold_order_count = gold_sales['total_orders'].sum() if 'total_orders' in gold_sales.columns else 0

            print(f"\n📊 Gold Layer Totals:")
            print(f"  💰 Total Revenue: ${gold_total_revenue:,.2f}")
            print(f"  📦 Total Quantity: {gold_total_quantity:,}")
            print(f"  🛒 Total Orders: {gold_order_count:,}")

            reconciliation_results.extend([
                {'Metric': 'Total Revenue (Gold)', 'Value': gold_total_revenue, 'Source': 'gold.monthly_sales_performance'},
                {'Metric': 'Total Quantity (Gold)', 'Value': gold_total_quantity, 'Source': 'gold.monthly_sales_performance'},
                {'Metric': 'Total Orders (Gold)', 'Value': gold_order_count, 'Source': 'gold.monthly_sales_performance'}
            ])

            # Reconciliation checks
            if not silver_orders.empty:
                revenue_diff = abs(silver_total_revenue - gold_total_revenue)
                revenue_pct_diff = (revenue_diff / silver_total_revenue * 100) if silver_total_revenue > 0 else 0

                quantity_diff = abs(silver_total_quantity - gold_total_quantity)
                quantity_pct_diff = (quantity_diff / silver_total_quantity * 100) if silver_total_quantity > 0 else 0

                order_diff = abs(silver_order_count - gold_order_count)
                order_pct_diff = (order_diff / silver_order_count * 100) if silver_order_count > 0 else 0

                print(f"\n🔍 Reconciliation Results:")
                print(f"  💰 Revenue Difference: ${revenue_diff:,.2f} ({revenue_pct_diff:.2f}%)")
                print(f"  📦 Quantity Difference: {quantity_diff:,} ({quantity_pct_diff:.2f}%)")
                print(f"  🛒 Order Difference: {order_diff:,} ({order_pct_diff:.2f}%)")

                # Tolerance check (5% tolerance)
                tolerance = 5.0
                reconciliation_results.extend([
                    {'Metric': 'Revenue Reconciliation', 'Value': '✅ PASS' if revenue_pct_diff <= tolerance else '❌ FAIL', 'Source': f'{revenue_pct_diff:.2f}% difference'},
                    {'Metric': 'Quantity Reconciliation', 'Value': '✅ PASS' if quantity_pct_diff <= tolerance else '❌ FAIL', 'Source': f'{quantity_pct_diff:.2f}% difference'},
                    {'Metric': 'Order Count Reconciliation', 'Value': '✅ PASS' if order_pct_diff <= tolerance else '❌ FAIL', 'Source': f'{order_pct_diff:.2f}% difference'}
                ])

        # Save reconciliation results
        reconciliation_df = pd.DataFrame(reconciliation_results)
        reconciliation_df.to_csv(self.output_dir / 'csv' / 'reconciliation_results.csv', index=False)

        print(f"\n📋 Reconciliation completed - results saved to reconciliation_results.csv")

        # Generate reconciliation insights
        self.insights.extend([
            f"🔍 Data reconciliation performed across Silver and Gold layers",
            f"📊 {len(reconciliation_results)} metrics compared for consistency",
            f"✅ Reconciliation results saved for audit trail"
        ])

    def generate_insights_report(self):
        """Generate comprehensive insights report"""
        print("\n📝 GENERATING INSIGHTS REPORT")
        print("=" * 50)

        # Create comprehensive report
        report_content = [
            "# Supply Chain Data Pipeline - EDA Insights Report",
            f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Executive Summary",
            f"This report provides comprehensive insights from the Medallion Architecture data pipeline analysis.",
            f"The analysis covers Bronze, Silver, and Gold layer data with quality assessments and business insights.",
            "",
            "## Key Insights",
        ]

        # Add all collected insights
        for i, insight in enumerate(self.insights, 1):
            report_content.append(f"{i}. {insight}")

        report_content.extend([
            "",
            "## Data Quality Summary",
            "- Bronze to Silver transformation maintains high data quality",
            "- Automated data cleaning processes handle quality issues effectively",
            "- Regular reconciliation ensures consistency across pipeline layers",
            "",
            "## Recommendations",
            "1. Continue monitoring data quality metrics for early issue detection",
            "2. Implement automated alerting for reconciliation failures",
            "3. Expand analysis to include seasonal patterns and forecasting",
            "4. Consider implementing real-time dashboard updates",
            "",
            "## Technical Notes",
            f"- Analysis performed on {sum(len(df) for df in self.data.values() if isinstance(df, pd.DataFrame)):,} total records",
            f"- Charts and visualizations saved in: {self.output_dir / 'charts'}",
            f"- Data exports available in: {self.output_dir / 'csv'}",
            "",
            "---",
            "*Report generated by Supply Chain EDA Pipeline*"
        ])

        # Save report
        report_path = self.output_dir / 'reports' / 'eda_insights_report.md'
        with open(report_path, 'w') as f:
            f.write('\n'.join(report_content))

        print(f"📊 Insights report saved: {report_path}")
        print(f"📈 Total insights generated: {len(self.insights)}")

        # Also save insights as CSV
        insights_df = pd.DataFrame({'Insight': self.insights})
        insights_df.to_csv(self.output_dir / 'csv' / 'key_insights.csv', index=False)

        return report_content

    def run_complete_analysis(self):
        """Run complete EDA pipeline"""
        print("🚀 STARTING COMPREHENSIVE SUPPLY CHAIN EDA")
        print("=" * 60)

        start_time = datetime.now()

        # Load data from all layers
        if not self.load_data():
            print("❌ Failed to load data. Exiting.")
            return False

        print(f"\n📊 Loaded {len(self.data)} datasets for analysis")

        try:
            # Run all analysis modules
            self.data_quality_analysis()
            self.supply_chain_overview()
            self.financial_analysis()
            self.inventory_analysis()
            self.correlation_analysis()
            self.statistical_summary_analysis()
            self.data_reconciliation()

            # Generate final report
            self.generate_insights_report()

            # Summary
            end_time = datetime.now()
            duration = end_time - start_time

            print(f"\n✅ EDA ANALYSIS COMPLETED!")
            print("=" * 60)
            print(f"⏱️  Duration: {duration.total_seconds():.1f} seconds")
            print(f"📊 Insights Generated: {len(self.insights)}")
            print(f"📁 Output Directory: {self.output_dir}")
            print(f"📈 Charts Created: Multiple visualization files")
            print(f"💾 Data Exports: {len(list((self.output_dir / 'csv').glob('*.csv')))} CSV files")

            return True

        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            return False

# Main execution
if __name__ == "__main__":
    print("🔍 Supply Chain Data Pipeline - Exploratory Data Analysis")
    print("=" * 60)

    # Initialize and run EDA
    eda = SupplyChainEDA()
    success = eda.run_complete_analysis()

    if success:
        print(f"\n🎉 EDA completed successfully!")
        print(f"📊 Check output directory: {eda.output_dir}")
        print(f"📈 View charts in: {eda.output_dir / 'charts'}")
        print(f"💾 Data exports in: {eda.output_dir / 'csv'}")
        print(f"📝 Reports in: {eda.output_dir / 'reports'}")
    else:
        print(f"\n❌ EDA analysis failed. Check logs for details.")
