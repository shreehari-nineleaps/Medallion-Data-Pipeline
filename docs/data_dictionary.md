# Data Dictionary - Medallion Data Pipeline

## Overview
This document provides comprehensive definitions for all tables, columns, and data structures used in the Supply Chain Medallion Data Pipeline.

**Pipeline Architecture**: Bronze → Silver → Gold  
**Database**: PostgreSQL  
**Generated**: 2025-08-27  
**Version**: 1.0  

---

## Bronze Layer (Raw Data)
*Raw data ingested directly from Google Sheets sources*

### bronze.suppliers
| Column | Data Type | Description | Source | Constraints |
|--------|-----------|-------------|---------|-------------|
| supplier_id | INT | Unique supplier identifier | Google Sheets | PRIMARY KEY |
| supplier_name | TEXT | Legal name of supplier company | Google Sheets | NOT NULL |
| contact_email | TEXT | Primary contact email address | Google Sheets | - |
| phone_number | TEXT | Primary phone number | Google Sheets | - |

**Record Count**: ~50,000  
**Data Quality**: Raw, unvalidated data from source

### bronze.products
| Column | Data Type | Description | Source | Constraints |
|--------|-----------|-------------|---------|-------------|
| product_id | INT | Unique product identifier | Google Sheets | PRIMARY KEY |
| product_name | TEXT | Product display name | Google Sheets | - |
| unit_cost | NUMERIC(15,6) | Cost per unit from supplier | Google Sheets | - |
| selling_price | NUMERIC(15,6) | Retail selling price | Google Sheets | - |
| supplier_id | INT | Reference to supplier | Google Sheets | - |
| product_category | TEXT | Product category classification | Google Sheets | - |
| status | TEXT | Product status (active/discontinued) | Google Sheets | - |

**Record Count**: ~50,000  
**Data Quality**: Raw pricing and product information

### bronze.warehouses
| Column | Data Type | Description | Source | Constraints |
|--------|-----------|-------------|---------|-------------|
| warehouse_id | INT | Unique warehouse identifier | Google Sheets | PRIMARY KEY |
| warehouse_name | TEXT | Warehouse facility name | Google Sheets | - |
| city | TEXT | City location | Google Sheets | - |
| region | TEXT | Regional classification | Google Sheets | - |
| storage_capacity | INT | Maximum storage capacity | Google Sheets | - |

**Record Count**: ~50,000  
**Data Quality**: Raw facility information

### bronze.inventory
| Column | Data Type | Description | Source | Constraints |
|--------|-----------|-------------|---------|-------------|
| inventory_id | INT | Unique inventory record ID | Google Sheets | PRIMARY KEY |
| product_id | TEXT | Product reference (as text) | Google Sheets | - |
| warehouse_id | TEXT | Warehouse reference (as text) | Google Sheets | - |
| quantity_on_hand | TEXT | Current inventory quantity (as text) | Google Sheets | - |
| last_stocked_date | TEXT | Last restocking date (as text) | Google Sheets | - |

**Record Count**: ~50,000  
**Data Quality**: Raw inventory data with text formatting

### bronze.retail_stores
| Column | Data Type | Description | Source | Constraints |
|--------|-----------|-------------|---------|-------------|
| retail_store_id | INT | Unique store identifier | Google Sheets | PRIMARY KEY |
| store_name | TEXT | Store display name | Google Sheets | - |
| city | TEXT | Store city location | Google Sheets | - |
| region | TEXT | Regional classification | Google Sheets | - |
| store_type | TEXT | Store format type | Google Sheets | - |
| store_status | TEXT | Operational status | Google Sheets | - |

**Record Count**: ~50,000  
**Data Quality**: Raw retail location data

### bronze.supply_orders
| Column | Data Type | Description | Source | Constraints |
|--------|-----------|-------------|---------|-------------|
| supply_order_id | INT | Unique order identifier | Google Sheets | PRIMARY KEY |
| product_id | TEXT | Product reference (as text) | Google Sheets | - |
| warehouse_id | TEXT | Destination warehouse (as text) | Google Sheets | - |
| retail_store_id | TEXT | Retail store reference (as text) | Google Sheets | - |
| quantity | TEXT | Order quantity (as text) | Google Sheets | - |
| price | TEXT | Unit price (as text) | Google Sheets | - |
| total_invoice | TEXT | Total order value (as text) | Google Sheets | - |
| order_date | TEXT | Order placement date (as text) | Google Sheets | - |
| shipped_date | TEXT | Shipment date (as text) | Google Sheets | - |
| delivered_date | TEXT | Delivery date (as text) | Google Sheets | - |
| status | TEXT | Order status | Google Sheets | - |

**Record Count**: ~50,000  
**Data Quality**: Raw transactional data with text formatting

---

## Silver Layer (Cleaned Data)
*Validated, cleaned, and standardized data with quality controls*

### silver.suppliers
| Column | Data Type | Description | Business Rules | Constraints |
|--------|-----------|-------------|---------------|-------------|
| supplier_id | INT | Unique supplier identifier | Must be positive integer | PRIMARY KEY |
| supplier_name | TEXT | Validated supplier company name | Trimmed, proper case | NOT NULL |
| contact_email | TEXT | Validated email address | Email format validation | - |
| phone_number | TEXT | Standardized phone number | Format standardized | - |
| quality_score | DECIMAL(5,2) | Data quality score (0-100) | Calculated metric | - |
| created_at | TIMESTAMP | Record creation timestamp | Auto-generated | DEFAULT CURRENT_TIMESTAMP |

**Record Count**: ~50,000  
**Data Quality**: 99%+ quality rate after cleaning

### silver.products
| Column | Data Type | Description | Business Rules | Constraints |
|--------|-----------|-------------|---------------|-------------|
| product_id | INT | Unique product identifier | Must be positive integer | PRIMARY KEY |
| product_name | TEXT | Validated product name | Trimmed, proper formatting | NOT NULL |
| unit_cost | DECIMAL(15,4) | Cost per unit (USD) | Must be >= 0 | CHECK (unit_cost >= 0) |
| selling_price | DECIMAL(15,4) | Retail price (USD) | Must be >= unit_cost | CHECK (selling_price >= 0) |
| supplier_id | INT | Reference to valid supplier | FK validation | REFERENCES suppliers |
| product_category | TEXT | Standardized category | Enum validation | DEFAULT 'Uncategorized' |
| status | TEXT | Product lifecycle status | active/discontinued only | DEFAULT 'active' |
| price_margin | DECIMAL(15,4) | Profit margin (selling - cost) | Calculated field | - |
| quality_score | DECIMAL(5,2) | Data quality score | 0-100 scale | - |
| created_at | TIMESTAMP | Record creation timestamp | Auto-generated | DEFAULT CURRENT_TIMESTAMP |

**Record Count**: ~50,000  
**Business Logic**: Automated profit margin calculation, category standardization

### silver.warehouses
| Column | Data Type | Description | Business Rules | Constraints |
|--------|-----------|-------------|---------------|-------------|
| warehouse_id | INT | Unique warehouse identifier | Must be positive integer | PRIMARY KEY |
| warehouse_name | TEXT | Validated facility name | Trimmed, standardized | NOT NULL |
| city | TEXT | City location | Proper case formatting | - |
| region | TEXT | Standardized region code | Enum validation | - |
| storage_capacity | INT | Maximum capacity (units) | Must be positive | CHECK (storage_capacity >= 0) |
| quality_score | DECIMAL(5,2) | Data quality score | 0-100 scale | - |
| created_at | TIMESTAMP | Record creation timestamp | Auto-generated | DEFAULT CURRENT_TIMESTAMP |

**Record Count**: ~50,000  
**Business Logic**: Regional standardization, capacity validation

### silver.inventory
| Column | Data Type | Description | Business Rules | Constraints |
|--------|-----------|-------------|---------------|-------------|
| inventory_id | INT | Unique inventory record ID | Must be positive integer | PRIMARY KEY |
| product_id | INT | Reference to valid product | FK validation | REFERENCES products |
| warehouse_id | INT | Reference to valid warehouse | FK validation | REFERENCES warehouses |
| quantity_on_hand | INT | Current stock quantity | Must be >= 0 | CHECK (quantity_on_hand >= 0) |
| last_stocked_date | DATE | Last restocking date | Valid date format | - |
| quality_score | DECIMAL(5,2) | Data quality score | 0-100 scale | - |
| created_at | TIMESTAMP | Record creation timestamp | Auto-generated | DEFAULT CURRENT_TIMESTAMP |

**Record Count**: ~48,873 (1,127 rejected for quality issues)  
**Business Logic**: Foreign key validation, negative quantity rejection

### silver.retail_stores
| Column | Data Type | Description | Business Rules | Constraints |
|--------|-----------|-------------|---------------|-------------|
| retail_store_id | INT | Unique store identifier | Must be positive integer | PRIMARY KEY |
| store_name | TEXT | Validated store name | Trimmed, standardized | NOT NULL |
| city | TEXT | Store city location | Proper case formatting | - |
| region | TEXT | Standardized region code | Enum validation | - |
| store_type | TEXT | Store format classification | Enum validation | - |
| store_status | TEXT | Operational status | active/inactive only | DEFAULT 'active' |
| quality_score | DECIMAL(5,2) | Data quality score | 0-100 scale | - |
| created_at | TIMESTAMP | Record creation timestamp | Auto-generated | DEFAULT CURRENT_TIMESTAMP |

**Record Count**: ~50,000  
**Business Logic**: Store type standardization, status validation

### silver.supply_orders
| Column | Data Type | Description | Business Rules | Constraints |
|--------|-----------|-------------|---------------|-------------|
| supply_order_id | INT | Unique order identifier | Must be positive integer | PRIMARY KEY |
| product_id | INT | Reference to valid product | FK validation | REFERENCES products |
| warehouse_id | INT | Destination warehouse | FK validation | REFERENCES warehouses |
| retail_store_id | INT | Store reference | FK validation | REFERENCES retail_stores |
| quantity | INT | Order quantity | Must be > 0 | CHECK (quantity >= 0) |
| price | DECIMAL(15,4) | Unit price at time of order | Must be >= 0 | CHECK (price >= 0) |
| total_invoice | DECIMAL(15,4) | Total order value (price × quantity) | Calculated and validated | CHECK (total_invoice >= 0) |
| order_date | DATE | Order placement date | Valid date, not future | NOT NULL |
| shipped_date | DATE | Shipment date | Must be >= order_date | - |
| delivered_date | DATE | Delivery completion date | Must be >= shipped_date | - |
| status | TEXT | Current order status | Enum validation | DEFAULT 'pending' |
| is_calculation_correct | BOOLEAN | Invoice calculation validation | price × quantity = total_invoice | - |
| date_logic_valid | BOOLEAN | Date sequence validation | order ≤ shipped ≤ delivered | - |
| quality_score | DECIMAL(5,2) | Data quality score | 0-100 scale | - |
| created_at | TIMESTAMP | Record creation timestamp | Auto-generated | DEFAULT CURRENT_TIMESTAMP |

**Record Count**: ~50,000  
**Business Logic**: Comprehensive validation of calculations, dates, and references

---

## Gold Layer (Analytics-Ready Data)
*Aggregated business intelligence tables optimized for reporting*

### gold.monthly_sales_performance
| Column | Data Type | Description | Aggregation Logic | Business Context |
|--------|-----------|-------------|------------------|------------------|
| sales_month | DATE | Month of sales activity | DATE_TRUNC('month', order_date) | Monthly reporting period |
| region | TEXT | Regional classification | Standardized region codes | Geographic analysis |
| store_type | TEXT | Store format type | Standardized store types | Channel analysis |
| product_category | TEXT | Product category | Standardized categories | Category performance |
| total_orders | INT | Count of orders in period | COUNT(DISTINCT supply_order_id) | Volume metrics |
| total_quantity_sold | INT | Sum of quantities | SUM(quantity) | Units moved |
| total_revenue | DECIMAL(15,4) | Sum of order values | SUM(total_invoice) | Revenue metrics |
| avg_order_value | DECIMAL(15,4) | Average order size | AVG(total_invoice) | Order size analysis |
| unique_customers | INT | Count of distinct stores | COUNT(DISTINCT retail_store_id) | Customer reach |

**Record Count**: ~28,475  
**Grain**: One row per month/region/store_type/category combination  
**Purpose**: Monthly business performance analysis and trending

### gold.inventory_health_metrics
| Column | Data Type | Description | Calculation Logic | Business Context |
|--------|-----------|-------------|------------------|------------------|
| region | TEXT | Warehouse region | From warehouses dimension | Regional inventory view |
| city | TEXT | Warehouse city | From warehouses dimension | Location-specific analysis |
| warehouse_name | TEXT | Warehouse facility name | From warehouses dimension | Facility-level metrics |
| product_category | TEXT | Product category | From products dimension | Category inventory health |
| product_count | INT | Distinct products in stock | COUNT(DISTINCT product_id) | SKU diversity |
| total_stock_quantity | INT | Sum of inventory quantities | SUM(quantity_on_hand) | Total units in stock |
| total_stock_value | DECIMAL(15,4) | Inventory value at cost | SUM(quantity × unit_cost) | Financial exposure |
| avg_stock_per_product | DECIMAL(15,4) | Average stock level | total_stock_quantity / product_count | Stock distribution |
| stock_utilization_pct | DECIMAL(5,2) | Capacity utilization | (total_stock / capacity) × 100 | Efficiency metric |
| low_stock_items | INT | Items below threshold | COUNT WHERE quantity ≤ 50 | Risk indicator |

**Record Count**: ~48,494  
**Grain**: One row per warehouse/category combination  
**Purpose**: Inventory optimization and risk management

### gold.supplier_performance_monthly
| Column | Data Type | Description | Calculation Logic | Business Context |
|--------|-----------|-------------|------------------|------------------|
| month | DATE | Performance month | DATE_TRUNC('month', order_date) | Monthly evaluation period |
| supplier_id | INT | Supplier identifier | From suppliers dimension | Supplier tracking |
| supplier_name | TEXT | Supplier company name | From suppliers dimension | Supplier identification |
| total_orders | INT | Orders placed with supplier | COUNT(DISTINCT supply_order_id) | Volume relationship |
| total_units | INT | Units ordered | SUM(quantity) | Volume metrics |
| total_revenue | DECIMAL(15,4) | Revenue generated | SUM(total_invoice) | Financial relationship |
| avg_lead_time_days | DECIMAL(5,2) | Average delivery time | AVG(delivered_date - order_date) | Performance metric |
| fulfilled_orders | INT | Successfully fulfilled orders | COUNT WHERE status IN ('delivered', 'shipped') | Reliability metric |
| delivered_orders | INT | Completed deliveries | COUNT WHERE status = 'delivered' | Completion rate |
| on_time_rate_pct | DECIMAL(5,2) | On-time delivery percentage | (on_time_orders / total_orders) × 100 | Timeliness KPI |
| in_full_rate_pct | DECIMAL(5,2) | Complete order percentage | (in_full_orders / total_orders) × 100 | Accuracy KPI |
| otif_proxy_pct | DECIMAL(5,2) | On-Time In-Full proxy | Combined performance indicator | Overall performance |
| created_at | TIMESTAMP | Record creation time | Auto-generated | Data lineage |

**Record Count**: ~35,480  
**Grain**: One row per supplier per month  
**Purpose**: Supplier relationship management and performance evaluation

### gold.supply_chain_dashboard
| Column | Data Type | Description | Source Logic | Dashboard Usage |
|--------|-----------|-------------|-------------|-----------------|
| supply_order_id | INT | Unique order identifier | Direct from silver.supply_orders | Transaction drill-down |
| order_date | DATE | Order placement date | Direct from silver.supply_orders | Time-based filtering |
| shipped_date | DATE | Shipment date | Direct from silver.supply_orders | Logistics tracking |
| delivered_date | DATE | Delivery date | Direct from silver.supply_orders | Performance measurement |
| order_status | TEXT | Current order status | Direct from silver.supply_orders | Status monitoring |
| product_name | TEXT | Product description | JOIN with products | Product analysis |
| product_category | TEXT | Product category | JOIN with products | Category performance |
| supplier_name | TEXT | Supplier company | JOIN through products to suppliers | Supplier analysis |
| warehouse_name | TEXT | Destination warehouse | JOIN with warehouses | Logistics view |
| region | TEXT | Regional classification | From warehouse dimension | Geographic analysis |
| store_name | TEXT | Destination store | JOIN with retail_stores | Customer analysis |
| store_type | TEXT | Store format | From retail_stores | Channel analysis |
| quantity | INT | Order quantity | Direct from silver.supply_orders | Volume analysis |
| price | DECIMAL(15,4) | Unit price | Direct from silver.supply_orders | Pricing analysis |
| total_revenue | DECIMAL(15,4) | Order total value | Direct from silver.supply_orders | Revenue tracking |
| unit_cost | DECIMAL(15,4) | Product cost | FROM products dimension | Cost analysis |
| profit_margin | DECIMAL(15,4) | Order profit | (price - unit_cost) × quantity | Profitability |
| delivery_days | INT | Days from order to delivery | delivered_date - order_date | Performance metric |
| is_shipped | BOOLEAN | Shipment status flag | status = 'shipped' | Logistics KPI |
| is_delivered | BOOLEAN | Delivery status flag | status = 'delivered' | Completion KPI |
| is_canceled | BOOLEAN | Cancellation flag | status = 'canceled' | Risk indicator |
| delivery_performance | TEXT | Performance rating | CASE WHEN delivery_days ≤ 3 THEN 'Excellent'... | Performance categorization |

**Record Count**: ~50,000  
**Grain**: One row per supply order (transaction level)  
**Purpose**: Executive dashboard and operational reporting

### gold.forecasts
| Column | Data Type | Description | Source | Forecast Context |
|--------|-----------|-------------|--------|------------------|
| ds | DATE | Forecast date | LightGBM model output | Future date prediction |
| yhat | DECIMAL(15,4) | Predicted demand | Model prediction | Expected demand |
| yhat_lower | DECIMAL(15,4) | Lower confidence bound | Model confidence interval | Risk assessment |
| yhat_upper | DECIMAL(15,4) | Upper confidence bound | Model confidence interval | Upside planning |
| level | TEXT | Forecast granularity | Model configuration | Analysis level |
| entity_id | TEXT | Entity identifier | Model input | Forecast subject |
| model | TEXT | Model type used | 'lgbm' (LightGBM) | Model attribution |
| granularity | TEXT | Time granularity | 'weekly' | Forecast frequency |

**Record Count**: ~141,588  
**Grain**: Weekly forecasts by entity and level  
**Purpose**: Demand planning and inventory optimization

---

## Audit Tables

### audit.rejected_rows
| Column | Data Type | Description | Purpose |
|--------|-----------|-------------|---------|
| table_name | TEXT | Source table name | Track rejection source |
| rejected_count | INT | Count of rejected records | Volume tracking |
| rejection_reason | TEXT | Reason for rejection | Quality analysis |
| created_at | TIMESTAMP | Rejection timestamp | Audit trail |

### audit.dq_results
| Column | Data Type | Description | Purpose |
|--------|-----------|-------------|---------|
| table_name | TEXT | Table being checked | Quality scope |
| check_name | TEXT | Data quality check name | Check identification |
| check_result | TEXT | Pass/Fail status | Quality status |
| issues_found | INT | Count of issues | Quality metrics |
| created_at | TIMESTAMP | Check execution time | Audit timeline |

### audit.etl_log
| Column | Data Type | Description | Purpose |
|--------|-----------|-------------|---------|
| run_id | TEXT | ETL execution identifier | Run tracking |
| layer | TEXT | Pipeline layer (bronze/silver/gold) | Process tracking |
| table_name | TEXT | Table being processed | Granular tracking |
| status | TEXT | Success/Failure status | Process monitoring |
| rows_processed | INT | Count of rows processed | Volume metrics |
| duration_seconds | INT | Processing time | Performance metrics |
| created_at | TIMESTAMP | Log entry timestamp | Process timeline |

---

## Data Quality Standards

### Silver Layer Quality Rules
1. **Primary Keys**: Must be unique and non-null
2. **Foreign Keys**: Must reference valid records in parent tables
3. **Data Types**: All fields properly typed and validated
4. **Business Rules**: Prices ≥ 0, dates in logical sequence
5. **Calculations**: Invoice totals must equal price × quantity
6. **Enumerations**: Status fields limited to valid values

### Gold Layer Aggregation Rules
1. **Completeness**: All source records included in aggregations
2. **Accuracy**: Totals reconcile with Silver layer
3. **Consistency**: Same business rules across all aggregations
4. **Timeliness**: Updated with each ETL run
5. **Granularity**: Appropriate level for business analysis

### Data Lineage
- **Bronze → Silver**: 1:1 or 1:0 (rejected) transformation
- **Silver → Gold**: Many:1 aggregation with full auditability
- **Reconciliation**: Automated validation between layers
- **Change Tracking**: Full audit log of all modifications

---

## Usage Guidelines

### For Analysts
- Use **Silver** tables for detailed operational analysis
- Use **Gold** tables for executive reporting and dashboards
- Reference **Audit** tables for data quality assessment

### For Dashboard Developers
- **gold.supply_chain_dashboard**: Primary table for operational dashboards
- **gold.monthly_sales_performance**: Time-series and trend analysis
- **gold.forecasts**: Predictive analytics and planning

### For Data Engineers
- Monitor **audit** tables for pipeline health
- Use **quality_score** fields to identify data issues
- Follow **created_at** timestamps for processing sequence

---

**Document Version**: 1.0  
**Last Updated**: 2025-08-27  
**Maintained By**: Data Engineering Team  
**Contact**: pipeline-support@company.com