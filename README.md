# Medallion Data Pipeline

A clean, efficient ETL pipeline implementing the **Medallion Architecture** (Bronze, Silver, Gold layers) that extracts supplier data from Google Sheets and loads it into PostgreSQL.

## 🏗️ Architecture Overview

### Medallion Layers
- **🥉 Bronze Layer**: Raw data ingestion from Google Sheets (250K+ records)
- **🥈 Silver Layer**: Cleaned and validated data transformations (Ready for development)
- **🥇 Gold Layer**: Business analytics and KPIs (Ready for development)

### Data Flow
```
Google Sheets → Bronze (Raw) → Silver (Cleaned) → Gold (Analytics)
```

## 📁 Project Structure

```
Medallion-Data-Pipeline/
├── bronze/
│   ├── database_setup.py      # Setup bronze layer database schema
│   └── data_loader.py         # Load raw data from Google Sheets
├── silver/                    # Cleaned & validated data (future)
│   └── README.md             # Silver layer documentation
├── gold/                      # Business analytics (future)  
│   └── README.md             # Gold layer documentation
├── logs/                      # Pipeline execution logs
├── config.py                  # Configuration settings
├── requirements.txt           # Python dependencies
└── README.md                 # This file
```

## ⚡ Quick Start

### 1. Prerequisites

**PostgreSQL Setup:**
```bash
sudo apt update && sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'password123';"
```

**Google Sheets API:**
1. Enable Google Sheets API in Google Cloud Console
2. Create service account and download credentials JSON
3. Update credentials path in `config.py`

### 2. Installation

```bash
cd /home/nineleaps/PycharmProject/Medallion-Data-Pipeline
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Run Bronze Layer Pipeline

```bash
# Step 1: Setup database schema
python bronze/database_setup.py

# Step 2: Load data from Google Sheets
python bronze/data_loader.py
```

## 📊 Pipeline Results

After successful execution of bronze layer:

| **Table** | **Records** | **Description** |
|-----------|-------------|-----------------|
| bronze.suppliers | 50,000 | Raw supplier master data |
| bronze.products | 50,000 | Raw product catalog |
| bronze.warehouses | 50,000 | Raw warehouse information |
| bronze.inventory | 50,000 | Raw inventory levels |
| bronze.shipments | 50,000 | Raw shipment transactions |
| **Total** | **250,000** | **Complete supply chain dataset** |

## 🔧 Configuration

Update settings in `config.py`:

```python
# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'supply_chain',
    'user': 'postgres',
    'password': 'password123',  # Update this!
    'port': 5432
}

# Google Sheets Configuration  
GOOGLE_SHEETS_CONFIG = {
    'credentials_path': '/path/to/your/credentials.json',  # Update this!
    'spreadsheet_id': 'your_sheet_id'  # Update this!
}
```

## 📋 Database Schema

### Bronze Layer (Raw Data)
```sql
bronze.suppliers     # Supplier master data (50K records)
bronze.products      # Product catalog (50K records)
bronze.warehouses    # Warehouse locations (50K records)
bronze.inventory     # Current stock levels (50K records)
bronze.shipments     # Shipment transactions (50K records)
```

### Silver Layer Views (Auto-created)
```sql
silver.suppliers_clean   # Validated suppliers with clean formats
silver.products_clean    # Clean product data with proper joins
```

### Gold Layer Analytics (Auto-created)
```sql
gold.inventory_summary   # Warehouse inventory KPIs (31K+ summaries)
gold.shipment_metrics    # Daily shipment analytics (5K+ metrics)
```

## 📝 Sample Output

```
🚀 Starting Bronze Layer Data Loading Pipeline...
================================================================================

📊 Processing suppliers...
📥 Loading suppliers data to bronze layer...
✅ Successfully loaded 50,000 suppliers to bronze layer

📊 Processing warehouses...
📥 Loading warehouses data to bronze layer...
✅ Successfully loaded 50,000 warehouses to bronze layer

📊 Processing products...
📥 Loading products data to bronze layer...
✅ Successfully loaded 50,000 products to bronze layer

📊 Processing inventory...
📥 Loading inventory data to bronze layer...
✅ Successfully loaded 50,000 inventory records to bronze layer

📊 Processing shipments...
📥 Loading shipments data to bronze layer...
✅ Successfully loaded 50,000 shipment records to bronze layer

🎯 BRONZE LAYER LOADING SUMMARY
================================================================================
  suppliers   : ✅ SUCCESS
  warehouses  : ✅ SUCCESS  
  products    : ✅ SUCCESS
  inventory   : ✅ SUCCESS
  shipments   : ✅ SUCCESS

📈 Overall: 5/5 sheets loaded successfully
🎉 All data loaded successfully to bronze layer!

📊 Bronze Layer Record Counts:
==================================================
  suppliers   :   50,000 records
  products    :   50,000 records
  warehouses  :   50,000 records
  inventory   :   50,000 records
  shipments   :   50,000 records
--------------------------------------------------
  TOTAL       :  250,000 records
```

## 🐛 Troubleshooting

### Common Issues

| **Issue** | **Solution** |
|-----------|-------------|
| PostgreSQL connection error | `sudo systemctl status postgresql` |
| Google Sheets API error | Verify credentials file path in config.py |
| Import errors | Ensure virtual environment is activated |
| Permission denied | Check database user privileges |

### Check Logs
```bash
tail -f logs/database_setup.log     # Database setup logs
tail -f logs/data_loader.log        # Data loading logs
```

### Verify Data
```bash
psql -U postgres -d supply_chain -c "
SELECT schemaname, tablename, n_tup_ins as records 
FROM pg_stat_user_tables 
WHERE schemaname = 'bronze'
ORDER BY tablename;"
```

## 🚀 Features

- ✅ **Clean Architecture**: Medallion pattern with clear layer separation
- ✅ **Scalable**: Handles 250K+ records efficiently
- ✅ **Google Sheets Integration**: Direct API data extraction
- ✅ **PostgreSQL Storage**: Reliable, enterprise-grade database
- ✅ **Comprehensive Logging**: Detailed logs for monitoring and debugging
- ✅ **Error Handling**: Robust error handling and recovery
- ✅ **Future-Ready**: Silver and Gold layers ready for development

## 📈 Performance

- **Data Volume**: 250,000+ records across 5 tables
- **Processing Time**: ~5-7 minutes for complete bronze layer
- **Memory Usage**: Optimized for large datasets with pandas
- **Reliability**: Handles API rate limits and connection issues

## 🔄 Development Roadmap

### Current: Bronze Layer ✅
- [x] Raw data ingestion from Google Sheets
- [x] PostgreSQL database setup
- [x] Data loading with error handling
- [x] Comprehensive logging

### Next: Silver Layer 🚧
- [ ] Data cleaning and validation functions
- [ ] Business rule enforcement
- [ ] Data quality monitoring
- [ ] Standardization pipelines

### Future: Gold Layer 📈
- [ ] Business KPI calculations
- [ ] Executive dashboards
- [ ] Automated reporting
- [ ] Real-time analytics

## 🛠️ Extending the Pipeline

### Adding New Data Sources
1. Add new sheet range to `config.py`
2. Create load function in `bronze/data_loader.py`
3. Add table schema to `bronze/database_setup.py`

### Silver Layer Development
1. Create transformation functions in `silver/`
2. Implement data quality checks
3. Add business validation rules

### Gold Layer Development  
1. Design business metrics in `gold/`
2. Create aggregation functions
3. Build dashboard integrations

## 📄 Dependencies

Core packages (see `requirements.txt`):
- `google-api-python-client` - Google Sheets API integration
- `psycopg2-binary` - PostgreSQL database adapter
- `pandas` - Data manipulation and analysis
- `httplib2` - HTTP client for API calls

## 🎯 Getting Help

1. **Check Logs**: Always check logs in `logs/` directory first
2. **Verify Config**: Ensure `config.py` has correct database and API settings
3. **Test Connection**: Run database setup to verify PostgreSQL connection
4. **Check Prerequisites**: Ensure PostgreSQL is running and credentials are valid

## 📄 License

This project implements the Medallion Data Pipeline architecture for supply chain analytics.

---

**⭐ Ready to process your supply chain data with a clean, scalable medallion architecture!**

**Current Status**: Bronze Layer Complete ✅ | Silver & Gold Ready for Development 🚧