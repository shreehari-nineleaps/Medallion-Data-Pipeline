import os

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'supply_chain'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password123'),
    'port': int(os.getenv('DB_PORT', '5432'))
}

# Google Sheets Configuration
GOOGLE_SHEETS_CONFIG = {
    'credentials_path': os.getenv('GOOGLE_CREDS_PATH', '/home/nineleaps/Desktop/etl/plenary-agility-469511-r7-26ec3f017851.json'),
    'scopes': ["https://www.googleapis.com/auth/spreadsheets.readonly"],
    'spreadsheet_id': os.getenv('SPREADSHEET_ID', '1Q7ND4AGHcPaqn75kWq3sT7oUHP8UCirNZ6FzgIQbxac')
}

# Sheet Ranges for different data types
SHEET_RANGES = {
    'suppliers': 'Suppliers!A:D',
    'products': 'Products!A:G',
    'warehouses': 'Warehouses!A:E',
    'inventory': 'Inventory!A:E',
    'retail_stores': 'RetailStores!A:F',
    'supply_orders': 'SupplyOrders!A:L'
}

# Logging Configuration
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'log_dir': 'logs'
}
