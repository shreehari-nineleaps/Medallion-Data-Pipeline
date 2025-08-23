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
    'credentials_path': os.getenv('GOOGLE_CREDS_PATH', '/home/nineleaps/Desktop/etl/plenary-agility-469511-r7-9a217afa87a9.json'),
    'scopes': ["https://www.googleapis.com/auth/spreadsheets.readonly"],
    'spreadsheet_id': os.getenv('SPREADSHEET_ID', '1Q7ND4AGHcPaqn75kWq3sT7oUHP8UCirNZ6FzgIQbxac')
}

# Sheet Ranges for different data types
SHEET_RANGES = {
    'suppliers': 'Suppliers!A:D',
    'products': 'Products!A:E',
    'warehouses': 'Warehouses!A:D',
    'inventory': 'Inventory!A:F',
    'shipments': 'Shipments!A:H'
}

# Logging Configuration
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'log_dir': 'logs'
}
