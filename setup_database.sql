-- Step 1: Create Database (run this from your "postgres" connection)
CREATE DATABASE supply_chain;

-- Step 2: (In DBeaver, change your connection to "supply_chain")
-- No need for \c here, just switch database from the dropdown.

-- Step 3: Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Step 4: Create Tables in Bronze Schema

-- 1. Suppliers
CREATE TABLE IF NOT EXISTS bronze.suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name TEXT NOT NULL,
    contact_email TEXT UNIQUE NOT NULL,
    phone_number TEXT NOT NULL
);

-- 2. Products
CREATE TABLE IF NOT EXISTS bronze.products (
    product_id SERIAL PRIMARY KEY,
    sku TEXT UNIQUE NOT NULL,
    product_name TEXT NOT NULL,
    unit_cost NUMERIC(10,2) NOT NULL,
    supplier_id INT REFERENCES bronze.suppliers(supplier_id)
);

-- 3. Warehouses
CREATE TABLE IF NOT EXISTS bronze.warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    warehouse_name TEXT NOT NULL,
    location_city TEXT NOT NULL,
    storage_capacity INT NOT NULL
);

-- 4. Inventory
CREATE TABLE IF NOT EXISTS bronze.inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES bronze.products(product_id),
    warehouse_id INT REFERENCES bronze.warehouses(warehouse_id),
    quantity_on_hand INT NOT NULL,
    last_stocked_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Shipments
CREATE TABLE IF NOT EXISTS bronze.shipments (
    shipment_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES bronze.products(product_id),
    warehouse_id INT REFERENCES bronze.warehouses(warehouse_id),
    quantity_shipped INT NOT NULL,
    shipment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    destination TEXT NOT NULL,
    status TEXT CHECK (status IN ('In Transit','Delivered','Delayed','Cancelled')),
    weight_kg NUMERIC(10,2)
);
