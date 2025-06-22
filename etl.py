import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
import os # Import os module

# --- 1. Konfigurasi Database ---
pg_user = "postgres"
pg_pass = "*********" 
pg_host = "localhost"
pg_port = "5432"

pg_adventureworks_source = "Adventureworks"     # Database OLTP sumber
pg_staging_db = "adventureworks_staging"        # Database baru untuk Staging Area
pg_dw_db = "adventureworks_dw"                  # Database baru untuk Data Warehouse

# SQLAlchemy Engines
engine_adventure_source = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_adventureworks_source}")
engine_staging = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_staging_db}")
engine_dw = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_dw_db}")

# --- 2. Konfigurasi Logging ---
# Pastikan folder logs ada sebelum konfigurasi logging
log_dir = "logs" # Sesuaikan path ini jika folder logs tidak di root proyek Anda
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, f"etl_adventureworks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"), encoding='utf-8'),
        logging.StreamHandler() # Biarkan default, atau bisa diset encoding='utf-8' jika terminal mendukung
    ]
)

# --- 3. Fungsi Utama ETL ---

def drop_all_tables_in_dbs():
    """Drops all tables in staging and DW databases."""
    
    # Tables to drop from Staging Database (raw_ and stg_ tables)
    tables_to_drop_staging = [
        "raw_salesorderdetail", "raw_salesorderheader", "raw_product", "raw_customer",
        "raw_person", "raw_productcategory", "raw_productsubcategory", "raw_store",
        "raw_vendor", "raw_productvendor", "raw_employeedepartmenthistory", "raw_department",
        "raw_employee",
        "stg_address", "stg_businessentityaddress", "stg_countryregion", "stg_customer",
        "stg_department", "stg_emailaddress", "stg_employee", "stg_employeedepartmenthistory",
        "stg_person", "stg_personphone", "stg_product", "stg_productcategory",
        "stg_productsubcategory", "stg_productvendor", "stg_salesorderdetail", "stg_salesorderheader",
        "stg_stateprovince", "stg_store", "stg_vendor"
    ]
    
    # Tables to drop from Data Warehouse Database (dim_ and fact_ tables, including Data Lake ones)
    tables_to_drop_dw = [
        "dim_product", "dim_customer", "dim_store", "dim_vendor", "dim_employee", 
        "dim_date", "fact_sales",
        # Tambahkan tabel DW untuk Data Lake di sini (jika sudah ada, agar ikut di-drop)
        "dim_warehouse_zone", "dim_sentiment_category", "fact_warehouse_temperature",
        "fact_social_media_sentiment"
    ]

    logging.info("Dropping existing tables from Staging and DW databases...")

    with engine_staging.connect() as conn_stg:
        for table in tables_to_drop_staging:
            try:
                conn_stg.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
                logging.info(f"Dropped {table} from {pg_staging_db}.")
            except Exception as e:
                logging.error(f"Failed to drop {table} from {pg_staging_db}: {e}")
        conn_stg.commit()

    with engine_dw.connect() as conn_dw:
        for table in tables_to_drop_dw:
            try:
                conn_dw.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
                logging.info(f"Dropped {table} from {pg_dw_db}.")
            except Exception as e:
                logging.error(f"Failed to drop {table} from {pg_dw_db}: {e}")
        conn_dw.commit()
    
    logging.info("All specified tables dropped from staging and DW databases.")


def copy_raw_tables_to_staging():
    """Copies selected tables from AdventureWorks source to the staging database."""
    tables_to_copy = {
        "Sales.SalesOrderDetail": "raw_salesorderdetail",
        "Sales.SalesOrderHeader": "raw_salesorderheader",
        "Production.Product": "raw_product",
        "Sales.Customer": "raw_customer",
        "Person.Person": "raw_person",
        "Production.ProductCategory": "raw_productcategory",
        "Production.ProductSubcategory": "raw_productsubcategory",
        "Sales.Store": "raw_store",
        "Purchasing.Vendor": "raw_vendor",
        "Purchasing.ProductVendor": "raw_productvendor",
        "HumanResources.Employee": "raw_employee",
        "HumanResources.EmployeeDepartmentHistory": "raw_employeedepartmenthistory",
        "HumanResources.Department": "raw_department"
    }

    for src_table, dest_table in tables_to_copy.items():
        try:
            logging.info(f"Copying {src_table} to {dest_table} in {pg_staging_db}...")
            df = pd.read_sql(f"SELECT * FROM {src_table}", engine_adventure_source)
            df.to_sql(dest_table, engine_staging, if_exists='replace', index=False)
            logging.info(f"{dest_table} copied.")
        except Exception as e:
            logging.error(f"Failed to copy {src_table}: {e}")

# Note: Jika kamu punya transformasi dari raw_ ke stg_..., fungsi-fungsi itu akan ada di sini
# Contoh placeholder untuk transformasi ke stg_address (sesuai yang kamu punya)
def transform_raw_to_stg_address():
    try:
        logging.info("Transforming raw data to stg_address...")
        # Asumsi ada raw_address atau perlu join dari raw_person dll.
        # Ini hanya contoh, sesuaikan dengan logika ETL Anda yang sebenarnya
        df_address = pd.read_sql("SELECT AddressID, AddressLine1, City FROM Person.Address", engine_adventure_source)
        df_address.to_sql("stg_address", engine_staging, if_exists='replace', index=False)
        logging.info("stg_address created.")
    except Exception as e:
        logging.error(f"Failed to create stg_address: {e}")

# ... tambahkan fungsi transform_raw_to_stg_... lainnya jika ada

def create_dim_fact_tables_aw():
    """Creates Star Schema tables for AdventureWorks in the DW database."""
    logging.info("Creating AdventureWorks Star Schema tables in DW database...")
    with engine_dw.connect() as conn_dw:
        # Dimensi
        conn_dw.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_product (
            productid INT PRIMARY KEY,
            name TEXT, color TEXT, size TEXT, weight NUMERIC
        );"""))
        conn_dw.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_customer (
            customerid INT PRIMARY KEY,
            name TEXT, title TEXT, demographic TEXT
        );"""))
        conn_dw.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_store (
            storeid INT PRIMARY KEY,
            storename TEXT
        );"""))
        conn_dw.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_vendor (
            vendorid INT PRIMARY KEY,
            vendorname TEXT
        );"""))
        conn_dw.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_employee (
            employeeid INT PRIMARY KEY,
            fullname TEXT,
            jobtitle TEXT,
            department TEXT
        );"""))
        conn_dw.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_date (
            datekey INT PRIMARY KEY,
            fulldate DATE,
            day INT,
            month INT,
            year INT
        );"""))

        # Fact table
        conn_dw.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_sales (
            factid SERIAL PRIMARY KEY,
            productid INT REFERENCES dim_product(productid),
            customerid INT REFERENCES dim_customer(customerid),
            storeid INT REFERENCES dim_store(storeid),
            vendorid INT REFERENCES dim_vendor(vendorid),
            employeeid INT REFERENCES dim_employee(employeeid),
            datekey INT REFERENCES dim_date(datekey),
            qtyproduct INT,
            unitprice NUMERIC,
            unitpricedisc NUMERIC,
            totalpenjualan NUMERIC
        );"""))
        conn_dw.commit()
    logging.info("AdventureWorks Star Schema tables created.")

# --- Extraction Functions (membaca dari staging) ---

def extract_dim_product():
    return pd.read_sql("""
        SELECT ProductID AS productid, Name, Color, Size, Weight 
        FROM raw_product
    """, engine_staging)

def extract_dim_customer():
    return pd.read_sql("""
        SELECT c.CustomerID AS customerid,
               p.FirstName || ' ' || p.LastName AS name,
               p.Title,
               p.AdditionalContactInfo AS demographic
        FROM raw_customer c
        JOIN raw_person p ON c.PersonID = p.BusinessEntityID
    """, engine_staging)

def extract_dim_store():
    return pd.read_sql("""
        SELECT BusinessEntityID AS storeid, Name AS storename
        FROM raw_store
    """, engine_staging)

def extract_dim_vendor():
    return pd.read_sql("""
        SELECT BusinessEntityID AS vendorid, Name AS vendorname
        FROM raw_vendor
    """, engine_staging)

def extract_dim_employee():
    return pd.read_sql("""
        SELECT e.BusinessEntityID AS employeeid,
               p.FirstName || ' ' || p.LastName AS fullname,
               e.JobTitle,
               d.Name AS department
        FROM raw_employee e
        JOIN raw_person p ON e.BusinessEntityID = p.BusinessEntityID
        JOIN raw_employeedepartmenthistory edh ON e.BusinessEntityID = edh.BusinessEntityID
        JOIN raw_department d ON edh.DepartmentID = d.DepartmentID
        WHERE edh.EndDate IS NULL
    """, engine_staging)

def generate_dim_date(start='2010-01-01', end='2014-12-31'):
    date_range = pd.date_range(start=start, end=end)
    df = pd.DataFrame()
    df['fulldate'] = date_range
    df['datekey'] = df['fulldate'].dt.strftime('%Y%m%d').astype(int)
    df['day'] = df['fulldate'].dt.day
    df['month'] = df['fulldate'].dt.month
    df['year'] = df['fulldate'].dt.year
    return df[['datekey', 'fulldate', 'day', 'month', 'year']]

def extract_fact_sales_order_detail():
    return pd.read_sql("""
        SELECT salesorderdetailid, productid, orderqty AS qtyproduct,
               unitprice, unitpricediscount AS unitpricedisc, salesorderid
        FROM raw_salesorderdetail
    """, engine_staging)

def extract_fact_sales_order_header():
    return pd.read_sql("""
        SELECT soh.salesorderid, soh.orderdate,
               soh.customerid, soh.salespersonid AS employeeid,
               c.storeid
        FROM raw_salesorderheader soh
        LEFT JOIN raw_customer c ON soh.customerid = c.customerid
    """, engine_staging)


def load_df_to_dw(df, table_name):
    """Loads a DataFrame to a specified table in the DW database."""
    try:
        df.to_sql(table_name, engine_dw, if_exists='append', index=False)
        logging.info(f"Loaded {len(df)} rows into {table_name} in {pg_dw_db}.")
    except Exception as e:
        logging.error(f"Failed to load {table_name} to {pg_dw_db}: {e}")


def run_adventureworks_etl():
    """Orchestrates the ETL process for AdventureWorks data."""
    
    logging.info("--- Starting AdventureWorks ETL Process ---")
    
    logging.info("Copying raw data from AdventureWorks source to staging database...")
    copy_raw_tables_to_staging()

    # Jika kamu punya transformasi dari raw_ ke stg_..., panggil di sini
    # Contoh:
    # logging.info("Transforming raw data to staging tables...")
    # transform_raw_to_stg_address() 
    # ... panggil fungsi stg_... lainnya ...

    logging.info("Creating AdventureWorks Star Schema tables in DW database...")
    create_dim_fact_tables_aw()

    logging.info("Transforming and Loading dimension tables for AdventureWorks...")
    load_df_to_dw(extract_dim_product(), "dim_product")
    load_df_to_dw(extract_dim_customer(), "dim_customer")
    load_df_to_dw(extract_dim_store(), "dim_store")
    load_df_to_dw(extract_dim_vendor(), "dim_vendor")
    load_df_to_dw(extract_dim_employee(), "dim_employee")

    logging.info("Generating and Loading dim_date for AdventureWorks...")
    load_df_to_dw(generate_dim_date(), "dim_date")

    logging.info("Transforming and Loading fact_sales for AdventureWorks...")
    df_detail = extract_fact_sales_order_detail()
    df_header = extract_fact_sales_order_header()
    df_fact = pd.merge(df_detail, df_header, on="salesorderid")

    df_pv = pd.read_sql("SELECT productid, businessentityid AS vendorid FROM raw_productvendor", engine_staging)
    df_fact = df_fact.merge(df_pv, on='productid', how='left')

    df_fact['datekey'] = pd.to_datetime(df_fact['orderdate']).dt.strftime('%Y%m%d').astype(int)
    df_fact['totalpenjualan'] = df_fact['qtyproduct'] * (df_fact['unitprice'] - df_fact['unitpricedisc'])

    load_df_to_dw(df_fact[['productid', 'customerid', 'storeid', 'vendorid',
                             'employeeid', 'datekey', 'qtyproduct',
                             'unitprice', 'unitpricedisc', 'totalpenjualan']], 'fact_sales')
    
    logging.info("--- AdventureWorks ETL Process completed successfully. ---")

# --- Bagian Main (untuk menjalankan sebagai script mandiri) ---
if __name__ == "__main__":
    # Ini jika kamu ingin menjalankan ETL AdventureWorks secara terpisah untuk pengujian.
    # Dalam pipeline Data Lake utama (main_orchestrator.py), kamu akan memanggil run_adventureworks_etl().
    
    # PERINGATAN: Drop semua tabel akan menghapus data yang sudah ada. 
    # Untuk demo, ini biasanya aman karena akan selalu dimulai dari bersih.
    drop_all_tables_in_dbs() 
    run_adventureworks_etl()