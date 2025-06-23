import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
import os
from sqlalchemy.dialects import postgresql # Import ini untuk ON CONFLICT DO NOTHING

# --- 1. Konfigurasi Database ---
pg_user = "postgres"
pg_pass = "************" 
pg_host = "localhost"
pg_port = "5432"

pg_adventureworks_source = "Adventureworks"     # Database OLTP sumber
pg_staging_db = "adventureworks_staging"        # Database untuk Staging Area
pg_dw_db = "adventureworks_dw"                  # Database untuk Data Warehouse

# SQLAlchemy Engines
engine_adventure_source = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_adventureworks_source}")
engine_staging = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_staging_db}")
engine_dw = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_dw_db}")

# --- 2. Konfigurasi Logging ---
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, f"etl_adventureworks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- 3. Fungsi Utama ETL ---

def drop_all_tables_in_dbs():
    """Truncates (clears) all tables in staging and DW databases for a clean run."""
    
    tables_to_clear_staging = [
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
    
    tables_to_clear_dw = [
        "dim_product", "dim_customer", "dim_store", "dim_vendor", "dim_employee", 
        "dim_date", "fact_sales",
        "dim_warehouse_zone", "dim_sentiment_category", "fact_warehouse_temperature",
        "fact_social_media_sentiment", "dim_company", "fact_financial" # Tambahkan tabel finansial
    ]

    logging.info("Clearing existing data from Staging and DW databases (TRUNCATE TABLE)...")

    with engine_dw.connect() as conn_dw:
        for table in tables_to_clear_dw:
            try:
                conn_dw.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))
                logging.info(f"Cleared data from {table} in {pg_dw_db}.")
            except Exception as e:
                logging.warning(f"Failed to clear data from {table} in {pg_dw_db} (table might not exist yet or in use): {e}")
        conn_dw.commit()

    with engine_staging.connect() as conn_stg:
        for table in tables_to_clear_staging:
            try:
                conn_stg.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))
                logging.info(f"Cleared data from {table} in {pg_staging_db}.")
            except Exception as e:
                logging.warning(f"Failed to clear data from {table} in {pg_staging_db} (table might not exist yet or in use): {e}")
        conn_stg.commit()
    
    logging.info("All specified tables cleared in staging and DW databases.")


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


def create_stg_tables():
    """
    Placeholder function: Schema for intermediate staging tables (stg_...)
    is now managed by an external SQL script (create_database_schemas.sql).
    """
    logging.info("Schema creation for intermediate staging tables handled by external SQL script.")
    pass


def transform_raw_to_stg_tables():
    """
    Performs transformations from raw_ tables (or directly from source if preferred)
    to stg_ tables in the staging database.
    
    NOTE: Customize transformation logic here for each stg_ table.
    """
    logging.info("Transforming raw_ tables to stg_ tables...")

    tables_for_stg_load = {
        "Person.Address": "stg_address",
        "Person.BusinessEntityAddress": "stg_businessentityaddress",
        "Person.CountryRegion": "stg_countryregion", 
        "Sales.Customer": "stg_customer",
        "HumanResources.Department": "stg_department",
        "Person.EmailAddress": "stg_emailaddress",
        "HumanResources.Employee": "stg_employee",
        "HumanResources.EmployeeDepartmentHistory": "stg_employeedepartmenthistory",
        "Person.Person": "stg_person",
        "Person.PersonPhone": "stg_personphone",
        "Production.Product": "stg_product",
        "Production.ProductCategory": "stg_productcategory",
        "Production.ProductSubcategory": "stg_productsubcategory",
        "Purchasing.ProductVendor": "stg_productvendor",
        "Sales.SalesOrderDetail": "stg_salesorderdetail",
        "Sales.SalesOrderHeader": "stg_salesorderheader",
        "Person.StateProvince": "stg_stateprovince", 
        "Sales.Store": "stg_store",
        "Purchasing.Vendor": "stg_vendor"
    }
    
    for src_table_name, dest_stg_table_name in tables_for_stg_load.items():
        try:
            logging.info(f"Loading {src_table_name} to {dest_stg_table_name}...")
            df = pd.read_sql(f"SELECT * FROM {src_table_name}", engine_adventure_source)
            df.to_sql(dest_stg_table_name, engine_staging, if_exists='replace', index=False)
            logging.info(f"{dest_stg_table_name} loaded successfully.")
        except Exception as e:
            logging.error(f"Failed to load {src_table_name} to {dest_stg_table_name}: {e}")

    logging.info("Transformation from raw_ to stg_ tables completed.")


def create_dim_fact_tables_aw():
    """
    Placeholder function: Schema for AdventureWorks Star Schema tables
    is now managed by an external SQL script (create_database_schemas.sql).
    """
    logging.info("Schema creation for AdventureWorks Star Schema tables handled by external SQL script.")
    pass

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

def generate_dim_date(start='2010-01-01', end='2025-12-31'): # Rentang tahun diperluas
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


# --- REVISED: load_df_to_dw untuk menangani UniqueViolation pada dimensi ---
def load_df_to_dw(df, table_name, pk_col=None):
    """
    Loads a DataFrame to a specified table in the DW database.
    For dimension tables, it attempts to insert and ignores duplicates based on primary key.
    For fact tables, it appends data.
    """
    try:
        if pk_col: # Ini adalah tabel dimensi
            data_to_insert = df.to_dict(orient='records')
            
            columns = ", ".join(df.columns)
            placeholders = ", ".join([f":{col}" for col in df.columns])
            
            insert_stmt = text(f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders})
                ON CONFLICT ({pk_col}) DO NOTHING;
            """)
            
            with engine_dw.connect() as conn_dw:
                conn_dw.execute(insert_stmt, data_to_insert)
                conn_dw.commit()
            logging.info(f"Loaded {len(df)} rows into {table_name} in {pg_dw_db} (ON CONFLICT DO NOTHING for PK '{pk_col}').")

        else: # Ini adalah tabel fakta (tidak ada pk_col yang diberikan, akan selalu 'append')
            df.to_sql(table_name, engine_dw, if_exists='append', index=False)
            logging.info(f"Loaded {len(df)} rows into {table_name} in {pg_dw_db} (appended).")

    except Exception as e:
        logging.error(f"Failed to load {table_name} to {pg_dw_db}: {e}")


def run_adventureworks_etl():
    """Orchestrates the ETL process for AdventureWorks data."""
    
    logging.info("--- Starting AdventureWorks ETL Process ---")
    
    logging.info("Copying raw data from AdventureWorks source to staging database...")
    copy_raw_tables_to_staging()

    logging.info("Creating intermediate staging tables (stg_...) schema...")
    create_stg_tables() # Panggilan fungsi tanpa DDL di dalamnya

    logging.info("Transforming and Loading raw data to intermediate staging tables (stg_)...")
    transform_raw_to_stg_tables() # Mengisi tabel stg_...

    logging.info("Creating AdventureWorks Star Schema tables in DW database...")
    create_dim_fact_tables_aw() # Panggilan fungsi tanpa DDL di dalamnya

    logging.info("Transforming and Loading dimension tables for AdventureWorks...")
    load_df_to_dw(extract_dim_product(), "dim_product", pk_col="productid")
    load_df_to_dw(extract_dim_customer(), "dim_customer", pk_col="customerid")
    load_df_to_dw(extract_dim_store(), "dim_store", pk_col="storeid")
    load_df_to_dw(extract_dim_vendor(), "dim_vendor", pk_col="vendorid")
    load_df_to_dw(extract_dim_employee(), "dim_employee", pk_col="employeeid")

    logging.info("Generating and Loading dim_date for AdventureWorks...")
    load_df_to_dw(generate_dim_date(), "dim_date", pk_col="datekey")

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
    logging.warning("Running etl_adventureworks.py as standalone script. Ensure database schemas are pre-created via SQL.")
    # Jika ingin bersih setiap kali jalankan STANDALONE, Anda bisa panggil:
    # drop_all_tables_in_dbs()
    run_adventureworks_etl()