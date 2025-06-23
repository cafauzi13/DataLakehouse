import logging
from datetime import datetime
import os
import sys

# Direktori root proyek adalah satu level di atas folder 'scripts'
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(script_dir, '..') 

# Tambahkan direktori ROOT proyek ke sys.path
# Ini penting agar Python dapat mengidentifikasi 'scripts' sebagai paket dari root.
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# DEBUG: Untuk melihat path yang digunakan Python
print("Current sys.path:", sys.path)
print("Expected script_dir (where modules are):", script_dir)
print("Expected project_root (where packages start):", project_root)

# Import modul-modul lain yang akan dijalankan
# Impor _01_setup_folders (atau setup_folders) DIHAPUS dari sini
from scripts import etl_adventureworks
from scripts import ingest_datalake
from scripts import analyze_datalake
from scripts import load_datalake_to_dw
from scripts import api_interface 

# Konfigurasi Logging Global untuk Orkestrator
log_dir = os.path.join(project_root, 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, f"main_pipeline_orchestration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def run_full_data_pipeline():
    """
    Menjalankan seluruh pipeline Data Lake dan ETL AdventureWorks secara berurutan.
    """
    logging.info("=====================================================")
    logging.info("--- Starting Full Data Pipeline Orchestration ---")
    logging.info("=====================================================")

    # 1. Setup Folder (Sekarang diasumsikan sudah dilakukan secara manual)
    logging.info("--- Phase 1: Folder setup assumed to be done manually. ---")
    logging.info("Please ensure all necessary folders exist in the project root: input_data_sources/, raw_data_lake/, processed_staging/, logs/, documentation/, scripts/utils/.")
    # Kode panggilan setup_folders.setup_project_folders() DIHAPUS di sini

    # 2. Clear existing data from databases (TRUNCATE TABLES)
    try:
        logging.info("\n--- Phase 2: Clearing existing data from databases ---")
        etl_adventureworks.drop_all_tables_in_dbs()
        logging.info("Phase 2: All specified tables cleared for a fresh start.")
    except Exception as e:
        logging.error(f"FATAL ERROR during database data clearing: {e}")
        logging.info("--- Pipeline terminated prematurely. ---")
        return

    # 3. Run AdventureWorks ETL
    try:
        logging.info("\n--- Phase 3: Running AdventureWorks ETL Process ---")
        etl_adventureworks.run_adventureworks_etl()
        logging.info("Phase 3: AdventureWorks ETL process completed successfully.")
    except Exception as e:
        logging.error(f"FATAL ERROR during AdventureWorks ETL: {e}")
        logging.info("--- Pipeline terminated prematurely. ---")
        return

    # 4. Run Data Lake Ingest (Copy raw files to raw_data_lake)
    try:
        logging.info("\n--- Phase 4: Running Data Lake Ingest Process ---")
        ingest_datalake.ingest_raw_data_to_datalake()
        logging.info("Phase 4: Data Lake Ingest process completed successfully.")
    except Exception as e:
        logging.error(f"FATAL ERROR during Data Lake Ingest: {e}")
        logging.info("--- Pipeline terminated prematurely. ---")
        return

    # 5. Run Data Lake Analyze (Process raw data to processed_staging)
    try:
        logging.info("\n--- Phase 5: Running Data Lake Analysis Process ---")
        analyze_datalake.analyze_all_datalake_data()
        logging.info("Phase 5: Data Lake Analyze process completed successfully.")
    except Exception as e:
        logging.error(f"FATAL ERROR during Data Lake Analyze: {e}")
        logging.info("--- Pipeline terminated prematurely. ---")
        return
    
    # 6. Load Data Lake data to Data Warehouse
    try:
        logging.info("\n--- Phase 6: Loading Data Lake Data to Data Warehouse ---")
        load_datalake_to_dw.load_all_datalake_data_to_dw()
        logging.info("Phase 6: Data Lake data loaded to Data Warehouse successfully.")
    except Exception as e:
        logging.error(f"FATAL ERROR during Data Lake load to DW: {e}")
        logging.info("--- Pipeline terminated prematurely. ---")
        return

    logging.info("=====================================================")
    logging.info("--- Full Data Pipeline Orchestration Completed! ---")
    logging.info("=====================================================")

    # Optional: Demo API calls at the end (untuk menunjukkan akses data)
    logging.info("\n--- Phase 7: Demonstrating API Access to Data Warehouse ---")
    print("\n--- Testing API Functions (output from print statements) ---")
    
    try:
        print("\nTotal Sales by Product Category (2010) from AdventureWorks DW:")
        sales_2010 = api_interface.get_total_sales_by_product_category(year=2010)
        if sales_2010:
            for row in sales_2010:
                print(f"  {row.get('product_category', 'N/A')}: {row.get('total_sales', 0):.2f}")
        else:
            print("  No sales data found for 2010 or API error.")
    except Exception as e:
        print(f"  Error calling AdventureWorks Sales API: {e}")

    try:
        print("\nAverage Warehouse Temperature for Zone A1 on 2023-01-01 (from Data Lake DW):")
        temp_A1 = api_interface.get_average_warehouse_temperature(zone_name='A1', date='2023-01-01')
        if temp_A1:
            for row in temp_A1:
                print(f"  Date: {row.get('measurement_date', 'N/A')}, Zone: {row.get('zone_name', 'N/A')}, Temp: {row.get('avg_temperature_c', 0):.2f}C, Humidity: {row.get('avg_humidity_percent', 0):.2f}%")
        else:
            print("  No temperature data found for A1/2023-01-01 or API error.")
    except Exception as e:
        print(f"  Error calling Data Lake Temp API: {e}")

    try:
        print("\nSentiment Analysis Summary (Positive) from Data Lake DW:")
        sentiment_positive = api_interface.get_sentiment_analysis_summary(sentiment_category='Positive')
        if sentiment_positive:
            for row in sentiment_positive:
                print(f"  {row.get('sentiment', 'N/A')}: Tweets: {row.get('total_tweets', 0)}, Avg Score: {row.get('average_score', 0):.2f}")
        else:
            print("  No positive sentiment data found or API error.")
    except Exception as e:
        print(f"  Error calling Data Lake Sentiment API: {e}")

    try:
        print("\nFinancial Report Summary (Competitor X) from Data Lake DW:")
        financial_compX = api_interface.get_financial_summary(company_name='Competitor X') 
        if financial_compX:
            for row in financial_compX:
                print(f"  Company: {row.get('company_name', 'N/A')}, Date: {row.get('report_date', 'N/A')}, Revenue: {row.get('revenue', 0):.2f}, Type: {row.get('report_type', 'N/A')}")
        else:
            print("  No financial data for Competitor X found or API error.")
    except Exception as e:
        print(f"  Error calling Data Lake Financial API: {e}")

    logging.info("\n--- API demonstration finished. ---")


if __name__ == "__main__":
    run_full_data_pipeline()