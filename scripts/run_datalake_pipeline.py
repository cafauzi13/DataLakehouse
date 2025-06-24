# run_datalake_pipeline.py

import logging
from datetime import datetime
import os
import sys

# --- KUNCI PERBAIKAN PATH ---
# Kita perlu menambahkan folder root proyek (DataLakehouse), BUKAN folder scripts, ke dalam path
# __file__ akan menunjuk ke file ini sendiri.
# os.path.dirname(__file__) akan menunjuk ke folder tempat file ini berada (misal: d:/DataLakehouse/scripts)
# os.path.dirname(...) dari hasil sebelumnya akan naik satu level ke folder root (d:/DataLakehouse)
try:
    # Jika file ini ada di dalam folder scripts
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    # Fallback jika dijalankan di lingkungan interaktif
    project_root = os.path.abspath('.')

if project_root not in sys.path:
    sys.path.insert(0, project_root)
# ---------------------------

# Sekarang import ini akan berhasil karena Python mencari dari D:\DataLakehouse
from scripts import ingest_datalake, analyze_datalake, load_datalake_to_dw, api_interface

# Konfigurasi Logging
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, f"datalake_pipeline_orchestration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")),
        logging.StreamHandler()
    ]
)

def run_data_lake_only_pipeline():
    """
    Menjalankan pipeline yang FOKUS pada Data Lake:
    Ingest -> Analyze -> Load to DW -> Test API.
    """
    logging.info("=========================================================")
    logging.info("--- Starting Data Lake Focused Pipeline Orchestration ---")
    logging.info("=========================================================")

    # Fase 1: Ingest Data Lake
    try:
        logging.info("\n--- Phase 1: Running Data Lake Ingest Process ---")
        ingest_datalake.ingest_raw_data_to_datalake()
        logging.info("Phase 1: Data Lake Ingest process completed successfully.")
    except Exception as e:
        logging.error(f"FATAL ERROR during Data Lake Ingest: {e}", exc_info=True)
        return

    # Fase 2: Analisis Data Lake
    try:
        logging.info("\n--- Phase 2: Running Data Lake Analysis Process ---")
        analyze_datalake.analyze_all_datalake_data()
        logging.info("Phase 2: Data Lake Analyze process completed successfully.")
    except Exception as e:
        logging.error(f"FATAL ERROR during Data Lake Analyze: {e}", exc_info=True)
        return
    
    # Fase 3: Load Data Lake ke Data Warehouse
    try:
        logging.info("\n--- Phase 3: Loading Data Lake Data to Data Warehouse ---")
        load_datalake_to_dw.load_all_datalake_data_to_dw()
        logging.info("Phase 3: Data Lake data loaded to Data Warehouse successfully.")
    except Exception as e:
        logging.error(f"FATAL ERROR during Data Lake load to DW: {e}", exc_info=True)
        return

    logging.info("=====================================================")
    logging.info("--- Data Lake Pipeline Orchestration Completed! ---")
    logging.info("=====================================================")

    # Fase 4 (Opsional): Menjalankan tes visualisasi
    logging.info("\n--- Phase 4: Generating Visualizations from API ---")
    
    # Visualisasi Data Finansial
    competitor_data = api_interface.get_financial_summary(exclude_companies=['AdventureWorks', 'Market Report'])
    api_interface.generate_competitor_trend_chart(competitor_data, log_dir)

    # Visualisasi Data Gudang
    temp_data = api_interface.get_all_warehouse_temperatures()
    api_interface.generate_warehouse_temp_chart(temp_data, log_dir)
    
    # Visualisasi Data Media Sosial
    word_freq = api_interface.get_word_frequency_data()
    api_interface.generate_social_media_wordcloud(word_freq, log_dir)
    
    logging.info("--- All visualizations generated. Check the 'logs' folder. ---")


if __name__ == "__main__":
    run_data_lake_only_pipeline()