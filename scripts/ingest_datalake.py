import os
import shutil
import logging
from datetime import datetime

# Konfigurasi Logging (akan diatur oleh main_orchestrator, tapi baiknya ada default untuk testing mandiri)
log_dir_for_testing = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
if not os.path.exists(log_dir_for_testing):
    os.makedirs(log_dir_for_testing, exist_ok=True) # Tambah exist_ok=True agar tidak error jika sudah ada

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir_for_testing, f"ingest_datalake_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Konfigurasi Path untuk folder input dan raw data lake
INPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'input_data_sources')
RAW_LAKE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'raw_data_lake')

def ingest_raw_data_to_datalake():
    """
    Mengambil file dari input_data_sources/ dan menyalinnya ke raw_data_lake/ dalam satu level folder.
    """
    logging.info("--- Starting Data Lake Ingest Process ---")
    
    # Pastikan RAW_LAKE_DIR kosong sebelum ingest baru untuk demo
    # (Opsional, tapi bagus untuk demo bersih)
    for item in os.listdir(RAW_LAKE_DIR):
        item_path = os.path.join(RAW_LAKE_DIR, item)
        if os.path.isfile(item_path):
            os.remove(item_path)
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)
    logging.info(f"Cleaned up existing files/folders in {RAW_LAKE_DIR}.")


    for root, dirs, files in os.walk(INPUT_DIR):
        for file in files:
            source_file_path = os.path.join(root, file)
            
            # Tujuan: langsung ke root raw_data_lake/
            destination_file_path = os.path.join(RAW_LAKE_DIR, file) # Perubahan utama di sini
            
            try:
                shutil.copy2(source_file_path, destination_file_path)
                logging.info(f"Ingested: {os.path.basename(source_file_path)} to {destination_file_path}")
            except Exception as e:
                logging.error(f"Failed to ingest {source_file_path}: {e}")
    
    logging.info("--- Data Lake Ingest Process Completed ---")

if __name__ == "__main__":
    ingest_raw_data_to_datalake()