import os
import shutil
import logging
from datetime import datetime

# Konfigurasi Logging (akan diatur oleh main_orchestrator, tapi baiknya ada default untuk testing mandiri)
log_dir_for_testing = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
if not os.path.exists(log_dir_for_testing):
    os.makedirs(log_dir_for_testing, exist_ok=True)  # Tambah exist_ok=True agar tidak error jika sudah ada

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

def sort_file_by_type(file_name):
    """
    Fungsi untuk mengembalikan folder tujuan berdasarkan tipe file.
    Menyortir file berdasarkan ekstensi file.
    """
    file_extension = file_name.split('.')[-1].lower()
    if file_extension in ['pdf']:
        return 'pdf'
    elif file_extension in ['csv']:
        return 'csv'
    elif file_extension in ['txt']:
        return 'txt'
    else:
        return 'others'

def ingest_raw_data_to_datalake():
    """
    Mengambil file dari input_data_sources/ dan menyalinnya ke raw_data_lake/
    Menyortir file berdasarkan jenisnya (misal PDF, CSV, TXT) dan menyalinnya ke folder yang sesuai.
    """
    logging.info("--- Starting Data Lake Ingest Process ---")
    
    # Pastikan RAW_LAKE_DIR kosong sebelum ingest baru untuk demo
    if os.path.exists(RAW_LAKE_DIR):
        for item in os.listdir(RAW_LAKE_DIR):
            item_path = os.path.join(RAW_LAKE_DIR, item)
            if os.path.isfile(item_path):
                os.remove(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
        logging.info(f"Cleaned up existing files/folders in {RAW_LAKE_DIR}.")
    else:
        logging.info(f"RAW_LAKE_DIR does not exist, creating it now.")
        os.makedirs(RAW_LAKE_DIR, exist_ok=True)

    if not os.listdir(INPUT_DIR):
        logging.warning(f"No files found in {INPUT_DIR}. No data ingested.")
    
    for root, dirs, files in os.walk(INPUT_DIR):
        for file in files:
            source_file_path = os.path.join(root, file)
            
            # Tentukan folder tujuan berdasarkan tipe file
            file_type_folder = sort_file_by_type(file)
            destination_dir = os.path.join(RAW_LAKE_DIR, file_type_folder)
            
            # Buat folder jika belum ada
            if not os.path.exists(destination_dir):
                os.makedirs(destination_dir)
                logging.info(f"Created folder {destination_dir} for file type '{file_type_folder}'.")

            # Tentukan file tujuan
            destination_file_path = os.path.join(destination_dir, file)

            # Cek jika file sudah ada di folder tujuan, tambahkan timestamp jika sudah ada
            if os.path.exists(destination_file_path):
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                file_name, file_extension = os.path.splitext(file)
                new_file_name = f"{file_name}_{timestamp}{file_extension}"
                destination_file_path = os.path.join(destination_dir, new_file_name)
                logging.info(f"File already exists, renaming to {new_file_name}")

            try:
                shutil.copy2(source_file_path, destination_file_path)
                logging.info(f"Ingested: {os.path.basename(source_file_path)} to {destination_file_path}")
            except Exception as e:
                logging.error(f"Failed to ingest {source_file_path}: {e}")
    
    logging.info("--- Data Lake Ingest Process Completed ---")

if __name__ == "__main__":
    ingest_raw_data_to_datalake()
