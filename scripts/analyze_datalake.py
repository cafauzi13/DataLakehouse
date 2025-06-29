import os
import pandas as pd
import logging
import re
from textblob import TextBlob
from collections import Counter
import fitz  # PyMuPDF
import docx  # python-docx
from sqlalchemy import create_engine, text

# --- Konfigurasi Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s'
)

# --- Konfigurasi Path ---
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.join(SCRIPT_DIR, '..')
    RAW_DATA_LAKE_DIR = os.path.join(PROJECT_ROOT, 'raw_data_lake')
    PROCESSED_STAGING_DIR = os.path.join(PROJECT_ROOT, 'processed_staging')
    
    os.makedirs(PROCESSED_STAGING_DIR, exist_ok=True)
    logging.info("Path directories configured successfully.")
except Exception as e:
    logging.critical(f"FATAL: Could not configure paths. Error: {e}")
    exit()

# --- Konfigurasi Koneksi Database Staging ---
# >>> GANTI USERNAME DAN PASSWORD DI BAWAH INI <<<
DB_USER = "postgres"     # Ganti dengan username PostgreSQL-mu
DB_PASSWORD = "*********" # Ganti dengan password-mu
DB_HOST = "localhost"               # Biasanya 'localhost'
DB_PORT = "5432"                    # Port default PostgreSQL
DB_STAGING_NAME = "datalake_staging"  # Sesuai permintaanmu

try:
    connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_STAGING_NAME}'
    engine_staging = create_engine(connection_string)
    # Uji koneksi (SUDAH DIPERBAIKI)
    with engine_staging.connect() as conn:
        conn.execute(text("SELECT 1"))
    logging.info(f"Successfully connected to the staging database '{DB_STAGING_NAME}'.")
except Exception as e:
    logging.critical(f"FATAL: Could not connect to staging database. Error: {e}")
    exit()

# --- FUNGSI-FUNGSI PEMROSESAN DATA ---

def process_sensor_data(file_list, processed_dir, db_engine):
    """Menerima daftar file sensor, mengagregasi, menyimpan ke CSV DAN memuat ke DB Staging."""
    logging.info(f"--- Processing {len(file_list)} Warehouse Sensor file(s) ---")
    try:
        df_list = [pd.read_csv(f) for f in file_list]
        df = pd.concat(df_list, ignore_index=True)

        if df.empty:
            logging.warning("Warehouse sensor data is empty after reading files. No summary generated.")
            return

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date
        summary_df = df.groupby(['date', 'zone_id']).agg(
            avg_temperature_c=('temperature_c', 'mean'),
            avg_humidity_percent=('humidity_percent', 'mean')
        ).reset_index()
        summary_df['avg_temperature_c'] = summary_df['avg_temperature_c'].round(2)
        summary_df['avg_humidity_percent'] = summary_df['avg_humidity_percent'].round(2)

        if summary_df.empty:
            logging.warning("Warehouse sensor summary is empty after aggregation. Nothing to save.")
            return

        # 1. TETAP SIMPAN KE CSV (untuk skrip load_to_dw)
        output_path = os.path.join(processed_dir, 'warehouse_daily_sensor_summary.csv')
        summary_df.to_csv(output_path, index=False)
        logging.info(f"Successfully generated warehouse summary CSV with {len(summary_df)} rows.")

        # 2. BARU: MUAT KE DATABASE STAGING
        table_name = 'warehouse_daily_sensor_summary'
        summary_df.to_sql(table_name, con=db_engine, if_exists='replace', index=False, method='multi')
        logging.info(f"Successfully loaded data to staging DB table '{table_name}'.")

    except Exception as e:
        logging.error(f"An error occurred during sensor data processing: {e}", exc_info=True)


def process_social_media_data(file_list, processed_dir, db_engine):
    """Menerima daftar file media sosial, menganalisis, menyimpan ke CSV DAN memuat ke DB Staging."""
    logging.info(f"--- Processing {len(file_list)} Social Media file(s) ---")
    try:
        all_tweets = []
        for file_path in file_list:
            if file_path.endswith('.csv'):
                df_temp = pd.read_csv(file_path)
                if 'tweet_text' in df_temp.columns:
                    all_tweets.extend(df_temp['tweet_text'].dropna().tolist())
            elif file_path.endswith('.txt'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    all_tweets.extend(f.read().splitlines())
        
        if not all_tweets:
            logging.warning("No tweets found in social media files. Skipping.")
            return

        df = pd.DataFrame(all_tweets, columns=['tweet_text'])
        df.dropna(subset=['tweet_text'], inplace=True)
        df = df[df['tweet_text'].str.strip() != '']

        if df.empty:
            logging.warning("DataFrame is empty after cleaning tweets. Skipping.")
            return

        def get_sentiment(text):
            try: return TextBlob(str(text)).sentiment.polarity
            except: return 0.0
        df['sentiment_score'] = df['tweet_text'].apply(get_sentiment)
        
        def get_category(score):
            if score > 0.1: return 'Positive'
            elif score < -0.1: return 'Negative'
            else: return 'Neutral'
        df['sentiment_category'] = df['sentiment_score'].apply(get_category)
        
        def get_top_words(text):
            try:
                words = [word.lower() for word in TextBlob(str(text)).words if len(word) > 2 and word.isalpha()]
                return str(Counter(words).most_common(10))
            except:
                return str([])
        
        df['top_words_json'] = df['tweet_text'].apply(get_top_words)
        df['date_processed'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        df['original_filename'] = "aggregated_from_multiple_files"

        # 1. TETAP SIMPAN KE CSV
        output_path = os.path.join(processed_dir, 'social_media_analysis_summary.csv')
        df.to_csv(output_path, index=False)
        logging.info(f"Successfully generated social media analysis CSV with {len(df)} rows.")

        # 2. BARU: MUAT KE DATABASE STAGING
        table_name = 'social_media_analysis_summary'
        df.to_sql(table_name, con=db_engine, if_exists='replace', index=False, method='multi')
        logging.info(f"Successfully loaded data to staging DB table '{table_name}'.")

    except Exception as e:
        logging.error(f"An error occurred during social media processing: {e}", exc_info=True)


def parse_indonesian_currency(value_str):
    """Helper function untuk mengubah string '1.2 Triliun' menjadi angka."""
    if not isinstance(value_str, str): return None
    value_str = value_str.lower().replace('idr', '').strip()
    num_part_match = re.search(r'([\d.,]+)', value_str)
    if not num_part_match: return None
    num_str = num_part_match.group(1).replace('.', '').replace(',', '.')
    num = float(num_str)
    if 'triliun' in value_str: num *= 1_000_000_000_000
    elif 'miliar' in value_str: num *= 1_000_000_000
    elif 'juta' in value_str: num *= 1_000_000
    return int(num)


def process_financial_reports(file_list, processed_dir, db_engine):
    """Menerima daftar file laporan, mengekstrak, menyimpan ke CSV DAN memuat ke DB Staging."""
    logging.info(f"--- Processing {len(file_list)} Financial Report file(s) ---")
    try:
        extracted_data = []
        for file_path in file_list:
            content = ""
            filename = os.path.basename(file_path)
            logging.info(f"Extracting text from: {filename}")
            try:
                if file_path.endswith('.txt'):
                    with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
                elif file_path.endswith('.pdf'):
                    with fitz.open(file_path) as doc: content = "".join(page.get_text() for page in doc)
                elif file_path.endswith('.docx'):
                    doc = docx.Document(file_path)
                    content = "\n".join([p.text for p in doc.paragraphs])
            except Exception as extract_error:
                logging.error(f"Could not extract text from {filename}. Error: {extract_error}")
                continue

            if 'competitor' in filename.lower():
                pattern = re.compile(
                    r"Competitor\s+(?P<company_name>.+?)\s+-\s+Year:\s+(?P<year>\d{4}).*?"
                    r"Revenue:\s+IDR\s+(?P<revenue>[\d,]+).*?"
                    r"Net\s+Income:\s+IDR\s+(?P<net_income>[\d,]+)",
                    re.DOTALL | re.IGNORECASE)
                matches = pattern.finditer(content)
                for match in matches:
                    name_from_file = match.group('company_name').strip()
                    company_name = name_from_file if 'competitor' in name_from_file.lower() else f"Competitor {name_from_file}"
                    extracted_data.append({
                        'original_filename': filename,
                        'company_name': company_name,
                        'report_year': int(match.group('year')),
                        'report_type': 'Annual',
                        'extracted_revenue': int(match.group('revenue').replace(',', '')),
                        'extracted_net_profit': int(match.group('net_income').replace(',', ''))
                    })
            elif 'adventureworks' in filename.lower():
                flags = re.IGNORECASE | re.DOTALL
                year_match = re.search(r'tahun fiskal (\d{4})', content, flags)
                revenue_match = re.search(r'pendapatan kotor.*? (IDR [\d.,]+ (?:Triliun|Miliar|Juta))', content, flags)
                profit_match = re.search(r'laba bersih.*? (IDR [\d.,]+ (?:Triliun|Miliar|Juta))', content, flags)
                extracted_data.append({
                    'original_filename': filename, 'company_name': 'AdventureWorks',
                    'report_year': int(year_match.group(1)) if year_match else None,
                    'report_type': 'Annual',
                    'extracted_revenue': parse_indonesian_currency(revenue_match.group(1)) if revenue_match else None,
                    'extracted_net_profit': parse_indonesian_currency(profit_match.group(1)) if profit_match else None
                })
            elif 'market_report' in filename.lower():
                logging.info(f"Skipping financial data extraction for market report: {filename}")

        if not extracted_data:
            logging.warning("Could not extract any structured data from any of the financial reports.")
            return
            
        df = pd.DataFrame(extracted_data)
        df.dropna(subset=['report_year', 'extracted_revenue'], inplace=True)
        if df.empty:
            logging.warning("Financial reports data frame is empty after filtering. Nothing to save.")
            return

        # 1. TETAP SIMPAN KE CSV
        output_path = os.path.join(processed_dir, 'financial_reports_summary.csv')
        df.to_csv(output_path, index=False)
        logging.info(f"SUCCESS: Successfully generated financial reports summary CSV with {len(df)} rows.")

        # 2. BARU: MUAT KE DATABASE STAGING
        table_name = 'financial_reports_summary'
        df.to_sql(table_name, con=db_engine, if_exists='replace', index=False, method='multi')
        logging.info(f"Successfully loaded data to staging DB table '{table_name}'.")

    except Exception as e:
        logging.error(f"A critical error occurred during financial report processing: {e}", exc_info=True)

# --- FUNGSI ORKESTRATOR UTAMA ---

def analyze_all_datalake_data():
    """
    Menjelajahi semua file, mengklasifikasikannya, lalu mendelegasikan
    ke fungsi pemroses yang sesuai.
    """
    logging.info("====== STARTING DATA LAKE ANALYSIS (SMART CLASSIFICATION) ======")
    
    # Bagian ini tetap relevan untuk membersihkan file CSV lama
    logging.info("Cleaning up old processed files from local staging area...")
    for filename in os.listdir(PROCESSED_STAGING_DIR):
        try:
            os.remove(os.path.join(PROCESSED_STAGING_DIR, filename))
        except OSError as e:
            logging.error(f"Error removing file {os.path.join(PROCESSED_STAGING_DIR, filename)}: {e}")

    KEYWORD_MAP = {
        'sensors': ['sensor', 'gudang', 'pendingin', 'warehouse', 'temp'],
        'social': ['tweet', 'social_media', 'socmed'],
        'financial': ['financial', 'laporan', 'report', 'keuangan', 'market', 'competitor']
    }
    
    files_by_category = {cat: [] for cat in KEYWORD_MAP.keys()}
    
    logging.info("Walking through raw_data_lake to classify all files...")
    for dirpath, _, filenames in os.walk(RAW_DATA_LAKE_DIR):
        for filename in filenames:
            file_lower = filename.lower()
            full_path = os.path.join(dirpath, filename)
            for category, keywords in KEYWORD_MAP.items():
                if any(keyword in file_lower for keyword in keywords):
                    files_by_category[category].append(full_path)
                    break 

    logging.info(f"Classification result: "
                 f"{len(files_by_category['sensors'])} sensor files, "
                 f"{len(files_by_category['social'])} social media files, "
                 f"{len(files_by_category['financial'])} financial reports.")

    # Panggil fungsi proses dengan menyertakan engine database
    if files_by_category['sensors']:
        process_sensor_data(files_by_category['sensors'], PROCESSED_STAGING_DIR, engine_staging)
    if files_by_category['social']:
        process_social_media_data(files_by_category['social'], PROCESSED_STAGING_DIR, engine_staging)
    if files_by_category['financial']:
        process_financial_reports(files_by_category['financial'], PROCESSED_STAGING_DIR, engine_staging)
    
    logging.info("====== DATA LAKE ANALYSIS PROCESS COMPLETED ======")


if __name__ == '__main__':
    logging.info("Running analyze_datalake.py as a standalone script for testing.")
    analyze_all_datalake_data()