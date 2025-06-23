import os
import pandas as pd
import logging
from datetime import datetime
import PyPDF2 # pip install PyPDF2
from docx import Document # pip install python-docx
from textblob import TextBlob # pip install textblob
from collections import Counter
# from wordcloud import WordCloud # opsional, jika ingin generate word cloud secara langsung

# Konfigurasi Logging (akan diatur oleh main_orchestrator, tapi ada default untuk testing mandiri)
# Ini akan memastikan script bisa diuji sendiri tanpa error logging jika belum ada orkestrator
log_dir_for_testing = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
if not os.path.exists(log_dir_for_testing):
    os.makedirs(log_dir_for_testing, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir_for_testing, f"analyze_datalake_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Konfigurasi Path
RAW_LAKE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'raw_data_lake')
PROCESSED_STAGING_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'processed_staging')

# --- Helper Functions untuk Ekstraksi Teks ---
def _extract_from_txt(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logging.error(f"Error extracting from TXT {os.path.basename(file_path)}: {e}")
        return None

def _extract_from_pdf(file_path):
    text = ""
    try:
        with open(file_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            for page_num in range(len(reader.pages)):
                page_text = reader.pages[page_num].extract_text()
                if page_text: # Tambahkan cek, kadang extract_text() bisa None
                    text += page_text + "\n"
        return text
    except Exception as e:
        logging.error(f"Error extracting from PDF {os.path.basename(file_path)}: {e}")
        return None

def _extract_from_docx(file_path):
    text = ""
    try:
        document = Document(file_path)
        for para in document.paragraphs:
            text += para.text + "\n"
        return text
    except Exception as e:
        logging.error(f"Error extracting from DOCX {os.path.basename(file_path)}: {e}")
        return None

def extract_full_text_from_file(file_path):
    """Dispatcher function to extract text based on file extension."""
    ext = os.path.splitext(file_path)[-1].lower()
    if ext == '.txt':
        return _extract_from_txt(file_path)
    elif ext == '.pdf':
        return _extract_from_pdf(file_path)
    elif ext == '.docx':
        return _extract_from_docx(file_path)
    else:
        logging.warning(f"Unsupported text file type for extraction: {os.path.basename(file_path)}")
        return None

# --- Analisis Spesifik ---
def analyze_csv_data(file_path):
    """Processes warehouse temperature sensor CSV data."""
    try:
        df = pd.read_csv(file_path, parse_dates=['timestamp'])
        
        # Agregasi: hitung rata-rata suhu dan kelembaban per hari dan per zona
        # Asumsi nama kolom di CSV dummy Anda: 'timestamp', 'zone_id', 'temperature_c', 'humidity_percent'
        daily_summary = df.groupby([df['timestamp'].dt.date, 'zone_id']).agg(
            avg_temperature_c=('temperature_c', 'mean'),
            avg_humidity_percent=('humidity_percent', 'mean')
        ).reset_index()
        daily_summary.rename(columns={'timestamp': 'date'}, inplace=True)
        
        output_path = os.path.join(PROCESSED_STAGING_DIR, 'warehouse_daily_sensor_summary.csv')
        daily_summary.to_csv(output_path, index=False)
        logging.info(f"Processed CSV data from {os.path.basename(file_path)} to {output_path}")
    except Exception as e:
        logging.error(f"Failed to analyze CSV {os.path.basename(file_path)}: {e}")

def analyze_text_content(file_path):
    """
    Extracts text and performs basic analysis (word count, sentiment)
    for various text-based files. Differentiates based on file name or content hints.
    """
    full_text = extract_full_text_from_file(file_path)
    if not full_text:
        return

    file_name_lower = os.path.basename(file_path).lower()
    
    # Determine if it's social media data based on filename hint
    is_social_media = 'tweet' in file_name_lower or 'komentar_pelanggan' in file_name_lower or 'adventureworks_tweets' in file_name_lower

    # Basic text cleaning (remove punctuation, lower case)
    # Menambahkan spasi agar kata tidak menyatu setelah penghapusan non-alphanumeric
    cleaned_text = ''.join(char.lower() if char.isalnum() else ' ' for char in full_text)
    words = [word for word in cleaned_text.split() if word.strip()] # Memastikan tidak ada kata kosong

    # Word Frequency
    word_counts = Counter(words)
    common_words = word_counts.most_common(50) # Top 50 words

    output_data = {
        'original_filename': os.path.basename(file_path),
        'date_processed': datetime.now().strftime('%Y-%m-%d'),
        'total_words': len(words),
        'top_words_json': str(common_words) # Convert list of tuples to string for CSV
    }

    if is_social_media:
        blob = TextBlob(full_text)
        sentiment_score = blob.sentiment.polarity # -1 (negative) to 1 (positive)
        if sentiment_score > 0.1:
            sentiment_category = 'Positive'
        elif sentiment_score < -0.1:
            sentiment_category = 'Negative'
        else:
            sentiment_category = 'Neutral'
        
        output_data['sentiment_score'] = sentiment_score
        output_data['sentiment_category'] = sentiment_category

        # Simpan hasil untuk social media (bisa di-append ke satu file CSV)
        output_filename = 'social_media_analysis_summary.csv'
        output_path = os.path.join(PROCESSED_STAGING_DIR, output_filename)
        
        # Buat DataFrame, cek apakah file sudah ada untuk append
        df_output = pd.DataFrame([output_data])
        if os.path.exists(output_path):
            # Memastikan header tidak ditulis ulang setiap kali append
            df_output.to_csv(output_path, mode='a', header=False, index=False, encoding='utf-8')
        else:
            df_output.to_csv(output_path, mode='w', header=True, index=False, encoding='utf-8')
        
        logging.info(f"Processed social media data from {os.path.basename(file_path)} to {output_path}")
    else: # Ini untuk laporan PDF/DOCX/TXT generik (non-social media)
        # Untuk laporan, kita bisa ekstrak teks dan juga simpan top words
        output_text_filename = os.path.basename(file_path).replace(os.path.splitext(file_path)[-1], '_extracted_text.txt')
        output_text_path = os.path.join(PROCESSED_STAGING_DIR, output_text_filename)
        with open(output_text_path, 'w', encoding='utf-8') as f:
            f.write(full_text)
        
        output_word_counts_filename = os.path.basename(file_path).replace(os.path.splitext(file_path)[-1], '_word_counts.csv')
        output_word_counts_path = os.path.join(PROCESSED_STAGING_DIR, output_word_counts_filename)
        pd.DataFrame(common_words, columns=['word', 'count']).to_csv(output_word_counts_path, index=False, encoding='utf-8')
        
        logging.info(f"Extracted text and word counts from {os.path.basename(file_path)} to {PROCESSED_STAGING_DIR}")

def analyze_all_datalake_data():
    """Orchestrates the analysis of all raw data in the data lake (single folder)."""
    logging.info("--- Starting Data Lake Analysis Process ---")

    # Pastikan folder output ada
    if not os.path.exists(PROCESSED_STAGING_DIR):
        os.makedirs(PROCESSED_STAGING_DIR, exist_ok=True)

    # Hapus file hasil analisis sebelumnya di processed_staging untuk demo bersih
    for f in os.listdir(PROCESSED_STAGING_DIR):
        file_to_delete = os.path.join(PROCESSED_STAGING_DIR, f)
        try:
            if os.path.isfile(file_to_delete):
                os.remove(file_to_delete)
        except Exception as e:
            logging.error(f"Failed to clean up {file_to_delete}: {e}")
    logging.info(f"Cleaned up existing files in {PROCESSED_STAGING_DIR}.")

    # Iterasi langsung di root RAW_LAKE_DIR karena file sudah tercampur
    for file in os.listdir(RAW_LAKE_DIR):
        file_path = os.path.join(RAW_LAKE_DIR, file)
        if os.path.isfile(file_path): # Pastikan itu file, bukan sub-folder jika ada
            ext = os.path.splitext(file_path)[-1].lower()

            if ext == '.csv':
                if 'sensor' in file.lower(): # Identifikasi file CSV sensor
                    analyze_csv_data(file_path)
                else:
                    logging.warning(f"Skipping unknown CSV file (not sensor data): {os.path.basename(file_path)}")
            elif ext in ('.txt', '.pdf', '.docx'): # Proses semua tipe teks di fungsi yang sama
                analyze_text_content(file_path)
            else:
                logging.warning(f"Skipping unsupported file type for analysis: {os.path.basename(file_path)}")

    logging.info("--- Data Lake Analysis Process Completed ---")

if __name__ == "__main__":
    analyze_all_datalake_data()
