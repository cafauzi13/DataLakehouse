import os
import pandas as pd
import logging
from datetime import datetime
import PyPDF2 # pip install PyPDF2
from docx import Document # pip install python-docx
from textblob import TextBlob # pip install textblob
from collections import Counter
import re # Import modul regex baru yang dibutuhkan untuk ekstraksi finansial

# Konfigurasi Logging (akan diatur oleh main_orchestrator, tapi ada default untuk testing mandiri)
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

# --- Helper Functions untuk Ekstraksi Teks (TETAP SAMA) ---
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
                if page_text:
                    text += page_text + "\n"
        # Baris 'return text' harus berada di sini,
        # di dalam blok 'try' dan setelah semua operasi yang mungkin berhasil.
        return text 
    except PyPDF2.errors.PdfReadError: # Ini adalah penanganan error spesifik PyPDF2
        logging.error(f"Invalid PDF file or encrypted: {os.path.basename(file_path)}")
        return None
    except Exception as e: # Ini adalah penanganan error umum
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

# --- Analisis Spesifik (Revisi Bagian ini) ---
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
        
        # Convert date object to string for consistent CSV saving
        daily_summary['date'] = daily_summary['date'].astype(str)

        # Untuk memastikan semua data sensor terkumpul dalam satu CSV di processed_staging
        output_path = os.path.join(PROCESSED_STAGING_DIR, 'warehouse_daily_sensor_summary.csv')
        if os.path.exists(output_path):
            daily_summary.to_csv(output_path, mode='a', header=False, index=False, encoding='utf-8')
        else:
            daily_summary.to_csv(output_path, mode='w', header=True, index=False, encoding='utf-8')
        
        logging.info(f"Processed CSV data from {os.path.basename(file_path)} to {output_path}")
    except Exception as e:
        logging.error(f"Failed to analyze CSV {os.path.basename(file_path)}: {e}")

# Fungsi analyze_text_content yang sudah direvisi
def analyze_text_content(file_path):
    """
    Extracts text and performs basic analysis (word count, sentiment, financial extraction)
    for various text-based files.
    """
    full_text = extract_full_text_from_file(file_path)
    if not full_text:
        return

    file_name_lower = os.path.basename(file_path).lower()
    
    # Deteksi jenis file (social media atau report finansial/umum)
    is_social_media = 'tweet' in file_name_lower or 'komentar_pelanggan' in file_name_lower or 'adventureworks_tweets' in file_name_lower
    # Menambahkan deteksi untuk laporan finansial dan pasar
    is_financial_report = 'financial' in file_name_lower or 'annual_report' in file_name_lower or 'market_report' in file_name_lower

    # --- Bagian Analisis Teks Umum (Word Count) ---
    cleaned_text = ''.join(char.lower() if char.isalnum() else ' ' for char in full_text)
    words = [word for word in cleaned_text.split() if word.strip()]

    word_counts = Counter(words)
    common_words = word_counts.most_common(50)

    # --- Bagian Analisis Sosial Media (Sentiment) ---
    if is_social_media:
        blob = TextBlob(full_text)
        sentiment_score = blob.sentiment.polarity
        sentiment_category = 'Positive' if sentiment_score > 0.1 else ('Negative' if sentiment_score < -0.1 else 'Neutral')
        
        output_data = {
            'original_filename': os.path.basename(file_path),
            'date_processed': datetime.now().strftime('%Y-%m-%d'),
            'total_words': len(words),
            'sentiment_score': sentiment_score,
            'sentiment_category': sentiment_category,
            'top_words_json': str(common_words)
        }

        output_filename = 'social_media_analysis_summary.csv'
        output_path = os.path.join(PROCESSED_STAGING_DIR, output_filename)
        
        df_output = pd.DataFrame([output_data])
        if os.path.exists(output_path):
            df_output.to_csv(output_path, mode='a', header=False, index=False, encoding='utf-8')
        else:
            df_output.to_csv(output_path, mode='w', header=True, index=False, encoding='utf-8')
        
        logging.info(f"Processed social media data from {os.path.basename(file_path)} to {output_path}")

    # --- Bagian Analisis Laporan Finansial (NEWLY ADDED LOGIC) ---
    elif is_financial_report:
        company_name = "Unknown Company"
        report_type = "General"
        
        # Coba ekstrak nama perusahaan dari nama file atau konten (sesuai dummy Anda)
        if 'competitor_financials' in file_name_lower:
            company_name = "Competitor X" 
            report_type = "Annual"
        elif 'market_report' in file_name_lower:
            company_name = "Market Research Inc."
            report_type = "Quarterly"
        
        # Contoh ekstraksi pendapatan: mencari pola "Revenue: $X.X million" atau "Revenue increased by X%"
        # Ini sangat bergantung pada format teks dummy Anda. Anda mungkin perlu menyesuaikan regex.
        # Mencari angka setelah "Revenue", "Pendapatan", "Sales", atau "Total"
        revenue_match = re.search(r'(?:Revenue|Pendapatan|Sales|Total)\D*[\$€£]?[ \t]*([\d\.,]+)\s*(?:million|M|juta|b|Billion)?', full_text, re.IGNORECASE)
        
        extracted_revenue = None
        if revenue_match:
            revenue_str = revenue_match.group(1).replace('.', '').replace(',', '.') # Handle 1.000.000,00 -> 1000000.00
            try:
                extracted_revenue = float(revenue_str)
                # Skala jika ada 'million' atau 'billion'
                if 'million' in revenue_match.group(0).lower() or 'm' in revenue_match.group(0).lower():
                    extracted_revenue *= 1_000_000
                elif 'billion' in revenue_match.group(0).lower() or 'b' in revenue_match.group(0).lower():
                    extracted_revenue *= 1_000_000_000
            except ValueError:
                extracted_revenue = None # Gagal konversi

        # Contoh ekstraksi net profit (jika ada di dummy)
        net_profit_match = re.search(r'(?:Net Profit|Laba Bersih)\D*[\$€£]?[ \t]*([\d\.,]+)\s*(?:million|M|juta|b|Billion)?', full_text, re.IGNORECASE)
        extracted_net_profit = None
        if net_profit_match:
            net_profit_str = net_profit_match.group(1).replace('.', '').replace(',', '.')
            try:
                extracted_net_profit = float(net_profit_str)
                if 'million' in net_profit_match.group(0).lower() or 'm' in net_profit_match.group(0).lower():
                    extracted_net_profit *= 1_000_000
                elif 'billion' in net_profit_match.group(0).lower() or 'b' in net_profit_match.group(0).lower():
                    extracted_net_profit *= 1_000_000_000
            except ValueError:
                extracted_net_profit = None

        # Tanggal laporan (dari nama file atau tanggal proses)
        report_year = None
        date_match_filename = re.search(r'(\d{4})', file_name_lower) # Mencari 4 digit angka (tahun)
        if date_match_filename:
            report_year = int(date_match_filename.group(1)) # Ambil tahun

        output_data = {
            'original_filename': os.path.basename(file_path),
            'company_name': company_name,
            'report_year': report_year, # Tahun laporan
            'report_type': report_type,
            'extracted_revenue': extracted_revenue,
            'extracted_net_profit': extracted_net_profit,
            'total_words': len(words),
            'top_words_json': str(common_words)
        }

        output_filename = 'financial_reports_summary.csv'
        output_path = os.path.join(PROCESSED_STAGING_DIR, output_filename)
        
        df_output = pd.DataFrame([output_data])
        if os.path.exists(output_path):
            df_output.to_csv(output_path, mode='a', header=False, index=False, encoding='utf-8')
        else:
            df_output.to_csv(output_path, mode='w', header=True, index=False, encoding='utf-8')
        
        logging.info(f"Processed financial report from {os.path.basename(file_path)} to {output_path}")

    # --- Bagian untuk file teks generik yang bukan social media atau finansial ---
    else:
        # Ini akan menangani file TXT atau PDF/DOCX yang tidak terdeteksi sebagai social media atau laporan finansial.
        # Biasanya ini adalah laporan umum atau teks bebas lainnya.
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

    for file in os.listdir(RAW_LAKE_DIR):
        file_path = os.path.join(RAW_LAKE_DIR, file)
        if os.path.isfile(file_path):
            ext = os.path.splitext(file_path)[-1].lower()

            if ext == '.csv':
                # Pastikan ini hanya memproses file CSV sensor (sesuaikan dengan nama dummy Anda)
                if 'sensor' in file.lower():
                    analyze_csv_data(file_path)
                else:
                    logging.warning(f"Skipping unknown CSV file (not sensor data): {os.path.basename(file_path)}")
            elif ext in ('.txt', '.pdf', '.docx'):
                analyze_text_content(file_path)
            else:
                logging.warning(f"Skipping unsupported file type for analysis: {os.path.basename(file_path)}")

    logging.info("--- Data Lake Analysis Process Completed ---")

if __name__ == "__main__":
    analyze_all_datalake_data()