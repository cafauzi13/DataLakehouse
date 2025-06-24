import pandas as pd
from sqlalchemy import create_engine, text
import logging
import os
from datetime import datetime
from collections import Counter
import matplotlib
matplotlib.use('Agg')  # Mode non-interaktif, penting untuk server/otomasi
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# --- 1. Konfigurasi ---
pg_user = "postgres"
pg_pass = "**************"
pg_host = "localhost"
pg_port = "5432"
pg_dw_db = "adventureworks_dw"

# Setup Engine dan Logging
try:
    engine_dw = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_dw_db}")
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    LOG_DIR = os.path.join(SCRIPT_DIR, '..', 'logs')
    os.makedirs(LOG_DIR, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(LOG_DIR, f"api_interface_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")),
            logging.StreamHandler()
        ]
    )
    logging.info("API Interface script started and configured.")
except Exception as e:
    logging.critical(f"FATAL: Could not configure API Interface. Error: {e}")
    exit()

# --- 2. FUNGSI-FUNGSI PENGAMBILAN DATA (GETTERS) ---

def get_total_sales_by_product_category(year=None):
    # ... (Fungsi ini tidak berubah) ...
    logging.info(f"API: Fetching total sales for year {year or 'all years'}.")
    query = "SELECT dp.name AS product_category, SUM(fs.totalpenjualan) AS total_sales FROM fact_sales fs JOIN dim_product dp ON fs.productid = dp.productid JOIN dim_date dd ON fs.datekey = dd.datekey"
    params = {}
    if year:
        query += " WHERE dd.year = :year"
        params['year'] = year
    query += " GROUP BY dp.name ORDER BY total_sales DESC;"
    try:
        return pd.read_sql(text(query), engine_dw, params=params)
    except Exception as e:
        logging.error(f"API Error fetching sales data: {e}")
        return pd.DataFrame()

def get_all_warehouse_temperatures():
    # ... (Fungsi ini tidak berubah) ...
    logging.info(f"API: Fetching all warehouse temperature data.")
    query = "SELECT dd.fulldate AS measurement_date, dwz.zone_name, fwt.avg_temperature_c FROM fact_warehouse_temperature fwt JOIN dim_date dd ON fwt.datekey = dd.datekey JOIN dim_warehouse_zone dwz ON fwt.zone_id = dwz.zone_id;"
    try:
        return pd.read_sql(text(query), engine_dw)
    except Exception as e:
        logging.error(f"API Error fetching warehouse temperature data: {e}")
        return pd.DataFrame()

def get_word_frequency_data():
    # ... (Fungsi ini tidak berubah) ...
    logging.info(f"API: Fetching word frequency data for word cloud.")
    query = "SELECT top_words_json FROM fact_social_media_sentiment;"
    try:
        df = pd.read_sql(text(query), engine_dw)
        all_words_combined = Counter()
        for json_str in df['top_words_json'].dropna():
            try:
                word_list = eval(json_str)
                all_words_combined.update(dict(word_list))
            except Exception as e:
                logging.warning(f"Error parsing top_words_json string: {e}")
        return all_words_combined
    except Exception as e:
        logging.error(f"API Error fetching word frequency data: {e}")
        return Counter()

def get_financial_summary(exclude_companies=None):
    """
    Mengambil ringkasan finansial, dengan opsi untuk MENGECUALIKAN perusahaan tertentu.
    """
    logging.info(f"API: Fetching financial summary, excluding: {exclude_companies or 'None'}.")
    # Query dasar tetap sama
    query = """
    SELECT dd.fulldate AS report_date, dc.company_name, ff.revenue
    FROM fact_financial ff
    JOIN dim_date dd ON ff.datekey = dd.datekey
    JOIN dim_company dc ON ff.company_id = dc.company_id
    """
    params = {}
    
    # PERUBAHAN KUNCI: Menambahkan klausa WHERE untuk mengecualikan perusahaan
    if exclude_companies:
        query += " WHERE dc.company_name NOT IN :exclude_list"
        params['exclude_list'] = tuple(exclude_companies)
    
    try:
        return pd.read_sql(text(query), engine_dw, params=params)
    except Exception as e:
        logging.error(f"API Error fetching financial summary: {e}")
        return pd.DataFrame()

# --- 3. FUNGSI-FUNGSI GENERASI VISUALISASI (GENERATORS) ---

def generate_competitor_trend_chart(financial_data, output_dir):
    """
    Membuat SATU grafik garis gabungan yang membandingkan tren pendapatan
    HANYA untuk para kompetitor.
    """
    logging.info("Generating Line Chart: Competitor Revenue Trend...")
    if financial_data.empty:
        logging.warning("No competitor financial data to plot.")
        return

    df = financial_data.copy()
    df['report_date'] = pd.to_datetime(df['report_date'])
    df.sort_values(by=['company_name', 'report_date'], inplace=True)
    
    plt.figure(figsize=(14, 8))
    
    # Looping untuk setiap kompetitor unik dan plot di grafik yang sama
    for company in df['company_name'].unique():
        company_df = df[df['company_name'] == company]
        plt.plot(company_df['report_date'], company_df['revenue'], marker='o', linestyle='-', label=company)

    plt.title('Competitor Revenue Trend Comparison', fontsize=16)
    plt.xlabel('Report Date')
    plt.ylabel('Revenue (IDR)')
    plt.legend(title='Competitors')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.ticklabel_format(style='plain', axis='y') # Tampilkan angka penuh di sumbu Y
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, 'competitor_revenue_trend.png')
    plt.savefig(output_path)
    plt.close()
    logging.info(f"Competitor trend chart saved to {output_path}")

def generate_warehouse_temp_chart(temp_data, output_dir):
    # ... (Fungsi ini tidak berubah) ...
    logging.info("Generating Bar Chart: Average Warehouse Temperature per Zone...")
    if temp_data.empty: logging.warning("No temperature data to plot."); return
    avg_temp_per_zone = temp_data.sort_values('measurement_date').groupby('zone_name').last().reset_index()
    plt.figure(figsize=(12, 7)); plt.bar(avg_temp_per_zone['zone_name'], avg_temp_per_zone['avg_temperature_c'], color='skyblue')
    plt.xlabel('Warehouse Zone'); plt.ylabel('Average Temperature (°C)'); plt.title('Latest Average Warehouse Temperature per Zone')
    plt.grid(axis='y', linestyle='--'); plt.xticks(rotation=45, ha='right'); plt.tight_layout()
    output_path = os.path.join(output_dir, 'avg_warehouse_temp_bar_chart.png')
    plt.savefig(output_path); plt.close()
    logging.info(f"Warehouse temperature chart saved to {output_path}")

def generate_social_media_wordcloud(word_freq, output_dir):
    # ... (Fungsi ini tidak berubah) ...
    logging.info("Generating Word Cloud from Social Media Comments...")
    if not word_freq: logging.warning("No word frequency data to generate word cloud."); return
    wordcloud = WordCloud(width=1200, height=600, background_color='white', colormap='viridis').generate_from_frequencies(dict(word_freq.most_common(100)))
    output_path = os.path.join(output_dir, 'social_media_wordcloud.png')
    wordcloud.to_file(output_path)
    logging.info(f"Word Cloud saved to {output_path}")

# --- 4. BLOK EKSEKUSI UTAMA (UNTUK PENGUJIAN) ---

if __name__ == "__main__":
    logging.info("--- Running Standalone API Tests and Visualization Generation ---")

    # 1. Visualisasi Data Finansial (LOGIKA BARU)
    # Ambil data finansial, KECUALI AdventureWorks dan Market Report
    competitor_data = get_financial_summary(exclude_companies=['AdventureWorks', 'Market Report'])
    # Buat grafik HANYA untuk kompetitor
    generate_competitor_trend_chart(competitor_data, LOG_DIR)

    # 2. Visualisasi Data Gudang (Sama seperti sebelumnya)
    temp_data = get_all_warehouse_temperatures()
    generate_warehouse_temp_chart(temp_data, LOG_DIR)
    
    # 3. Visualisasi Data Media Sosial (Sama seperti sebelumnya)
    word_freq = get_word_frequency_data()
    generate_social_media_wordcloud(word_freq, LOG_DIR)
    
    logging.info("--- Standalone tests and visualization generation finished. ---")