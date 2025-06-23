import pandas as pd
from sqlalchemy import create_engine, text
import logging
import os
from datetime import datetime
from collections import Counter
from sqlalchemy.dialects import postgresql # Tidak langsung digunakan di API, tapi bagus ada untuk tipe data
import matplotlib.pyplot as plt # Import untuk plotting
from wordcloud import WordCloud # Import untuk word cloud

# --- 1. Konfigurasi Database ---
pg_user = "postgres"
pg_pass = "**********"
pg_host = "localhost"
pg_port = "5432"
pg_dw_db = "adventureworks_dw"

engine_dw = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_dw_db}")

# --- 2. Konfigurasi Logging ---
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, f"api_interface_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- 3. Fungsi-fungsi API untuk Akses Data ---

def get_total_sales_by_product_category(year=None):
    """
    Mengambil total penjualan per kategori produk dari DW (data AdventureWorks).
    """
    logging.info(f"API: Fetching total sales by product category for year {year if year else 'all years'}.")
    query = """
    SELECT 
        dp.name AS product_category, 
        SUM(fs.totalpenjualan) AS total_sales
    FROM fact_sales fs
    JOIN dim_product dp ON fs.productid = dp.productid
    JOIN dim_date dd ON fs.datekey = dd.datekey
    """
    params = {}
    if year:
        query += " WHERE dd.year = :year"
        params['year'] = year
    query += " GROUP BY dp.name ORDER BY total_sales DESC;"

    try:
        df = pd.read_sql(text(query), engine_dw, params=params)
        logging.info(f"API: Fetched {len(df)} rows.")
        return df.to_dict(orient='records')
    except Exception as e:
        logging.error(f"API Error: Failed to fetch total sales by product category: {e}")
        return []

def get_average_warehouse_temperature(zone_name=None, date=str(datetime.now().date())): # Default ke hari ini
    """
    Mengambil rata-rata suhu gudang dari DW (data Data Lake).
    """
    logging.info(f"API: Fetching average warehouse temperature for zone '{zone_name}' on date '{date}'.")
    query = """
    SELECT 
        dd.fulldate AS measurement_date, 
        dwz.zone_name, 
        fwt.avg_temperature_c, 
        fwt.avg_humidity_percent
    FROM fact_warehouse_temperature fwt
    JOIN dim_date dd ON fwt.datekey = dd.datekey
    JOIN dim_warehouse_zone dwz ON fwt.zone_id = dwz.zone_id
    WHERE 1=1
    """
    params = {}
    if zone_name:
        query += " AND dwz.zone_name = :zone_name"
        params['zone_name'] = zone_name
    if date:
        query += " AND dd.fulldate = :full_date_str" 
        params['full_date_str'] = str(date)
    
    try:
        df = pd.read_sql(text(query), engine_dw, params=params)
        logging.info(f"API: Fetched {len(df)} rows.")
        if 'measurement_date' in df.columns:
            df['measurement_date'] = df['measurement_date'].astype(str)
        return df.to_dict(orient='records')
    except Exception as e:
        logging.error(f"API Error: Failed to fetch average warehouse temperature: {e}")
        return []

def get_sentiment_analysis_summary(sentiment_category=None):
    """
    Mengambil ringkasan analisis sentimen dari DW (data Data Lake).
    """
    logging.info(f"API: Fetching sentiment analysis summary for category '{sentiment_category}'.")
    query = """
    SELECT 
        dsc.category_name AS sentiment, 
        SUM(fsm.tweet_count) AS total_tweets, 
        AVG(fsm.avg_sentiment_score) AS average_score
    FROM fact_social_media_sentiment fsm
    JOIN dim_sentiment_category dsc ON fsm.sentiment_id = dsc.sentiment_id
    WHERE 1=1
    """
    params = {}
    if sentiment_category:
        query += " AND dsc.category_name = :sentiment_category"
        params['sentiment_category'] = sentiment_category
    query += " GROUP BY dsc.category_name ORDER BY total_tweets DESC;"
    
    try:
        df = pd.read_sql(text(query), engine_dw, params=params)
        logging.info(f"API: Fetched {len(df)} rows.")
        return df.to_dict(orient='records')
    except Exception as e:
        logging.error(f"API Error: Failed to fetch sentiment analysis summary: {e}")
        return []

def get_word_frequency_data(sentiment_category=None, limit=50):
    """
    Mengambil data frekuensi kata dari DW (data Data Lake) untuk Word Cloud.
    Menggabungkan top_words_json dari fakta sentimen.
    """
    logging.info(f"API: Fetching word frequency data for word cloud (category: {sentiment_category}).")
    query = """
    SELECT fsm.top_words_json
    FROM fact_social_media_sentiment fsm
    JOIN dim_sentiment_category dsc ON fsm.sentiment_id = dsc.sentiment_id
    WHERE 1=1
    """
    params = {}
    if sentiment_category:
        query += " AND dsc.category_name = :sentiment_category"
        params['sentiment_category'] = sentiment_category

    try:
        df = pd.read_sql(text(query), engine_dw, params=params)
        
        all_words_combined = Counter()
        for json_str in df['top_words_json']:
            if json_str:
                try:
                    word_list = eval(json_str) 
                    for word, count in word_list:
                        all_words_combined[word] += count
                except Exception as e:
                    logging.warning(f"Error parsing top_words_json string: {e} - Data: {json_str[:50]}...")
        
        return all_words_combined.most_common(limit)

    except Exception as e:
        logging.error(f"API Error: Failed to fetch word frequency data: {e}")
        return []


def get_financial_summary(company_name=None, report_year=None, report_type=None):
    """
    Mengambil ringkasan laporan finansial dari DW (data Data Lake).
    """
    logging.info(f"API: Fetching financial summary for company '{company_name}', year '{report_year}', type '{report_type}'.")
    query = """
    SELECT
        dd.fulldate AS report_date,
        dc.company_name,
        ff.revenue,
        ff.net_profit,
        ff.report_type
    FROM fact_financial ff
    JOIN dim_date dd ON ff.datekey = dd.datekey
    JOIN dim_company dc ON ff.company_id = dc.company_id
    WHERE 1=1
    """
    params = {}
    if company_name:
        query += " AND dc.company_name = :company_name"
        params['company_name'] = company_name
    if report_year:
        query += " AND dd.year = :report_year"
        params['report_year'] = report_year
    if report_type:
        query += " AND ff.report_type = :report_type"
        params['report_type'] = report_type
    query += " ORDER BY dd.fulldate DESC;"

    try:
        df = pd.read_sql(text(query), engine_dw, params=params)
        logging.info(f"API: Fetched {len(df)} rows.")
        if 'report_date' in df.columns:
            df['report_date'] = df['report_date'].astype(str)
        return df.to_dict(orient='records')
    except Exception as e:
        logging.error(f"API Error: Failed to fetch financial summary: {e}")
        return []


# --- Bagian untuk Pengujian Visualisasi ---
if __name__ == "__main__":
    print("\n--- Testing API Functions (Standalone) and Generating Visualizations ---")
    
    # 1. Total Sales by Product Category (2010) - Contoh dari AdventureWorks ETL
    print("\nTotal Sales by Product Category (2010) from AdventureWorks DW:")
    sales_2010 = get_total_sales_by_product_category(year=2010)
    if sales_2010:
        for row in sales_2010:
            print(f"  {row.get('product_category', 'N/A')}: {row.get('total_sales', 0):.2f}")
    else:
        print("  No sales data found for 2010 or API error.")

    # 2. Average Warehouse Temperature - Bar Chart
    print("\nGenerating Bar Chart: Average Warehouse Temperature per Zone...")
    # Ambil data suhu dari semua zona yang ada di tahun 2023
    all_temp_data = get_average_warehouse_temperature(date='2023-01-01') # Mengambil data untuk satu hari sebagai contoh
    if all_temp_data:
        # Konversi ke DataFrame untuk plotting yang mudah
        df_temp_plot = pd.DataFrame(all_temp_data)
        
        # Agregasi untuk rata-rata per zona jika data dari berbagai hari
        # Jika get_average_warehouse_temperature sudah mengembalikan rata-rata per hari per zona, ini cukup sederhana
        # Untuk bar chart "per Sensor" seperti contoh teman, Anda perlu mengagregasi data per 'zone_name'
        avg_temp_per_zone = df_temp_plot.groupby('zone_name')['avg_temperature_c'].mean().reset_index()

        plt.figure(figsize=(10, 6))
        plt.bar(avg_temp_per_zone['zone_name'], avg_temp_per_zone['avg_temperature_c'], color='skyblue')
        plt.xlabel('Zone')
        plt.ylabel('Average Temperature (°C)')
        plt.title('Average Warehouse Temperature per Zone')
        plt.grid(axis='y', linestyle='--')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(log_dir, 'avg_warehouse_temp_bar_chart.png')) # Simpan chart
        # plt.show() # Tampilkan chart (hapus atau komentari ini saat demo, cukup simpan saja)
        print(f"  Bar chart saved to {os.path.join(log_dir, 'avg_warehouse_temp_bar_chart.png')}")
    else:
        print("  No temperature data found for plotting or API error.")

    # 3. Sentiment Analysis Summary
    print("\nSentiment Analysis Summary (Positive):")
    sentiment_positive = get_sentiment_analysis_summary(sentiment_category='Positive')
    if sentiment_positive:
        for row in sentiment_positive:
            print(f"  {row.get('sentiment', 'N/A')}: Tweets: {row.get('total_tweets', 0)}, Avg Score: {row.get('average_score', 0):.2f}")
    else:
        print("  No positive sentiment data found or API error.")

    # 4. Word Cloud from Social Media Comments
    print("\nGenerating Word Cloud from Social Media Comments...")
    word_freq = get_word_frequency_data(limit=50) # Ambil 50 kata teratas
    if word_freq:
        words_dict = dict(word_freq) # Konversi list of tuples ke dictionary
        wordcloud = WordCloud(width=800, height=400, background_color='white', colormap='viridis').generate_from_frequencies(words_dict)
        
        wordcloud_filename = os.path.join(log_dir, 'social_media_wordcloud.png')
        wordcloud.to_file(wordcloud_filename) # Simpan word cloud
        # plt.imshow(wordcloud, interpolation='bilinear') # Tampilkan word cloud
        # plt.axis('off')
        # plt.show() # Tampilkan chart (hapus atau komentari ini saat demo, cukup simpan saja)
        print(f"  Word Cloud saved to {wordcloud_filename}")
    else:
        print("  No word frequency data found for word cloud.")

    # 5. Financial Report Trend - Line Chart
    print("\nGenerating Line Chart: Financial Report Revenue Trend...")
    # Mengambil data finansial untuk plotting trend
    # Ini akan mengambil semua laporan finansial dari DW
    all_financial_data = get_financial_summary() 
    if all_financial_data:
        df_financial_plot = pd.DataFrame(all_financial_data)
        
        # Pastikan kolom 'report_date' adalah datetime untuk sorting
        df_financial_plot['report_date'] = pd.to_datetime(df_financial_plot['report_date'])
        df_financial_plot.sort_values(by=['company_name', 'report_date'], inplace=True)

        plt.figure(figsize=(12, 7))
        for company in df_financial_plot['company_name'].unique():
            company_data = df_financial_plot[df_financial_plot['company_name'] == company]
            plt.plot(company_data['report_date'], company_data['revenue'], marker='o', label=company)
        
        plt.xlabel('Report Date')
        plt.ylabel('Revenue')
        plt.title('Revenue Trend per Company')
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(log_dir, 'revenue_trend_line_chart.png')) # Simpan chart
        # plt.show() # Tampilkan chart (hapus atau komentari ini saat demo, cukup simpan saja)
        print(f"  Line chart saved to {os.path.join(log_dir, 'revenue_trend_line_chart.png')}")
    else:
        print("  No financial data found for plotting or API error.")

    print("\n--- All API tests and visualizations generated. ---")