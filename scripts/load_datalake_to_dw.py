import pandas as pd
from sqlalchemy import create_engine, text
import logging
import os
from datetime import datetime
from collections import Counter
from sqlalchemy.dialects import postgresql

print("DEBUG: load_datalake_to_dw.py script has started.")

# --- 1. Konfigurasi Database ---
pg_user = "postgres"
pg_pass = "*********" 
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
        logging.FileHandler(os.path.join(log_dir, f"load_datalake_to_dw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Konfigurasi Path
PROCESSED_STAGING_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'processed_staging')

# --- Fungsi DDL Placeholder ---
def create_datalake_dim_fact_tables():
    logging.info("Schema creation for Data Lake Star Schema tables handled by external SQL script.")
    pass

# --- Fungsi Load ke DW (Common) ---
def load_df_to_dw(df, table_name, pk_col=None):
    try:
        if pk_col:
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
        else:
            df.to_sql(table_name, engine_dw, if_exists='append', index=False)
            logging.info(f"Loaded {len(df)} rows into {table_name} in {pg_dw_db} (appended).")
    except Exception as e:
        logging.error(f"Failed to load {table_name} to {pg_dw_db}: {e}")

# --- Fungsi Utama untuk Memuat Data Lake ke DW ---
def load_all_datalake_data_to_dw():
    logging.info("--- Starting Data Lake Load to DW Process ---")

    create_datalake_dim_fact_tables() 

    # 1. Load warehouse temperature data (TETAP SAMA)
    try:
        temp_csv_path = os.path.join(PROCESSED_STAGING_DIR, 'warehouse_daily_sensor_summary.csv')
        if os.path.exists(temp_csv_path):
            df_temp = pd.read_csv(temp_csv_path, parse_dates=['date'])
            
            with engine_dw.connect() as conn_dw:
                df_zones_in_db = pd.read_sql("SELECT zone_id, zone_name FROM dim_warehouse_zone", conn_dw)
                new_zones_from_data = df_temp[~df_temp['zone_id'].isin(df_zones_in_db['zone_name'])]['zone_id'].unique()
                for zone_name in new_zones_from_data:
                    conn_dw.execute(text(f"INSERT INTO dim_warehouse_zone (zone_name) VALUES (:zone_name) ON CONFLICT (zone_name) DO NOTHING;"), {'zone_name': zone_name})
                conn_dw.commit()

                df_zones_updated = pd.read_sql("SELECT zone_id, zone_name FROM dim_warehouse_zone", conn_dw)
                df_temp_merged = pd.merge(df_temp, df_zones_updated, left_on='zone_id', right_on='zone_name', how='left')
                df_temp_merged['zone_id_fk'] = df_temp_merged['zone_id_y'] 
                df_temp_merged = df_temp_merged.drop(columns=['zone_id_y', 'zone_name'])

                df_temp_merged['datekey'] = df_temp_merged['date'].dt.strftime('%Y%m%d').astype(int)

                data_to_insert_fact_temp = df_temp_merged[['datekey', 'zone_id_fk', 'avg_temperature_c', 'avg_humidity_percent']].to_dict(orient='records')
                insert_fact_temp_stmt = text("""
                    INSERT INTO fact_warehouse_temperature (datekey, zone_id, avg_temperature_c, avg_humidity_percent)
                    VALUES (:datekey, :zone_id_fk, :avg_temperature_c, :avg_humidity_percent);
                """)
                with engine_dw.connect() as conn:
                    conn.execute(insert_fact_temp_stmt, data_to_insert_fact_temp)
                    conn.commit()
            logging.info("Warehouse temperature data loaded to DW.")
        else:
            logging.warning(f"Warehouse temperature summary file not found: {temp_csv_path}")
    except Exception as e:
        logging.error(f"Failed to load warehouse temperature data to DW: {e}")

    # 2. Load social media sentiment data (PERUBAHAN DI SINI)
    try:
        sentiment_csv_path = os.path.join(PROCESSED_STAGING_DIR, 'social_media_analysis_summary.csv')
        if os.path.exists(sentiment_csv_path):
            df_sentiment = pd.read_csv(sentiment_csv_path)
            
            with engine_dw.connect() as conn_dw:
                df_sentiment_dim = pd.read_sql("SELECT sentiment_id, category_name FROM dim_sentiment_category", conn_dw)
                df_sentiment_merged = pd.merge(df_sentiment, df_sentiment_dim, left_on='sentiment_category', right_on='category_name', how='left')
                
                # REVISI PENTING: Akses kolom 'sentiment_id' langsung setelah merge
                df_sentiment_merged['sentiment_id_fk'] = df_sentiment_merged['sentiment_id'].fillna(-1).astype(int) 
                # Tidak perlu drop columns seperti 'sentiment_id_y' jika Anda tidak membuat kolom itu secara eksplisit
                
                df_sentiment_merged['datekey'] = pd.to_datetime(df_sentiment_merged['date_processed']).dt.strftime('%Y%m%d').astype(int)

                sentiment_agg = df_sentiment_merged.groupby(['datekey', 'sentiment_id_fk']).agg(
                    tweet_count=('original_filename', 'count'),
                    avg_sentiment_score=('sentiment_score', 'mean'),
                    top_words_list=('top_words_json', lambda x: [eval(item) for item in x if item is not None])
                ).reset_index()

                sentiment_agg['combined_top_words'] = sentiment_agg['top_words_list'].apply(
                    lambda list_of_word_counts: str(Counter(word for sublist in list_of_word_counts for word, count in sublist).most_common(50))
                )
                
                data_to_insert_fact_sentiment = sentiment_agg[['datekey', 'sentiment_id_fk', 'tweet_count', 'avg_sentiment_score', 'combined_top_words']].to_dict(orient='records')
                insert_fact_sentiment_stmt = text("""
                    INSERT INTO fact_social_media_sentiment (datekey, sentiment_id, tweet_count, avg_sentiment_score, top_words_json)
                    VALUES (:datekey, :sentiment_id_fk, :tweet_count, :avg_sentiment_score, :combined_top_words);
                """)
                with engine_dw.connect() as conn:
                    conn.execute(insert_fact_sentiment_stmt, data_to_insert_fact_sentiment)
                    conn.commit()
            logging.info("Social media sentiment data loaded to DW.")
        else:
            logging.warning(f"Social media analysis summary file not found: {sentiment_csv_path}")
    except Exception as e:
        logging.error(f"Failed to load social media sentiment data to DW: {e}")

    # 3. Load financial reports data (PERUBAHAN DI SINI)
    try:
        financial_csv_path = os.path.join(PROCESSED_STAGING_DIR, 'financial_reports_summary.csv')
        if os.path.exists(financial_csv_path):
            df_financial = pd.read_csv(financial_csv_path)
            
            with engine_dw.connect() as conn_dw:
                df_financial['report_date'] = df_financial['report_year'].astype(str) + '-01-01'
                df_financial['datekey'] = pd.to_datetime(df_financial['report_date']).dt.strftime('%Y%m%d').astype(int)

                companies_to_insert = df_financial[['company_name']].drop_duplicates().to_dict(orient='records')
                insert_company_stmt = text("""
                    INSERT INTO dim_company (company_name)
                    VALUES (:company_name)
                    ON CONFLICT (company_name) DO NOTHING;
                """)
                conn_dw.execute(insert_company_stmt, companies_to_insert)
                conn_dw.commit()
                logging.info("Companies loaded to dim_company (ON CONFLICT DO NOTHING).")

                df_companies_in_db = pd.read_sql("SELECT company_id, company_name FROM dim_company", conn_dw)
                df_financial_merged = pd.merge(df_financial, df_companies_in_db, on='company_name', how='left')
                
                # REVISI PENTING: Akses kolom 'company_id' langsung setelah merge
                df_financial_merged['company_id_fk'] = df_financial_merged['company_id'].fillna(-1).astype(int)
                # Tidak perlu drop columns seperti 'company_id_y'
                
                data_to_insert_fact_financial = df_financial_merged[['datekey', 'company_id_fk', 'extracted_revenue', 'extracted_net_profit', 'report_type']].to_dict(orient='records')
                
                data_to_insert_fact_financial = [
                    row for row in data_to_insert_fact_financial 
                    if row['extracted_revenue'] is not None
                ]

                insert_fact_financial_stmt = text("""
                    INSERT INTO fact_financial (datekey, company_id, revenue, net_profit, report_type)
                    VALUES (:datekey, :company_id_fk, :extracted_revenue, :extracted_net_profit, :report_type);
                """)
                with engine_dw.connect() as conn:
                    conn.execute(insert_fact_financial_stmt, data_to_insert_fact_financial)
                    conn.commit()
                logging.info(f"Financial reports data loaded to DW.")
            logging.info("Financial reports data loaded to DW.")
        else:
            logging.warning(f"Financial reports summary file not found: {financial_csv_path}")
    except Exception as e:
        logging.error(f"Failed to load financial reports data to DW: {e}")

    logging.info("--- Data Lake Load to DW Process Completed ---")

if __name__ == "__main__":
    logging.warning("Running load_datalake_to_dw.py as standalone script. Ensure database schemas are pre-created and dim_date is populated.")
    create_datalake_dim_fact_tables() 
    load_all_datalake_data_to_dw()