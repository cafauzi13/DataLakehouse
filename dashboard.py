# dashboard.py

import streamlit as st
import pandas as pd
import sys
import os

# --- Konfigurasi Path agar bisa mengimpor dari folder 'scripts' ---
try:
    # Asumsi file dashboard.py ada di folder root proyek
    project_root = os.path.dirname(os.path.abspath(__file__))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    
    from scripts import api_interface
except ImportError as e:
    st.error(f"Gagal mengimpor modul dari folder 'scripts'. Pastikan file ini ada di folder root proyek. Error: {e}")
    st.stop()

# --- Konfigurasi Halaman Dasbor ---
st.set_page_config(
    layout="wide",
    page_title="DataLakehouse Dashboard",
    page_icon="📊"
)

# --- Fungsi Cache untuk Performa ---
# Mencegah query berulang ke database setiap kali ada interaksi
@st.cache_data
def load_financial_data():
    return api_interface.get_financial_summary()

@st.cache_data
def load_warehouse_data():
    return api_interface.get_all_warehouse_temperatures()

@st.cache_data
def load_word_frequency():
    return api_interface.get_word_frequency_data().most_common(100)

# --- Tampilan Utama Dasbor ---

st.title("📊 Dashboard Analisis DataLakehouse")
st.markdown("Dasbor ini memvisualisasikan data yang telah diolah dari berbagai sumber melalui pipeline ETL.")

# --- Sidebar untuk Filter ---
st.sidebar.title("⚙️ Filter & Opsi")
st.sidebar.info("Gunakan filter di bawah ini untuk berinteraksi dengan data di halaman utama.")

# --- Konten Dasbor dengan Tab ---
tab1, tab2, tab3 = st.tabs(["📈 Analisis Finansial", "🌡️ Analisis Gudang", "🐦 Analisis Media Sosial"])

# --- Tab 1: Finansial ---
with tab1:
    st.header("Perbandingan Pendapatan Antar Perusahaan")
    
    financial_data = load_financial_data()

    if not financial_data.empty:
        all_companies = sorted(financial_data['company_name'].unique())
        default_competitors = [c for c in all_companies if 'competitor' in c.lower()]
        
        selected_companies = st.sidebar.multiselect(
            'Pilih Perusahaan untuk Ditampilkan:',
            options=all_companies,
            default=default_competitors
        )

        if not selected_companies:
            st.warning("Silakan pilih minimal satu perusahaan di sidebar untuk menampilkan grafik.")
        else:
            df_plot = financial_data[financial_data['company_name'].isin(selected_companies)]

            # --- KUNCI PERBAIKAN DI SINI ---
            # Menggunakan parameter 'x', 'y', dan 'color' yang benar.
            st.line_chart(
                data=df_plot,
                x='report_date',        # Nama kolom untuk sumbu-X
                y='revenue',            # Nama kolom untuk sumbu-Y
                color='company_name'    # Nama kolom untuk membedakan warna garis
            )
            # --------------------------------
            
            with st.expander("Lihat Data Mentah Finansial yang Ditampilkan"):
                st.dataframe(df_plot)
    else:
        st.error("Gagal memuat data finansial dari database.")

# --- Tab 2: Gudang ---
with tab2:
    st.header("Suhu Rata-rata Terakhir per Zona Gudang")
    
    warehouse_data = load_warehouse_data()

    if not warehouse_data.empty:
        latest_temps = warehouse_data.sort_values('measurement_date').groupby('zone_name').last().reset_index()
        
        st.bar_chart(
            data=latest_temps,
            x='zone_name',
            y='avg_temperature_c'
        )

        with st.expander("Lihat Semua Data Temperatur"):
            st.dataframe(warehouse_data)
    else:
        st.error("Gagal memuat data temperatur gudang dari database.")

# --- Tab 3: Media Sosial ---
with tab3:
    st.header("Analisis Kata yang Paling Sering Muncul")
    
    word_freq_list = load_word_frequency()

    if word_freq_list:
        st.subheader("Word Cloud")
        
        word_freq_dict = dict(word_freq_list)
        # Menampilkan word cloud secara langsung jika library-nya ada,
        # jika tidak, tampilkan bar chart sebagai alternatif.
        try:
            from wordcloud import WordCloud
            import matplotlib.pyplot as plt

            wordcloud = WordCloud(width=800, height=400, background_color='white', colormap='viridis').generate_from_frequencies(word_freq_dict)
            fig, ax = plt.subplots()
            ax.imshow(wordcloud, interpolation='bilinear')
            ax.axis('off')
            st.pyplot(fig)

        except ImportError:
            st.warning("Library 'wordcloud' dan 'matplotlib' tidak terinstall. Menampilkan data sebagai bar chart.")
            st.bar_chart(pd.DataFrame.from_dict(word_freq_dict, orient='index', columns=['Frekuensi']))

        with st.expander("Lihat Detail Frekuensi Kata"):
            st.dataframe(pd.DataFrame(word_freq_list, columns=['Kata', 'Frekuensi']))
    else:
        st.error("Gagal memuat data frekuensi kata dari database.")