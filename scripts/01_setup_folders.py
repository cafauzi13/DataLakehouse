import os

def setup_project_folders():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    folders_to_create = [
        # --- Folder untuk input_data_sources (berbasis tanggal) ---
        # Kita akan buat folder per tanggal dari file sensor
        os.path.join(project_root, 'input_data_sources', '2023-01-01'), # Untuk file 20230101
        os.path.join(project_root, 'input_data_sources', '2023-01-02'), # Untuk file 20230102
        os.path.join(project_root, 'input_data_sources', '2023-02-01'), # Untuk file 20230201
        os.path.join(project_root, 'input_data_sources', '2023-02-02'), # Untuk file 20230202

        # Untuk laporan dan tweet yang tidak spesifik tanggal harian, kita bisa letakkan di tanggal pertama di Q1
        # Atau Anda bisa membuat folder khusus 'reports/' dan 'social_media/' jika ingin lebih terpisah,
        # tapi untuk konsep 'daily dump', letakkan saja di tanggal tertentu.
        # Mari kita coba letakkan semua laporan Q1, 2022, Day1 di 2023-01-01
        # dan Q2, 2023, Day2 di 2023-01-02, dan Q3, Day3 di 2023-01-03 (jika ada)

        # --- Folder Data Lake dan lainnya (tetap sama seperti sebelumnya) ---
        os.path.join(project_root, 'raw_data_lake'),
        os.path.join(project_root, 'processed_staging'),
        os.path.join(project_root, 'logs'),
        os.path.join(project_root, 'scripts', 'utils'),
        os.path.join(project_root, 'documentation'),
    ]

    print("--- Setting up project folders ---")
    for folder in folders_to_create:
        try:
            if not os.path.exists(folder):
                os.makedirs(folder)
                print(f"  Created folder: {folder}")
            else:
                print(f"  Folder already exists: {folder}")
        except Exception as e:
            print(f"  Error creating folder {folder}: {e}")
    print("--- Folder setup complete ---")

if __name__ == "__main__":
    setup_project_folders()