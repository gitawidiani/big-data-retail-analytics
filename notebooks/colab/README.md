# Menjalankan proyek di Google Colab

Versi ini tetap memakai satu repository GitHub yang sama dengan versi lokal. Perbedaannya hanya pada proses setup lingkungan dan lokasi penyimpanan data.

## Persiapan satu kali

1. Upload seluruh folder `Synthetic Retail Dataset - 10 Million Transactions` ke Google Drive.
2. Pastikan struktur akhirnya seperti berikut:

```text
MyDrive/
└── Synthetic Retail Dataset - 10 Million Transactions/
    ├── retail_clean_dataset/
    └── retail_contaminated_dataset/
```

3. Publish repository ini ke GitHub.
4. Di masing-masing notebook Colab, ganti:

```python
REPOSITORY_URL = "https://github.com/USERNAME/big-data-retail-analytics.git"
```

dengan URL repository milikmu.

## Urutan menjalankan

1. `01_exploration.ipynb`
2. `02_pyspark_transform.ipynb`
3. `03_etl_validation.ipynb`

Notebook 02 menghasilkan data Parquet dan tabel agregat. Notebook 03 membaca hasil tersebut untuk validasi dan pembuatan grafik, sehingga Notebook 02 harus selesai terlebih dahulu.

Hasil Colab disimpan di:

```text
MyDrive/big-data-retail-analytics-output/
```

Folder tersebut berisi hasil Parquet, tabel validasi, dan grafik. Penyimpanan di Google Drive membuat hasil tetap tersedia walaupun runtime Colab terputus.

## Jika folder dataset berbeda

Ubah nilai berikut pada sel setup notebook:

```python
COLAB_DATASET_ROOT = "/content/drive/MyDrive/nama-folder-dataset"
```

Jangan memasukkan token Kaggle, password database, atau file `.env` ke notebook maupun GitHub.
