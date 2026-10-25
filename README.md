# Retail Sales and Inventory Risk Analytics

Proyek praktikum Big Data Analytics menggunakan Pandas, PySpark, Parquet, PostgreSQL, dan Matplotlib. Analisis menggunakan dataset retail sintetis berskala hampir 10 juta baris transaksi dengan tabel toko, produk, pelanggan, promosi, dan inventori.

## Tujuan

1. Memahami struktur dan kualitas data retail relasional.
2. Melakukan transformasi dan agregasi berskala besar dengan PySpark.
3. Menyimpan hasil detail ke Parquet dan data mart ke PostgreSQL.
4. Menghasilkan grafik, insight berbasis angka, dan rekomendasi bisnis.
5. Menguji performa pada 10 ribu, 100 ribu, 1 juta, dan data penuh.

## Sumber data

- Kaggle: [Synthetic Retail Dataset - 10 Million Transactions](https://www.kaggle.com/datasets/mrayyanshehzad/synthetic-retail-dataset-10-million-transactions)
- Lisensi menurut dokumentasi dataset: Apache License 2.0.
- Dataset tidak disimpan dalam repository karena setiap CSV transaksi berukuran sekitar 764 MB.

Letakkan folder dataset sebagai saudara folder repository:

```text
Project UTS/
├── Synthetic Retail Dataset - 10 Million Transactions/
│   ├── retail_clean_dataset/
│   └── retail_contaminated_dataset/
└── big-data-retail-analytics/
```

## Struktur proyek

```text
big-data-retail-analytics/
├── data/
│   ├── README.md
│   └── sample/
├── docs/
│   └── data_audit.md
├── notebooks/
│   ├── README.md
│   ├── local/
│   │   ├── 01_exploration.ipynb
│   │   ├── 02_pyspark_transform.ipynb
│   │   └── 03_etl_validation.ipynb
│   └── colab/
│       ├── README.md
│       ├── 01_exploration.ipynb
│       ├── 02_pyspark_transform.ipynb
│       └── 03_etl_validation.ipynb
├── output/
│   ├── charts/
│   ├── parquet/
│   └── validation/
├── reports/
│   └── laporan_akhir.md
├── scripts/
│   ├── benchmark.py
│   └── create_sample.py
├── sql/
│   ├── analysis_queries.sql
│   └── schema.sql
├── src/
│   ├── config.py
│   └── spark_pipeline.py
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```

## Pertanyaan analisis

1. Bagaimana tren nilai penjualan bulanan selama 2022-2025?
2. Toko, kota, kategori, dan kanal mana yang memberikan kontribusi terbesar?
3. Bagaimana pola diskon dan promosi pada transaksi?
4. SKU mana yang berisiko stockout?
5. SKU mana yang merupakan slow mover dan berpotensi mengikat modal kerja?

## Persiapan lokal

Gunakan Python 3.11 dan Java 17 untuk kompatibilitas dengan PySpark 3.5.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
jupyter lab
```

Sesuaikan `DATASET_ROOT` di `.env` jika folder dataset berada di lokasi lain.

## Persiapan Google Colab

1. Upload folder `Synthetic Retail Dataset - 10 Million Transactions` ke `MyDrive`.
2. Buka notebook dari folder `notebooks/colab/` melalui GitHub.
3. Ganti `REPOSITORY_URL` pada sel setup dengan URL repository GitHub milikmu.
4. Jika lokasi dataset di Drive berbeda, ganti nilai `COLAB_DATASET_ROOT`.
5. Jalankan notebook secara berurutan dari nomor 01 sampai 03.

Notebook Colab memasang dependensi, melakukan mount Google Drive, dan menyimpan hasil secara permanen ke `MyDrive/big-data-retail-analytics-output`. Panduan lengkap tersedia di [`notebooks/colab/README.md`](notebooks/colab/README.md).

## Urutan pengerjaan

### Jupyter lokal

1. Jalankan `notebooks/local/01_exploration.ipynb` untuk profiling dengan Pandas.
2. Jalankan `notebooks/local/02_pyspark_transform.ipynb` untuk transformasi, quality checks, agregasi, dan penulisan Parquet.
3. Siapkan PostgreSQL dengan `sql/schema.sql`.
4. Jalankan `notebooks/local/03_etl_validation.ipynb` untuk validasi read-back, data mart PostgreSQL, grafik, dan benchmark.
5. Lengkapi `reports/laporan_akhir.md` dengan angka hasil eksekusi.

### Google Colab

Urutannya sama, tetapi gunakan ketiga file dalam `notebooks/colab/`. Notebook 02 harus selesai sebelum Notebook 03 karena Notebook 03 membaca hasil ETL yang dibuat oleh Notebook 02.

Panduan langkah demi langkah untuk pemula dan cara menerbitkan proyek melalui GitHub Desktop tersedia di [`docs/panduan_pemula.md`](docs/panduan_pemula.md).

## Catatan penting

- Data `contaminated` berisi anomali bisnis yang disengaja, bukan sekadar nilai kosong atau format rusak.
- `promo_id` boleh kosong jika transaksi tidak memakai promosi.
- Jangan menuliskan simbol mata uang tertentu sebelum mata uang dataset dikonfirmasi oleh sumber.
- Jangan mengunggah `.env`, CSV transaksi penuh, atau hasil Parquet ke GitHub.
- Angka hasil analisis di laporan harus berasal dari notebook yang benar-benar dijalankan.

