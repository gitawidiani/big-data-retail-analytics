# Panduan Pengerjaan untuk Pemula

## Gambaran paling sederhana

Proyek ini mengubah CSV mentah menjadi data yang siap dianalisis:

```text
CSV Kaggle
   ↓ Extract: dibaca oleh Pandas atau Spark
DataFrame
   ↓ Transform: diperiksa, digabung, dan ditambah kolom analisis
Data bersih dan ringkasan
   ↓ Load: disimpan ke Parquet dan, bila diperlukan, PostgreSQL
Grafik, insight, serta laporan
```

PySpark dipakai terutama karena tabel transaksi hampir 10 juta baris. Saat memakai `local[*]`, Spark membagi pekerjaan ke beberapa inti prosesor **pada laptop ini**. Proyek ini tidak otomatis memakai laptop lain. Agar beberapa komputer bekerja sebagai satu cluster, semuanya perlu dipasang dan dikonfigurasi sebagai cluster Spark; hal itu tidak diperlukan untuk tugas ini.

## Langkah 1 — Siapkan aplikasi

Pasang:

1. Python 3.11 (64-bit).
2. Java 17 (64-bit), lalu pastikan `JAVA_HOME` mengarah ke instalasinya.
3. Visual Studio Code beserta ekstensi Python dan Jupyter, atau JupyterLab.
4. GitHub Desktop jika ingin cara visual untuk commit dan upload. Aplikasi ini membantu, tetapi bukan kewajiban.
5. PostgreSQL hanya jika dosen benar-benar meminta hasil dimuat ke database.

## Langkah 2 — Buat lingkungan Python

Buka PowerShell pada folder proyek, lalu jalankan:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

Jika lokasi dataset tetap seperti sekarang, `.env` tidak perlu diubah. File `.env` sengaja diabaikan Git agar kata sandi database tidak terunggah.

## Langkah 3 — Mulai dari sampel

Jalankan:

```powershell
python scripts/create_sample.py --rows 10000
jupyter lab
```

Buka dan jalankan `notebooks/local/01_exploration.ipynb`. Tahap ini memakai Pandas pada sampel sehingga cepat dan mudah diperiksa. Pastikan semua quality check bernilai `True`.

## Langkah 4 — Jalankan ETL penuh

Buka `notebooks/local/02_pyspark_transform.ipynb`, lalu jalankan sel satu demi satu. Notebook tersebut melakukan:

1. **Extract:** membaca transaksi dan tabel master dari CSV.
2. **Validate:** memeriksa nilai kosong, nilai tidak masuk akal, rumus total, dan foreign key.
3. **Transform:** menggabungkan transaksi dengan toko, SKU, pelanggan, dan promosi.
4. **Aggregate:** membuat ringkasan bulanan, toko, kategori, kanal, dan promosi.
5. **Load:** menyimpan detail ke Parquet dan ringkasan ke CSV.
6. **Read-back:** membaca Parquet kembali untuk memastikan jumlah baris tidak berubah.

Output Parquet tidak akan masuk Git karena ukurannya besar.

## Langkah 5 — Validasi dan grafik

Jalankan `notebooks/local/03_etl_validation.ipynb`. Grafik akan muncul di `output/charts/`. Gunakan grafik dan angka tersebut untuk melengkapi `reports/laporan_akhir.md`.

Untuk uji performa:

```powershell
python scripts/benchmark.py
python scripts/benchmark.py --include-full
```

Perintah pertama menguji 10 ribu, 100 ribu, dan 1 juta baris. Perintah kedua menambahkan data penuh. Jalankan yang kedua ketika laptop tidak sedang mengerjakan pekerjaan berat lain.

## Langkah 6 — PostgreSQL opsional

Jika PostgreSQL diwajibkan:

1. Buat database kosong.
2. Jalankan `sql/schema.sql` pada database tersebut.
3. Isi pengaturan `POSTGRES_*` di `.env`.
4. Ubah `ENABLE_POSTGRES_LOAD=1`.
5. Jalankan bagian PostgreSQL pada notebook 03.

Tabel agregasi sudah cukup untuk membuktikan proses load. Memuat hampir 10 juta baris detail ke PostgreSQL hanya dilakukan jika memang diminta.

## Langkah 7 — Masukkan ke GitHub Desktop

Git sudah diinisialisasi pada folder proyek. Di GitHub Desktop:

1. Pilih **File → Add local repository**.
2. Pilih folder `big-data-retail-analytics`.
3. Periksa daftar perubahan. Pastikan tidak ada `sales_transactions.csv` ukuran ratusan MB dan tidak ada `.env`.
4. Isi ringkasan commit, misalnya `Initial retail analytics project`.
5. Klik **Commit to main**.
6. Klik **Publish repository**.
7. Tentukan nama repository dan pilih visibilitas **Private** bila tugas belum boleh dilihat umum.

GitHub Desktop bukan syarat untuk GitHub. Ia hanya antarmuka visual untuk perintah Git. Dataset penuh tidak perlu dan tidak boleh diunggah; README sudah memberi tautan sumber Kaggle agar orang lain dapat mengunduhnya sendiri.

## Bukti yang sebaiknya masuk laporan

- Jumlah baris dan kolom data.
- Hasil quality checks sebelum transformasi.
- Diagram alur ETL.
- Potongan skema hasil join.
- Waktu proses setiap ukuran benchmark.
- Tiga sampai lima grafik utama.
- Insight yang menyebut angka dari hasil eksekusi.
- Rekomendasi bisnis yang berhubungan langsung dengan insight.
