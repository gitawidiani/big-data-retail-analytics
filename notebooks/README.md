# Notebook proyek

Folder `local/` berisi notebook yang dijalankan melalui Jupyter di laptop Windows. Folder `colab/` berisi analisis yang sama dengan setup khusus Google Colab.

Urutan menjalankan notebook:

1. `local/01_exploration.ipynb`
2. `local/02_pyspark_transform.ipynb`
3. `local/03_etl_validation.ipynb`

Jalankan Jupyter dari root repository agar lingkungan Python dan file `.env` mudah ditemukan:

```powershell
.\.venv\Scripts\Activate.ps1
jupyter lab
```

Notebook lokal memakai Spark `local[2]`. Artinya, pekerjaan dibagi ke dua thread pemrosesan pada laptop ini, bukan dikirim ke laptop lain.

Untuk Colab, ikuti petunjuk di `colab/README.md`. Kode analisis bersama tetap diletakkan di `src/` agar perhitungan versi lokal dan Colab tidak berbeda.
