
# RocksDB Spring Boot Service

Layanan *dummy* berbasis Java Spring Boot untuk membaca file secara otomatis dan menyimpannya ke dalam RocksDB.

## Database (Column Families)

Data dipisah agar operasi lebih efisien dan tidak saling mengganggu:

* **`meta-data`**: Menyimpan log status file (`PROCESSING`, `COMPLETED`, `ARCHIVED`), tanggal, dan UUID.


* **`csv-data`**: Menyimpan isi baris CSV yang di-*ingest*.


* **`default`**: Bawaan RocksDB, tidak dipakai.



## Mode Penyimpanan

Diatur melalui konfigurasi:

* **Single Mode** (`app.rocksdb.use-batch=false`): Tulis data satu per satu. Lebih lambat untuk file besar.


* **Batch Mode** (`app.rocksdb.use-batch=true`): Kumpulkan data di *buffer* (default 1000 baris) lalu kirim sekaligus (*WriteBatch*). Sangat efisien untuk file besar.



## Komponen Sistem

Aplikasi berjalan dengan 5 komponen utama:

1. **FileScheduler**: Menjalankan *trigger scan* file tiap 30 detik & *cleanup* data tiap menit.


2. **FileScannerService**: Mendeteksi file baru di direktori.


3. **FileProcessingService**: Melakukan *ingest* data dan mengecek status metadata.


4. **FileManagerService**: Mengarsipkan file yang sudah selesai diproses.


5. **RocksDBService**: *Bridge* utama untuk operasi CRUD ke RocksDB.
