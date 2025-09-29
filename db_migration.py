import sqlite3

DB_PATH = "sensor_data.db"

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Tabel panel1
    cur.execute("""
        CREATE TABLE IF NOT EXISTS panel1 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tegangan REAL,
            arus REAL,
            daya REAL,
            energi REAL,
            frekuensi REAL,
            biaya REAL,
            tanggal TEXT
        )
    """)

    # Tabel panel2
    cur.execute("""
        CREATE TABLE IF NOT EXISTS panel2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tegangan REAL,
            arus REAL,
            daya REAL,
            energi REAL,
            frekuensi REAL,
            biaya REAL,
            tanggal TEXT
        )
    """)

    conn.commit()
    conn.close()
    print("✅ Migration selesai: tabel panel1 & panel2 siap digunakan.")

if __name__ == "__main__":
    migrate()
