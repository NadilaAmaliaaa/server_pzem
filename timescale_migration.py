import psycopg2
from psycopg2 import sql

DB_CONFIG = {
    "dbname": "sensor_data",     # pastikan DB ini sudah ada
    "user": "postgres",
    "password": "nadila",
    "host": "localhost",
    "port": 5432
}


def migrate():
    print("\n🚀 Menjalankan migrasi database TimescaleDB...")

    # koneksi awal (autocommit FALSE)
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # 1️⃣ Aktifkan ekstensi TimescaleDB
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

    # 2️⃣ Buat tabel dasar
    cur.execute("""
        CREATE TABLE IF NOT EXISTS buildings (
            id SERIAL PRIMARY KEY,
            code VARCHAR(50) NOT NULL UNIQUE,
            name VARCHAR(100) NOT NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sensors (
            id SERIAL PRIMARY KEY,
            building_id INT NOT NULL REFERENCES buildings(id) ON DELETE CASCADE,
            name VARCHAR(50) NOT NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_readings (
            sensor_id INT NOT NULL REFERENCES sensors(id) ON DELETE CASCADE,
            timestamp TIMESTAMPTZ NOT NULL,
            voltage DOUBLE PRECISION,
            current DOUBLE PRECISION,
            power DOUBLE PRECISION,
            energy DOUBLE PRECISION,
            frequency DOUBLE PRECISION,
            power_factor DOUBLE PRECISION,
            cost DOUBLE PRECISION,
            PRIMARY KEY (sensor_id, timestamp)
        );
    """)

    # 3️⃣ Jadikan hypertable
    cur.execute("""
        SELECT create_hypertable('sensor_readings', 'timestamp', if_not_exists => TRUE);
    """)

    # 4️⃣ Index tambahan
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sensor_time
        ON sensor_readings (sensor_id, timestamp DESC);
    """)

    conn.commit()
    cur.close()
    conn.close()

    # 5️⃣ Buat MATERIALIZED VIEW (autocommit mode)
    print("📈 Membuat continuous aggregate view (daily_energy)...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS daily_energy
        WITH (timescaledb.continuous) AS
        SELECT 
            time_bucket('1 day', timestamp) AS date,
            sensor_id,
            SUM(energy) AS total_energy_kWh,
            AVG(power) AS avg_power,
            MAX(power) AS peak_power,
            SUM(cost) AS total_cost
        FROM sensor_readings
        GROUP BY date, sensor_id
        WITH DATA;
    """)
    cur.close()
    conn.close()

    # 6️⃣ Tambahkan policy auto-refresh
    print("🕒 Menambahkan continuous aggregate policy (debug mode 5 menit)...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM timescaledb_information.jobs j
                WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
                AND j.hypertable_name = 'daily_energy'
            ) THEN
                PERFORM add_continuous_aggregate_policy(
                    'daily_energy',
                    start_offset => INTERVAL '7 days',
                    end_offset   => INTERVAL '0 minutes',
                    schedule_interval => INTERVAL '5 minutes'
                );
            END IF;
        END
        $$;
    """)
    conn.commit()
    cur.close()
    conn.close()


def seed():
    print("\n🌱 Menjalankan seeder...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM buildings;")
    if cur.fetchone()[0] > 0:
        print("ℹ️ Seeder dilewati: data sudah ada.")
        conn.close()
        return

    building_data = [
        ("Departement Pusat", "department1"),
        ("Departement Mesin", "department2"),
        ("Departement Elektronika", "department3"),
        ("Departement Otomotif", "department4"),
        ("Departement TI", "department5"),
        ("Departement Manajemen", "department6"),
        ("Departement Sipil", "department7")
    ]

    for name, code in building_data:
        cur.execute(
            "INSERT INTO buildings (name, code) VALUES (%s, %s) RETURNING id;",
            (name, code)
        )
        building_id = cur.fetchone()[0]
        cur.executemany(
            "INSERT INTO sensors (building_id, name) VALUES (%s, %s);",
            [(building_id, f"PZEM{i}") for i in range(1, 4)]
        )

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Seeder selesai: gedung & sensor berhasil dibuat.")

if __name__ == "__main__":
    migrate()
    seed()