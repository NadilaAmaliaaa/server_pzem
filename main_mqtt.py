import threading
import time
from flask import Flask, jsonify
import sqlite3
import datetime
import json
import paho.mqtt.client as mqtt
import os

# ----------------------- CONFIG -----------------------
app = Flask(__name__)

BROKER = "192.168.57.66"
PORT = 1883
TOPIC_PZEM1 = "sensor/pzem1"
TOPIC_PZEM2 = "sensor/pzem2"
TOPIC_PREDICT = "predict/pub"
TOPIC_PREDICT_RESULT = "predict/result"

DB_NAME = "sensor_data.db"
TARIF = 1444.7

# -------------------- THREAD SAFETY --------------------
db_write_lock = threading.Lock()
MODEL = None
MODEL_LOCK = threading.Lock()

# --------------------- DATABASE ------------------------
def get_db_connection():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    conn.close()

def save_sensor_data(table: str, data: dict):
    """Simpan data akumulasi ke DB. data wajib berisi: tegangan, arus, daya, energi, frekuensi, tanggal"""
    required = ('tegangan', 'arus', 'daya', 'energi', 'frekuensi', 'tanggal')
    if not all(k in data for k in required):
        raise ValueError("Data sensor tidak lengkap saat save_sensor_data")

    with db_write_lock:
        conn = get_db_connection()
        try:
            conn.execute(f"""
                INSERT INTO {table} (tegangan, arus, daya, energi, frekuensi, tanggal) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                round(data['tegangan'], 3),
                round(data['arus'], 3),
                round(data['daya'], 3),
                round(data['energi'], 3),
                round(data['frekuensi'], 3),
                data['tanggal']
            ))
            conn.commit()
            print(f"✅ Data disimpan ke {table}: {data}")
        finally:
            conn.close()

# ------------------- AGGREGATION BUFFER -------------------
agg_buffer = {}
agg_lock = threading.Lock()

def _now_minute(ts=None):
    if ts is None:
        ts = datetime.datetime.now()
    return ts.replace(second=0, microsecond=0)

def accumulate_sensor_data(table: str, data: dict):
    """Akumulasi data sensor berdasarkan menit. Menyimpan sum untuk arus/daya/energi
    dan menghitung rata-rata untuk tegangan/frekuensi saat flush."""
    now_min = _now_minute()

    with agg_lock:
        # kalau buffer belum ada → buat baru
        if table not in agg_buffer:
            agg_buffer[table] = {
                "minute": now_min,
                "sums": {k: 0.0 for k in ['tegangan', 'arus', 'daya', 'energi', 'frekuensi']},
                "count": 0
            }

        buf = agg_buffer[table]

        # kalau menit berganti, flush dulu buffer lama
        if buf["minute"] < now_min:
            flush_buffer(table)
            # setelah flush, buat buffer baru untuk menit sekarang
            agg_buffer[table] = {
                "minute": now_min,
                "sums": {k: 0.0 for k in ['tegangan', 'arus', 'daya', 'energi', 'frekuensi']},
                "count": 0
            }
            buf = agg_buffer[table]

        # tambahkan data ke buffer
        for k in ['tegangan', 'arus', 'daya', 'energi', 'frekuensi']:
            try:
                v = float(data.get(k, 0.0))
            except Exception:
                v = 0.0
            buf['sums'][k] += v
        buf['count'] += 1

        print(f"📊 Akumulasi sementara {table} @ {buf['minute']}: samples={buf['count']} sums={buf['sums']}")

def flush_buffer(table: str):
    """Simpan buffer lama ke DB. Menghitung rata-rata untuk tegangan & frekuensi,
    menjumlah (sum) untuk arus/daya/energi."""
    with agg_lock:
        if table not in agg_buffer:
            return

        buf = agg_buffer[table]
        count = buf.get('count', 0)
        minute_ts = buf.get('minute', _now_minute())

        if count == 0:
            print(f"♻️ Buffer {table} pada {minute_ts} kosong, skip flush.")
            # biarkan buffer tetap ada, tunggu data baru
            return

        sums = buf['sums']

        avg_tegangan = sums['tegangan']
        avg_frekuensi = sums['frekuensi']
        sum_arus = sums['arus']
        sum_daya = sums['daya']
        sum_energi = sums['energi']

        payload = {
            "tegangan": avg_tegangan,
            "arus": sum_arus,
            "daya": sum_daya,
            "energi": sum_energi,
            "frekuensi": avg_frekuensi,
            "tanggal": minute_ts.strftime("%Y-%m-%d %H:%M:%S")
        }

        try:
            save_sensor_data(table, payload)
            print(f"💾 Buffer {table} untuk menit {minute_ts} diflush ke DB (count={count}).")
        except Exception as e:
            print("❌ Gagal simpan hasil flush:", e)

        # hapus buffer lama, buffer baru dibuat lagi saat ada data baru masuk
        del agg_buffer[table]

# ---------------- BACKGROUND FLUSH THREAD ----------------
def flush_worker(interval: int = 60):
    """Thread untuk flush buffer setiap interval detik."""
    while True:
        time.sleep(interval)
        # flush any existing buffers (use list to avoid runtime-dict-changes)
        for table in list(agg_buffer.keys()):
            try:
                flush_buffer(table)
            except Exception as e:
                print("❌ Error di flush_worker:", e)

# ---------------------- MQTT HANDLER -------------------
def handle_sensor_message(table: str, data: dict):
    try:
        accumulate_sensor_data(table, data)
    except Exception as e:
        print("❌ Gagal akumulasi data sensor:", e)

def handle_predict_message(data: dict, client: mqtt.Client):
    # placeholder jika mau pakai prediksi
    pass

def handle_message(topic: str, data: dict, client: mqtt.Client):
    if topic == TOPIC_PZEM1:
        handle_sensor_message("panel1", data)
    elif topic == TOPIC_PZEM2:
        handle_sensor_message("panel2", data)
    elif topic == TOPIC_PREDICT:
        handle_predict_message(data, client)
    else:
        print("⚠️ Topik tidak dikenali:", topic)

# ---------------------- MQTT CALLBACK ------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker (rc=0)")
        client.subscribe([(TOPIC_PZEM1, 0), (TOPIC_PZEM2, 0), (TOPIC_PREDICT, 0)])
    else:
        print("❌ MQTT connect failed with rc:", rc)

def on_message(client, userdata, msg):
    print(f"✅ Data di on_message: {msg.topic}, {msg.payload}")
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)
        handle_message(msg.topic, data, client)
    except Exception as e:
        print("❌ Error di on_message:", e)

def start_mqtt(loop_forever=False):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    backoff = 1
    while True:
        try:
            client.connect(BROKER, PORT, 60)
            client.loop_start()  # non-blocking
            print("🔁 MQTT loop started")
            if loop_forever:
                while True:
                    time.sleep(1)
            else:
                return client
        except Exception as e:
            print("❌ Gagal connect ke MQTT broker:", e)
            print(f"🔁 Retry connect after {backoff}s")
            time.sleep(backoff)
            backoff = min(60, backoff * 2)

# ------------------------ MAIN STARTUP ------------------------
if __name__ == '__main__':
    init_db()
    mqtt_client = start_mqtt(loop_forever=False)

    # Jalankan thread flush background (tiap 60 detik)
    threading.Thread(target=flush_worker, args=(60,), daemon=True).start()

    app.run(host="0.0.0.0", port=4000, debug=True, use_reloader=False)