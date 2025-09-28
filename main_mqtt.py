import threading
import time
from flask import Flask, jsonify, request, render_template
import sqlite3
import datetime
import json
import paho.mqtt.client as mqtt
import os

# ----------------------- CONFIG -----------------------
app = Flask(__name__)

BROKER = "192.168.1.9"
PORT = 1883
TOPIC_PZEM1 = "sensor/pzem1"
TOPIC_PZEM2 = "sensor/pzem2"
TOPIC_PREDICT = "predict/pub"
TOPIC_PREDICT_RESULT = "predict/result"

# daftar semua topik sensor (kecuali predict)
TOPICS = [TOPIC_PZEM1, TOPIC_PZEM2]

DB_NAME = "sensor_data.db"
TARIF = 1444.7

# Menyimpan data terakhir dari setiap panel
latest_data = {topic: None for topic in TOPICS}

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

def accumulate_sensor_data(table: str, data: dict):
    """Akumulasi data sensor sampai di-flush worker"""
    with agg_lock:
        if table not in agg_buffer:
            agg_buffer[table] = {
                "sums": {k: 0.0 for k in ['tegangan', 'arus', 'daya', 'energi', 'frekuensi']},
                "count": 0
            }

        buf = agg_buffer[table]

        # Tambah ke buffer
        for k in ['tegangan', 'arus', 'daya', 'energi', 'frekuensi']:
            try:
                v = float(data.get(k, 0.0))
            except Exception:
                v = 0.0
            buf['sums'][k] += v
        buf['count'] += 1

        print(f"📊 Akumulasi sementara {table}: samples={buf['count']} sums={buf['sums']}")

def flush_buffer(table: str):
    with agg_lock:
        if table not in agg_buffer:
            return

        buf = agg_buffer[table]
        count = buf.get('count', 0)

        if count == 0:
            print(f"♻️ Buffer {table} kosong, skip flush.")
            return

        sums = buf['sums']

        payload = {
            "tegangan": round(sums['tegangan'] / count, 3),
            "arus": round(sums['arus'], 3),
            "daya": round(sums['daya'] / count, 3),
            "energi": round(sums['energi'], 3),
            "frekuensi": round(sums['frekuensi'] / count, 3),
            "tanggal": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # reset buffer
        agg_buffer[table] = {
            "sums": {k: 0.0 for k in ['tegangan', 'arus', 'daya', 'energi', 'frekuensi']},
            "count": 0
        }

    # 🔑 simpan ke DB di luar agg_lock
    try:
        save_sensor_data(table, payload)
        print(f"💾 Buffer {table} diflush ke DB (count={count}).")
    except Exception as e:
        print("❌ Gagal simpan hasil flush:", e)

# ---------------- BACKGROUND FLUSH THREAD ----------------
def flush_worker(interval: int = 60):
    while True:
        time.sleep(interval)
        for table in list(agg_buffer.keys()):
            flush_buffer(table)

# ---------------------- MQTT HANDLER -------------------
def handle_sensor_message(table: str, data: dict):
    try:
        accumulate_sensor_data(table, data)
    except Exception as e:
        print("❌ Gagal akumulasi data sensor:", e)

def handle_predict_message(data: dict, client: mqtt.Client):
    # bisa diisi logic prediksi
    pass

def handle_message(topic: str, data: dict, client: mqtt.Client):
    # update latest_data untuk realtime endpoint
    latest_data[topic] = data

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
        subs = [(t, 0) for t in TOPICS] + [(TOPIC_PREDICT, 0)]
        client.subscribe(subs)
    else:
        print("❌ MQTT connect failed with rc:", rc)

def on_message(client, userdata, msg):
    print(f"✅ Data di on_message: {msg.topic}, {msg.payload}")
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)
        latest_data[msg.topic] = payload 
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
            client.loop_start()
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

# ------------------------ BACKEND DASHBOARD PUSAT ------------------------
# ======================== REALTIME DATA ========================
@app.route("/index")
def index_page():
    """Render halaman dashboard"""
    return render_template("realtime_fetch.html")

@app.route("/index/realtime", methods=["GET"])
def get_realtime():
    field = request.args.get("field")

    data = {}
    for idx, topic in enumerate(TOPICS, start=1):
        panel_key = f"panel{idx}"
        value = latest_data.get(topic)

        if value:
            if field and field in value:
                data[panel_key] = {field: value.get(field)}
            else:
                data[panel_key] = value
        else:
            data[panel_key] = None

    # print("latest_data sekarang:", latest_data)
    return jsonify(data)

# ======================== LINE CHART ========================

# ------------------------ QUERY HELPER ------------------------
def query_db(query, args=(), one=False):
    conn = sqlite3.connect(DB_NAME)  # ✅ pakai DB_NAME, bukan DB_PATH
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, args)
    rows = cur.fetchall()
    conn.close()
    return (rows[0] if rows else None) if one else rows


# ------------------------ ENERGY USAGE API ------------------------
TABLES = {
    "Gedung Pusat": "panel1",
    "Gedung Logam & Mesin": "panel2",
    # kalau ada tabel lain tambahkan di sini
    # "Gedung Elektronika": "panel3",
    # "Gedung Otomotif": "panel4",
    # dst...
}

@app.route("/index/energy-usage")
def energy_usage():
    datasets = []
    labels = []

    for building, table in TABLES.items():
        rows = query_db(f"""
            SELECT strftime('%H:%M', tanggal) as jam, energi
            FROM {table} 
            ORDER BY tanggal ASC
            LIMIT 60
        """)

        usage = []
        building_labels = []
        for row in rows:
            building_labels.append(row["jam"])
            usage.append(row["energi"])

        datasets.append({
            "building": building,
            "usage": usage
        })

        # gunakan label dari tabel pertama sebagai acuan
        if not labels and building_labels:
            labels = building_labels

    return jsonify({
        "labels": labels,
        "datasets": datasets
    })

# ======================== PIE CHART ========================
def query_db(query, args=(), one=False):
    conn = sqlite3.connect(DB_NAME)  # ✅ pakai DB_NAME, bukan DB_PATH
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, args)
    rows = cur.fetchall()
    conn.close()
    return (rows[0] if rows else None) if one else rows
# ------------------------ ENERGY PIE API ------------------------
GEDUNG = {
    "Gedung Pusat": "panel1",
    "Gedung Logam & Mesin": "panel2",
    # "Gedung Elektronika": "panel3",
    # "Gedung Otomotif": "panel4",
    # "Gedung TI": "panel5",
    # "Gedung Manajemen": "panel6",
    # "Gedung Sipil": "panel7",
}


@app.route("/index/energy-pie")
def energy_pie():
    labels = []
    values = []

    for building, table in GEDUNG.items():
        row = query_db(f"""
            SELECT SUM(energi) as total
            FROM {table}
        """, one=True)

        total = row["total"] if row and row["total"] is not None else 0
        labels.append(building)
        values.append(total)

    return jsonify({
        "labels": labels,
        "values": values
    })

# ------------------------ MAIN STARTUP ------------------------
if __name__ == '__main__':
    init_db()
    mqtt_client = start_mqtt(loop_forever=False)

    threading.Thread(target=flush_worker, args=(60,), daemon=True).start()

    app.run(host="0.0.0.0", port=4000, debug=True, use_reloader=False)