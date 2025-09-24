import threading
import pickle
import time
from flask import Flask, jsonify, render_template
import sqlite3
import datetime
import json
import paho.mqtt.client as mqtt
import os

# ----------------------- CONFIG -----------------------
app = Flask(__name__)

BROKER = "192.168.57.2"
PORT = 1883
TOPIC_PZEM1 = "sensor/pzem1"
TOPIC_PZEM2 = "sensor/pzem2"
TOPIC_PREDICT = "predict/pub"
TOPIC_PREDICT_RESULT = "predict/result"

DB_NAME = "pzem.db"
TARIF = 1444.7

# -------------------- THREAD SAFETY --------------------
# Lock untuk melindungi operasi tulis ke DB
db_write_lock = threading.Lock()

# Lock & model variable untuk ML
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
    # Enable WAL mode for better concurrency (reads while writes)
    cur.execute("PRAGMA journal_mode=WAL;")
    # Optional: set synchronous to normal (tuneable)
    cur.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    conn.close()

def save_sensor_data(table: str, data: dict):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Validasi asumsi key ada (caller sudah validasi, ini double-check)
    required = ('tegangan', 'arus', 'daya', 'energi', 'frekuensi')
    if not all(k in data for k in required):
        raise ValueError("Data sensor tidak lengkap")

    with db_write_lock:
        conn = get_db_connection()
        try:
            conn.execute(f"""
                INSERT INTO {table} (tegangan, arus, daya, energi, frekuensi, tanggal) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                data['tegangan'],
                data['arus'],
                data['daya'],
                data['energi'],
                data['frekuensi'],
                now
            ))
            conn.commit()
            print(f"✅ Data disimpan ke {table}: {data}")
        finally:
            conn.close()

# --------------------- MACHINE LEARNING ----------------
# def load_model(path="predict.pkl"):
#     global MODEL
#     with MODEL_LOCK:
#         if MODEL is None:
#             if not os.path.exists(path):
#                 raise FileNotFoundError(f"Model file not found: {path}")
#             print("⏳ Loading ML model from", path)
#             with open(path, "rb") as f:
#                 MODEL = pickle.load(f)
#             print("✅ ML model loaded")
#     return MODEL

# def predict_biaya(data: dict):
#     model = load_model()  # thread-safe lazy load if not preloaded
#     try:
#         features = [[data['daya'], data['energi']]]
#         pred = model.predict(features)[0]
#         return float(pred)  # pastikan tipe serializable
#     except Exception as e:
#         print("❌ Gagal prediksi:", e)
#         return None

# ---------------------- MQTT HANDLER -------------------
def handle_sensor_message(table: str, data: dict):
    try:
        save_sensor_data(table, data)
    except Exception as e:
        print("❌ Gagal simpan data sensor:", e)

def handle_predict_message(data: dict, client: mqtt.Client):
    try:
        pred = predict_biaya(data)
        if pred is not None:
            payload = json.dumps({"biaya": pred})
            client.publish(TOPIC_PREDICT_RESULT, payload)
            print("✅ Prediksi dipublish:", payload)
        else:
            print("⚠️ Prediksi gagal, tidak publish")
    except Exception as e:
        print("❌ Error di handle_predict_message:", e)

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
            
            # Jika caller ingin block di sini, tunggu forever
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

# ------------------------ FLASK ROUTES ------------------------
# @app.route('/')
# def dashboard():
#     return render_template("index.html")

# @app.route('/dashboard')
# def dashboarddd():
#     return render_template("dashboard.html")

# @app.route("/check_data")
# def check_data():
#     conn = get_db_connection()
#     cur = conn.cursor()
#     cur.execute("SELECT tanggal, energi FROM panel1 ORDER BY tanggal DESC LIMIT 10")
#     rows = cur.fetchall()
#     conn.close()
#     return jsonify([dict(r) for r in rows])

# @app.route("/get_hourly")
# def get_hourly():
#     conn = get_db_connection()
#     cur = conn.cursor()

#     # Ambil label jam (pakai panel1 sebagai referensi waktu)
#     cur.execute("""
#         SELECT strftime('%H:%M', tanggal) as jam
#         FROM panel1
#         WHERE date(tanggal) = date('now')
#         GROUP BY jam
#         ORDER BY jam
#     """)
#     labels = [row["jam"] for row in cur.fetchall()]

#     def get_data(unit):
#         data = {}
#         for ruang, table in [("Ruang 1", "panel1"), ("Ruang 2", "panel2")]:
#             cur.execute(f"""
#                 SELECT {unit}
#                 FROM {table}
#                 WHERE date(tanggal) = date('now')
#                 ORDER BY tanggal
#             """)
#             data[ruang] = [row[0] for row in cur.fetchall()]
#         # Panel lain kosong
#         data["Ruang 3"] = []
#         data["Ruang 4"] = []
#         return data

#     response = {
#         "labels": labels,
#         "kWh": get_data("energi"),
#         "V": get_data("tegangan"),
#         "A": get_data("arus"),
#         "Hz": get_data("frekuensi")
#     }

#     conn.close()
#     return jsonify(response)

# @app.route("/get_totals")
# def get_totals():
#     conn = get_db_connection()
#     cur = conn.cursor()

#     def panel_stats_monthly(table):
#         cur.execute(f"""
#             SELECT MIN(energi) as minv, MAX(energi) as maxv
#             FROM {table}
#             WHERE strftime('%Y-%m', tanggal) = strftime('%Y-%m', 'now')
#         """)
#         row = cur.fetchone()
#         return max(0.0, (row["maxv"] or 0) - (row["minv"] or 0))

#     used1 = panel_stats_monthly("panel1")
#     used2 = panel_stats_monthly("panel2")
#     monthly_kwh = used1 + used2
#     monthly_cost = monthly_kwh * TARIF

#     conn.close()
#     return jsonify({
#         "total_energi": round(monthly_kwh, 2),
#         "total_tagihan": round(monthly_cost, 0)
#     })

# @app.route("/get_hourly2")
# def get_hourly2():
#     conn = get_db_connection()
#     cur = conn.cursor()

#     # Ambil semua jam unik dari kedua panel (union, bukan hanya panel1)
#     cur.execute("""
#         SELECT jam FROM (
#             SELECT strftime('%H:%M', tanggal) as jam FROM panel1 WHERE date(tanggal) = date('now')
#             UNION
#             SELECT strftime('%H:%M', tanggal) as jam FROM panel2 WHERE date(tanggal) = date('now')
#         )
#         GROUP BY jam
#         ORDER BY jam
#     """)
#     labels = [row["jam"] for row in cur.fetchall()]

#     def get_data(unit):
#         data = {"Ruang 1": [], "Ruang 2": [], "Ruang 3": [], "Ruang 4": []}

#         # Ambil data panel1 berdasarkan labels
#         cur.execute(f"""
#             SELECT strftime('%H:%M', tanggal) as jam, {unit}
#             FROM panel1
#             WHERE date(tanggal) = date('now')
#         """)
#         panel1_map = {row["jam"]: row[unit] for row in cur.fetchall()}

#         # Ambil data panel2 berdasarkan labels
#         cur.execute(f"""
#             SELECT strftime('%H:%M', tanggal) as jam, {unit}
#             FROM panel2
#             WHERE date(tanggal) = date('now')
#         """)
#         panel2_map = {row["jam"]: row[unit] for row in cur.fetchall()}

#         # Sinkronkan berdasarkan labels
#         for jam in labels:
#             data["Ruang 1"].append(panel1_map.get(jam, 0))  # jika tidak ada isi 0
#             data["Ruang 2"].append(panel2_map.get(jam, 0))
#             data["Ruang 3"].append(0)  # kosong
#             data["Ruang 4"].append(0)  # kosong

#         return data

#     response = {
#         "labels": labels,
#         "kWh": get_data("energi"),
#         "V": get_data("tegangan"),
#         "A": get_data("arus"),
#         "Hz": get_data("frekuensi")
#     }

#     conn.close()
#     return jsonify(response)

# ------------------------ MAIN STARTUP ------------------------
if __name__ == '__main__':
    init_db()
    # try:
    #     load_model("predict.pkl")
    # except Exception as e:
    #     print("⚠️ Warning: Gagal load model pada startup:", e)
    mqtt_client = start_mqtt(loop_forever=False)
    app.run(host="0.0.0.0", port=4000, debug=True, use_reloader=False)
