from flask import Flask, jsonify, render_template, redirect, url_for, session, flash, request
import sqlite3
import datetime

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect(r"E:\Magang Wajib\database\rst")
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def dashboard():
    return render_template("index.html")

@app.route('/dashboard')
def dashboarddd():
    return render_template("dashboard.html")

# Harian (per jam)
@app.route("/get_hourly")
def get_hourly():
    conn = get_db_connection()
    cur = conn.cursor()

    # Ambil label jam (pakai panel1 sebagai referensi waktu)
    cur.execute("""
        SELECT strftime('%H:%M', tanggal) as jam
        FROM panel1
        WHERE date(tanggal) = date('now')
        GROUP BY jam
        ORDER BY jam
    """)
    labels = [row["jam"] for row in cur.fetchall()]

    # Helper ambil data per tabel/ruang
    def get_data(unit):
        data = {}
        for ruang, table in [("Ruang 1", "panel1"), ("Ruang 2", "panel2")]:
            cur.execute(f"""
                SELECT {unit}
                FROM {table}
                WHERE date(tanggal) = date('now')
                ORDER BY tanggal
            """)
            data[ruang] = [row[0] for row in cur.fetchall()]

        # Kalau cuma ada 2 panel, sisanya dikosongin
        data["Ruang 3"] = []
        data["Ruang 4"] = []
        return data

    response = {
        "labels": labels,
        "kWh": get_data("energi"),
        "V": get_data("tegangan"),
        "A": get_data("arus"),
        "Hz": get_data("frekuensi")
    }

    conn.close()
    return jsonify(response)



@app.route("/get_totals")
def get_totals():
    conn = get_db_connection()
    cur = conn.cursor()

    def panel_stats_monthly(table):
        cur.execute(f"""
            SELECT 
                MIN(energi) as minv,
                MAX(energi) as maxv
            FROM {table}
            WHERE strftime('%Y-%m', tanggal) = strftime('%Y-%m', 'now')
        """)
        row = cur.fetchone()
        return max(0.0, (row["maxv"] or 0) - (row["minv"] or 0))

    used1 = panel_stats_monthly("panel1")
    used2 = panel_stats_monthly("panel2")
    monthly_kwh = used1 + used2

    tarif = 1444.7
    monthly_cost = monthly_kwh * tarif

    conn.close()
    return jsonify({
        "total_energi": round(monthly_kwh, 2),
        "total_tagihan": round(monthly_cost, 0)
    })



#kirim data sensor PZEM 1
@app.route('/pzem1', methods=['POST'])
def pzem1():
    try:
        data = request.get_json()
        tegangan = data.get('tegangan')
        arus = data.get('arus')
        daya = data.get('daya')
        energi = data.get('energi')
        frekuensi = data.get('frekuensi')

        if None in (tegangan, arus, daya, energi, frekuensi):
            return jsonify({"status": "error", "message": "Data tidak lengkap"}), 400

        # Ambil waktu sekarang
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        conn = get_db_connection()
        conn.execute("""
            INSERT INTO panel1 (tegangan, arus, daya, energi, frekuensi, tanggal) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, (tegangan, arus, daya, energi, frekuensi, now))
        conn.commit()
        conn.close()

        return jsonify({"status": "success", "message": "Data berhasil disimpan"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

#kirim data sensor PZEM 2
@app.route('/pzem2', methods=['POST'])
def pzem2():
    try:
        data = request.get_json()
        tegangan = data.get('tegangan')
        arus = data.get('arus')
        daya = data.get('daya')
        energi = data.get('energi')
        frekuensi = data.get('frekuensi')

        if None in (tegangan, arus, daya, energi, frekuensi):
            return jsonify({"status": "error", "message": "Data tidak lengkap"}), 400

        # Ambil waktu sekarang
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        conn = get_db_connection()
        conn.execute("""
            INSERT INTO panel2 (tegangan, arus, daya, energi, frekuensi, tanggal) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, (tegangan, arus, daya, energi, frekuensi, now))
        conn.commit()
        conn.close()

        return jsonify({"status": "success", "message": "Data berhasil disimpan"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/get_pie")
def get_pie():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT COALESCE(SUM(energi), 0) FROM panel1
        WHERE strftime('%Y-%m', tanggal) = strftime('%Y-%m', 'now')
    """)
    ruang1 = cur.fetchone()[0]

    cur.execute("""
        SELECT COALESCE(SUM(energi), 0) FROM panel2
        WHERE strftime('%Y-%m', tanggal) = strftime('%Y-%m', 'now')
    """)
    ruang2 = cur.fetchone()[0]

    # Kalau belum ada panel3 & panel4 → dummy kosong
    ruang3, ruang4 = 0, 0

    conn.close()
    return jsonify({
        "labels": ["Ruang 1", "Ruang 2", "Ruang 3", "Ruang 4"],
        "values": [ruang1, ruang2, ruang3, ruang4]
    })

@app.route("/get_daily")
def get_daily():
    conn = get_db_connection()
    cur = conn.cursor()

    def panel_stats_daily(table):
        cur.execute(f"""
            SELECT 
                MIN(energi) as minv,
                MAX(energi) as maxv
            FROM {table}
            WHERE strftime('%Y-%m-%d', tanggal) = strftime('%Y-%m-%d', 'now')
        """)
        row = cur.fetchone()
        if row and row["maxv"] is not None and row["minv"] is not None:
            return max(0.0, row["maxv"] - row["minv"])
        return 0.0

    used1 = panel_stats_daily("panel1")
    used2 = panel_stats_daily("panel2")
    daily_kwh = used1 + used2

    tarif = 1444.7
    daily_cost = daily_kwh * tarif

    conn.close()
    return jsonify({
        "total_energi": round(daily_kwh, 2),
        "total_tagihan": round(daily_cost, 0)
    })



@app.route("/check_data")
def check_data():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT tanggal, energi FROM panel1 ORDER BY tanggal DESC LIMIT 10")
    rows = cur.fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])



if __name__ == '__main__':
    app.run(host="0.0.0.0", port=4000, debug=True)
