from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import asyncio
import os

from utils.database import init_db
from utils.storage_db import (
    get_semua_akun, set_status_akun,
    get_semua_grup, get_grup_aktif,
    simpan_banyak_grup, set_status_grup,
    simpan_draft, get_semua_draft, hapus_draft,
    tambah_antrian, get_semua_antrian,
    update_status_antrian, hapus_antrian,
    get_riwayat_hari_ini, get_ringkasan_hari_ini,
    sudah_dikirim_hari_ini
)
from services.account_manager import login_akun, logout_akun, submit_otp, _clients
from services.group_manager import fetch_grup_dari_akun
from services.message_service import kirim_pesan_manual
from core.smart_sender import pilih_akun_tersedia, ringkasan_akun

app = Flask(__name__)
CORS(app)

init_db()

def run(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ── SERVE FRONTEND ────────────────────────────────────────
@app.route("/")
def index():
    frontend = os.path.join(os.path.dirname(__file__), '..', 'frontend')
    return send_from_directory(frontend, 'index.html')

@app.route("/<path:filename>")
def static_files(filename):
    frontend = os.path.join(os.path.dirname(__file__), '..', 'frontend')
    return send_from_directory(frontend, filename)


# ── AKUN ──────────────────────────────────────────────────
@app.route("/api/akun", methods=["GET"])
def api_get_akun():
    data = get_semua_akun()
    for a in data:
        a["online"] = a["phone"] in _clients
    return jsonify(data)

@app.route("/api/akun/login", methods=["POST"])
def api_login():
    phone = request.json.get("phone")
    if not phone:
        return jsonify({"error": "Nomor HP wajib"}), 400
    return jsonify(run(login_akun(phone)))

@app.route("/api/akun/otp", methods=["POST"])
def api_submit_otp():
    b = request.json
    return jsonify(run(submit_otp(b.get("phone"), b.get("kode"), b.get("password"))))

@app.route("/api/akun/logout", methods=["POST"])
def api_logout():
    return jsonify(run(logout_akun(request.json.get("phone"))))

@app.route("/api/akun/status", methods=["POST"])
def api_status_akun():
    b = request.json
    set_status_akun(b.get("phone"), b.get("status"))
    return jsonify({"ok": True})

@app.route("/api/akun/pulihkan", methods=["POST"])
def api_pulihkan_akun():
    set_status_akun(request.json.get("phone"), "active")
    return jsonify({"ok": True})

@app.route("/api/akun/tersedia", methods=["GET"])
def api_akun_tersedia():
    return jsonify(pilih_akun_tersedia(_clients))

@app.route("/api/akun/ringkasan", methods=["GET"])
def api_ringkasan_akun():
    phones = [a["phone"] for a in get_semua_akun()]
    return jsonify(ringkasan_akun(phones))


# ── GRUP ──────────────────────────────────────────────────
@app.route("/api/grup", methods=["GET"])
def api_get_grup():
    return jsonify(get_semua_grup())

@app.route("/api/grup/aktif", methods=["GET"])
def api_grup_aktif():
    return jsonify(get_grup_aktif())

@app.route("/api/grup/fetch", methods=["POST"])
def api_fetch_grup():
    phone = request.json.get("phone")
    if not phone:
        return jsonify({"error": "Pilih akun"}), 400
    hasil = run(fetch_grup_dari_akun(phone))
    simpan_banyak_grup(hasil)
    return jsonify(hasil)

@app.route("/api/grup/status", methods=["POST"])
def api_status_grup():
    b = request.json
    set_status_grup(int(b.get("grup_id")), b.get("status"))
    return jsonify({"ok": True})

@app.route("/api/grup/pulihkan", methods=["POST"])
def api_pulihkan_grup():
    set_status_grup(int(request.json.get("grup_id")), "active")
    return jsonify({"ok": True})


# ── KIRIM ─────────────────────────────────────────────────
@app.route("/api/pesan/kirim", methods=["POST"])
def api_kirim():
    b = request.json
    phone   = b.get("phone")
    grup_id = b.get("grup_id")
    pesan   = b.get("pesan")
    if not all([phone, grup_id, pesan]):
        return jsonify({"error": "phone, grup_id, pesan wajib"}), 400
    return jsonify(run(kirim_pesan_manual(phone, grup_id, pesan)))

@app.route("/api/pesan/log", methods=["GET"])
def api_log():
    return jsonify(get_riwayat_hari_ini())


# ── DRAFT ─────────────────────────────────────────────────
@app.route("/api/draft", methods=["GET"])
def api_get_draft():
    return jsonify(get_semua_draft())

@app.route("/api/draft", methods=["POST"])
def api_post_draft():
    b = request.json
    return jsonify(simpan_draft(b.get("judul"), b.get("isi")))

@app.route("/api/draft/<int:did>", methods=["DELETE"])
def api_del_draft(did):
    hapus_draft(did)
    return jsonify({"ok": True})


# ── ANTRIAN ───────────────────────────────────────────────
@app.route("/api/antrian", methods=["GET"])
def api_get_antrian():
    return jsonify(get_semua_antrian())

@app.route("/api/antrian", methods=["POST"])
def api_post_antrian():
    b = request.json
    return jsonify(tambah_antrian(
        b.get("phone"), int(b.get("grup_id")), b.get("pesan")
    ))

@app.route("/api/antrian/<int:iid>/kirim", methods=["POST"])
def api_kirim_antrian(iid):
    antrian = get_semua_antrian()
    item    = next((a for a in antrian if a["id"] == iid), None)
    if not item:
        return jsonify({"error": "Item tidak ditemukan"}), 404
    hasil  = run(kirim_pesan_manual(item["phone"], item["grup_id"], item["pesan"]))
    status = "terkirim" if hasil["status"] == "berhasil" else "gagal"
    update_status_antrian(iid, status)
    return jsonify(hasil)

@app.route("/api/antrian/<int:iid>", methods=["DELETE"])
def api_del_antrian(iid):
    hapus_antrian(iid)
    return jsonify({"ok": True})


# ── RIWAYAT ───────────────────────────────────────────────
@app.route("/api/riwayat", methods=["GET"])
def api_riwayat():
    return jsonify(get_riwayat_hari_ini())

@app.route("/api/riwayat/ringkasan", methods=["GET"])
def api_ringkasan():
    return jsonify(get_ringkasan_hari_ini())

@app.route("/api/riwayat/cek/<int:gid>", methods=["GET"])
def api_cek_kirim(gid):
    return jsonify({"sudah_dikirim": sudah_dikirim_hari_ini(gid)})


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)
