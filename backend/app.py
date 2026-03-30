from flask import Flask, jsonify, request
from flask_cors import CORS
import asyncio

from services.account_manager import login_akun, logout_akun, submit_otp, _clients
from services.group_manager import fetch_grup_dari_akun, baca_grup_tersimpan
from services.message_service import kirim_pesan_manual
from core.account_status import set_status as set_status_akun, pulihkan_akun, get_semua_aktif
from core.group_status import set_status_grup, pulihkan_grup, get_grup_aktif
from core.message_queue import (
    simpan_draft, ambil_semua_draft, hapus_draft,
    tambah_ke_antrian, ambil_semua_antrian,
    update_status_antrian, hapus_item_antrian
)
from core.send_history import ambil_riwayat_hari_ini, ringkasan_hari_ini, sudah_dikirim_hari_ini
from utils.storage import baca_json

app = Flask(__name__)
CORS(app)

def run(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# ── AKUN ──────────────────────────────────────────────────
@app.route("/api/akun", methods=["GET"])
def get_semua_akun():
    return jsonify(cek_status_semua())

@app.route("/api/akun/otp", methods=["POST"])
def api_submit_otp():
    b = request.json
    return jsonify(run(submit_otp(b.get("phone"), b.get("kode"), b.get("password"))))

@app.route("/api/akun/logout", methods=["POST"])
def post_logout_akun():
    return jsonify(run(logout_akun(request.json.get("phone"))))

@app.route("/api/akun/status", methods=["POST"])
def post_status_akun():
    body = request.json
    set_status_akun(body.get("phone"), body.get("status"))
    return jsonify({"ok": True})

@app.route("/api/akun/pulihkan", methods=["POST"])
def post_pulihkan_akun():
    pulihkan_akun(request.json.get("phone"))
    return jsonify({"ok": True})

# ── GRUP ──────────────────────────────────────────────────
@app.route("/api/grup", methods=["GET"])
def get_grup():
    return jsonify(baca_grup_tersimpan())

@app.route("/api/grup/aktif", methods=["GET"])
def get_grup_aktif():
    return jsonify(get_grup_aktif())

@app.route("/api/grup/fetch", methods=["POST"])
def post_fetch_grup():
    phone = request.json.get("phone")
    if not phone: return jsonify({"error": "Pilih akun dulu"}), 400
    return jsonify(run(fetch_grup_dari_akun(phone)))

@app.route("/api/grup/status", methods=["POST"])
def post_status_grup():
    body = request.json
    set_status_grup(int(body.get("grup_id")), body.get("status"))
    return jsonify({"ok": True})

@app.route("/api/grup/pulihkan", methods=["POST"])
def post_pulihkan_grup():
    pulihkan_grup(int(request.json.get("grup_id")))
    return jsonify({"ok": True})

# ── KIRIM LANGSUNG ────────────────────────────────────────
@app.route("/api/pesan/kirim", methods=["POST"])
def post_kirim_pesan():
    body = request.json
    phone, grup_id, pesan = body.get("phone"), body.get("grup_id"), body.get("pesan")
    if not all([phone, grup_id, pesan]):
        return jsonify({"error": "phone, grup_id, pesan wajib"}), 400
    return jsonify(run(kirim_pesan_manual(phone, grup_id, pesan)))

@app.route("/api/pesan/log", methods=["GET"])
def get_log_pesan():
    from datetime import datetime
    return jsonify(baca_json(f"data/log_{datetime.now().strftime('%Y-%m-%d')}.json"))

# ── DRAFT ─────────────────────────────────────────────────
@app.route("/api/draft", methods=["GET"])
def get_draft():
    return jsonify(ambil_semua_draft())

@app.route("/api/draft", methods=["POST"])
def post_draft():
    body = request.json
    return jsonify(simpan_draft(body.get("judul"), body.get("isi")))

@app.route("/api/draft/<int:draft_id>", methods=["DELETE"])
def delete_draft(draft_id):
    hapus_draft(draft_id)
    return jsonify({"ok": True})

# ── ANTRIAN ───────────────────────────────────────────────
@app.route("/api/antrian", methods=["GET"])
def get_antrian():
    return jsonify(ambil_semua_antrian())

@app.route("/api/antrian", methods=["POST"])
def post_antrian():
    body = request.json
    return jsonify(tambah_ke_antrian(
        body.get("phone"), int(body.get("grup_id")), body.get("pesan")
    ))

@app.route("/api/antrian/<int:item_id>/kirim", methods=["POST"])
def kirim_dari_antrian(item_id):
    antrian = ambil_semua_antrian()
    item    = next((a for a in antrian if a["id"] == item_id), None)
    if not item: return jsonify({"error": "Item tidak ditemukan"}), 404
    hasil      = run(kirim_pesan_manual(item["phone"], item["grup_id"], item["pesan"]))
    status_baru = "terkirim" if hasil["status"] == "berhasil" else "gagal"
    update_status_antrian(item_id, status_baru)
    return jsonify(hasil)

@app.route("/api/antrian/<int:item_id>", methods=["DELETE"])
def delete_antrian(item_id):
    hapus_item_antrian(item_id)
    return jsonify({"ok": True})

# ── RIWAYAT ───────────────────────────────────────────────
@app.route("/api/riwayat", methods=["GET"])
def get_riwayat():
    return jsonify(ambil_riwayat_hari_ini())

@app.route("/api/riwayat/ringkasan", methods=["GET"])
def get_ringkasan():
    return jsonify(ringkasan_hari_ini())

@app.route("/api/riwayat/cek/<int:grup_id>", methods=["GET"])
def cek_sudah_kirim(grup_id):
    return jsonify({"sudah_dikirim": sudah_dikirim_hari_ini(grup_id)})

if __name__ == "__main__":
    app.run(debug=True, port=5000)
