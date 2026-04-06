// ============================================================
// settings.js — Pengaturan Sistem (Bahasa Awam)
// ============================================================

async function muatTabSettings() {
    setLoading("settings-konten", "Memuat pengaturan...");
    try {
        const data = await _get("/settings");
        if (!data.length) {
            document.getElementById("settings-konten").innerHTML =
                `<div class="empty-state"><div class="icon">⚙️</div>
                 <p>Belum ada pengaturan. Jalankan migrate.py dulu.</p></div>`;
            return;
        }
        tampilSettings(data);
    } catch {
        document.getElementById("settings-konten").innerHTML =
            `<div class="empty-state"><div class="icon">⚠️</div><p>Gagal muat pengaturan.</p></div>`;
    }
}

// ── Label & catatan ramah pengguna ────────────────────────
// Setiap setting punya: label ringkas + catatan penjelasan
const _INFO = {
    // Auto Join
    auto_join_enabled:         { label: "Auto Join Aktif", note: "Akun otomatis masuk ke grup yang ditugaskan. Matikan kalau akun sering kena restrict dari Telegram." },
    auto_join_public_only:     { label: "Hanya join grup public", note: "Kalau aktif, hanya join grup yang punya @username. Grup tanpa username (private) dilewati. Disarankan aktif." },
    auto_join_max_per_cycle:   { label: "Maks join per akun per siklus (30 detik)", note: "Berapa grup yang boleh di-join oleh satu akun setiap 30 detik. Default 2 — jangan terlalu besar agar tidak kena ban." },
    auto_join_reserve_quota:   { label: "Sisakan kuota join harian", note: "Kalau sisa kuota join akun tinggal angka ini, sistem berhenti join. Default 2 — untuk menjaga akun tetap punya kuota darurat." },

    // Otomasi
    auto_import_enabled:       { label: "Auto Import Aktif", note: "Setelah scraper selesai, hasil grup otomatis masuk database. Kalau mati, kamu perlu klik 'Import' manual." },
    auto_assign_enabled:       { label: "Auto Assign Akun Aktif", note: "Sistem otomatis pilihkan akun mana yang akan 'pegang' tiap grup. Kalau mati, kamu perlu assign manual." },
    auto_campaign_enabled:     { label: "Auto Broadcast Aktif", note: "Sistem otomatis kirim pesan ke grup-grup yang sudah siap. Pastikan ada draft pesan aktif dulu." },
    auto_recovery_enabled:     { label: "Auto Recovery Aktif", note: "Kalau ada proses yang macet (assign gagal, broadcast stuck), sistem akan coba pulihkan sendiri. Disarankan tetap aktif." },
    maintenance_mode:          { label: "Mode Pemeliharaan", note: "Aktifkan ini kalau kamu sedang perbaikan sistem. Semua otomasi akan berhenti sementara." },
    pause_all_automation:      { label: "Hentikan Semua Otomasi Sementara", note: "Tombol darurat — hentikan semua proses otomatis tanpa mengubah pengaturan lainnya. Bisa diaktifkan/matikan kapan saja." },

    // Warming Level 1
    w1_hari_min:   { label: "Mulai hari ke berapa", note: "Akun yang umurnya di hari ke berapa mulai masuk Level Baru. Biasanya 0 (hari pertama login)." },
    w1_hari_max:   { label: "Sampai hari ke berapa", note: "Akun umur 0–7 hari dianggap 'Baru'. Setelah hari ke-7, naik ke Level Berkembang." },
    w1_maks_kirim: { label: "Maksimal kirim pesan per hari", note: "Akun baru hanya boleh kirim 5 pesan/hari agar tidak kena ban Telegram. Jangan terlalu besar untuk akun baru." },
    w1_maks_join:  { label: "Maksimal join grup per hari", note: "Akun baru hanya boleh join 3 grup/hari. Terlalu banyak join = akun kena restrict." },
    w1_jeda_kirim: { label: "Jeda antar kirim (detik)", note: "Tunggu 90 detik antar kirim pesan. Semakin lama = semakin aman dari deteksi spam." },
    w1_jeda_join:  { label: "Jeda antar join grup (detik)", note: "Tunggu 120 detik antar join grup. Penting untuk keamanan akun baru." },

    // Warming Level 2
    w2_hari_min:   { label: "Mulai hari ke berapa", note: "Akun mulai masuk Level Berkembang setelah hari ke-8." },
    w2_hari_max:   { label: "Sampai hari ke berapa", note: "Level Berkembang berlaku sampai hari ke-30." },
    w2_maks_kirim: { label: "Maksimal kirim pesan per hari", note: "Akun umur 8–30 hari boleh kirim 15 pesan/hari." },
    w2_maks_join:  { label: "Maksimal join grup per hari", note: "Akun berkembang boleh join 10 grup/hari." },
    w2_jeda_kirim: { label: "Jeda antar kirim (detik)", note: "Tunggu 45 detik antar kirim. Lebih cepat dari Level 1 karena akun sudah lebih dipercaya." },
    w2_jeda_join:  { label: "Jeda antar join grup (detik)", note: "Tunggu 60 detik antar join grup." },

    // Warming Level 3
    w3_hari_min:   { label: "Mulai hari ke berapa", note: "Akun masuk Level Dewasa setelah hari ke-31." },
    w3_hari_max:   { label: "Sampai hari ke berapa", note: "Level Dewasa berlaku sampai hari ke-90." },
    w3_maks_kirim: { label: "Maksimal kirim pesan per hari", note: "Akun dewasa boleh kirim 25 pesan/hari." },
    w3_maks_join:  { label: "Maksimal join grup per hari", note: "Akun dewasa boleh join 20 grup/hari." },
    w3_jeda_kirim: { label: "Jeda antar kirim (detik)", note: "Tunggu 30 detik antar kirim." },
    w3_jeda_join:  { label: "Jeda antar join grup (detik)", note: "Tunggu 30 detik antar join." },

    // Warming Level 4
    w4_hari_min:   { label: "Mulai hari ke berapa", note: "Akun masuk Level Terpercaya setelah hari ke-91." },
    w4_hari_max:   { label: "Sampai hari ke berapa", note: "Tidak ada batas atas (9999 = selamanya)." },
    w4_maks_kirim: { label: "Maksimal kirim pesan per hari", note: "Akun terpercaya boleh kirim 30 pesan/hari." },
    w4_maks_join:  { label: "Maksimal join grup per hari", note: "Akun terpercaya boleh join 30 grup/hari." },
    w4_jeda_kirim: { label: "Jeda antar kirim (detik)", note: "Tunggu 20 detik antar kirim. Akun lama sudah lebih aman." },
    w4_jeda_join:  { label: "Jeda antar join grup (detik)", note: "Tunggu 15 detik antar join." },

    // Scraper
    scraper_limit_per_keyword:     { label: "Jumlah hasil per kata kunci", note: "Scraper ambil maksimal 30 grup per kata kunci yang kamu masukkan. Naikkan kalau mau lebih banyak hasil." },
    scraper_min_members:           { label: "Minimal anggota grup", note: "Abaikan grup yang anggotanya di bawah angka ini. Isi 0 untuk ambil semua, isi 100 untuk ambil yang minimal 100 anggota." },
    scraper_recommended_score:     { label: "Batas skor rekomendasi grup", note: "Grup dengan skor di bawah ini dianggap 'kurang layak'. Isi 0 untuk ambil semua grup tanpa filter skor." },
    scraper_max_terms:             { label: "Maksimal total kata kunci per job", note: "Satu job scraper maksimal proses 80 kata kunci. Naikkan kalau kamu punya banyak keyword." },
    scraper_delay_keyword_min:     { label: "Jeda minimum antar kata kunci (detik)", note: "Jeda minimal sebelum scraper cari keyword berikutnya. Terlalu cepat bisa kena rate limit Telegram." },
    scraper_delay_keyword_max:     { label: "Jeda maksimum antar kata kunci (detik)", note: "Jeda maksimal sebelum scraper cari keyword berikutnya." },
    result_min_quality_score:      { label: "Skor minimum hasil scraper", note: "Grup dengan skor di bawah ini langsung diabaikan saat import. Turunkan kalau terlalu sedikit grup yang masuk." },
    result_username_required:      { label: "Wajib punya username", note: "Kalau aktif, grup tanpa username (@namgrup) tidak akan diimpor. Disarankan aktif agar broadcast lebih mudah." },
    result_allowed_entity_types:   { label: "Jenis entitas yang diizinkan", note: "Biasanya 'group,supergroup'. Jangan ubah kalau tidak tahu artinya." },

    // Assignment
    assignment_min_health_score:  { label: "Health score minimum akun", note: "Akun dengan health score di bawah angka ini tidak akan dipilih untuk pegang grup. Default 50. Turunkan ke 0 kalau semua akun tidak bisa lolos." },
    assignment_min_warming_level: { label: "Level warming minimum akun", note: "Akun harus minimal di level ini untuk bisa dipilih. 1=Baru, 2=Berkembang, 3=Dewasa, 4=Terpercaya. Default 1 (semua bisa)." },
    assignment_retry_count:       { label: "Berapa kali coba ulang assign", note: "Kalau assign gagal, sistem coba lagi sebanyak ini sebelum menyerah dan tandai sebagai gagal." },
    assignment_reassign_count:    { label: "Berapa kali boleh ganti akun", note: "Kalau akun yang dipilih tidak cocok, sistem boleh ganti ke akun lain sebanyak ini." },

    // Score Akun
    score_akun_bobot_umur:        { label: "Bobot penilaian: umur akun (%)", note: "Seberapa besar pengaruh umur akun terhadap skor total. Default 40% — akun lama lebih dipercaya." },
    score_akun_bobot_kesehatan:   { label: "Bobot penilaian: kesehatan (%)", note: "Seberapa besar pengaruh health score (tidak kena ban dll) terhadap skor total. Default 30%." },
    score_akun_bobot_performa:    { label: "Bobot penilaian: performa kirim (%)", note: "Seberapa besar pengaruh riwayat pengiriman (berhasil/gagal) terhadap skor total. Default 30%." },
    score_akun_terpercaya:        { label: "Batas skor label 'Terpercaya'", note: "Akun dengan skor di atas ini dapat label Terpercaya (hijau). Default 80." },
    score_akun_baik:              { label: "Batas skor label 'Baik'", note: "Akun dengan skor 60–79 dapat label Baik (biru)." },
    score_akun_perhatian:         { label: "Batas skor label 'Perlu Perhatian'", note: "Akun dengan skor di bawah 40 dapat label Perlu Perhatian (merah). Tandanya akun perlu dicek." },

    // Score Grup
    score_grup_bobot_member:      { label: "Bobot penilaian: jumlah anggota (%)", note: "Seberapa besar pengaruh jumlah anggota grup terhadap skor grup. Default 50%." },
    score_grup_bobot_riwayat:     { label: "Bobot penilaian: riwayat broadcast (%)", note: "Seberapa besar pengaruh riwayat pengiriman (sering berhasil = skor lebih tinggi). Default 50%." },
    score_grup_hot:               { label: "Batas skor grup 'Hot' 🔥", note: "Grup dengan skor di atas ini dianggap Hot (prioritas broadcast). Default 70." },
    score_grup_normal:            { label: "Batas skor grup 'Normal' ✅", note: "Grup dengan skor 30–69 dianggap Normal. Di bawah 30 dianggap Skip." },

    // Broadcast
    broadcast_jeda_min:           { label: "Jeda minimum antar kirim (detik)", note: "Setelah kirim ke satu grup, tunggu minimal 20 detik sebelum kirim ke grup berikutnya. Jangan kurang dari 15." },
    broadcast_jeda_max:           { label: "Jeda maksimum antar kirim (detik)", note: "Jeda acak sampai 60 detik. Variasi waktu membuat broadcast terlihat lebih natural." },
    broadcast_jadwal_ulang:       { label: "Kirim ulang ke grup setelah berapa hari", note: "Grup yang sudah dikirim baru bisa dikirim lagi setelah 3 hari. Sesuaikan dengan jadwal posting kamu." },
    broadcast_jadwal_aktif:       { label: "Aktifkan jadwal ulang otomatis", note: "Kalau aktif, grup yang sudah melewati masa tunggu otomatis siap dikirim lagi." },
    campaign_retry_delay_minutes: { label: "Tunggu berapa menit sebelum coba ulang kirim gagal", note: "Kalau pengiriman gagal, sistem tunggu 10 menit lalu coba lagi. Naikkan kalau akun sering kena rate limit." },

    // Guard / Filter Grup
    campaign_skip_inactive_groups_enabled: { label: "Tahan grup yang tidak aktif", note: "Kalau aktif, grup yang sepi (tidak ada chat selama X hari) tidak akan dikirim pesan. Hemat kuota akun." },
    campaign_inactive_threshold_days:      { label: "Berapa hari grup dianggap tidak aktif", note: "Grup yang tidak ada aktivitas selama 14 hari dianggap sepi dan ditahan. Naikkan kalau terlalu banyak grup ditahan." },
    campaign_skip_if_last_chat_is_ours:    { label: "Tahan jika pesan terakhir adalah pesan kita", note: "Kalau pesan terakhir di grup adalah pesan broadcast kita (belum ada balasan), tahan dulu. Mencegah spam beruntun." },
    campaign_group_cooldown_hours:         { label: "Jeda grup setelah berhasil dikirim (jam)", note: "Setelah berhasil kirim ke grup, grup itu 'istirahat' selama 72 jam sebelum bisa dikirim lagi." },

    // Pipeline
    assignment_broadcast_delay_minutes: { label: "Tahan grup baru sebelum broadcast (menit)", note: "Grup yang baru saja di-assign akun, ditahan 120 menit sebelum masuk antrian broadcast. Beri waktu akun untuk join grup dulu." },
    campaign_valid_permission_required:  { label: "Wajib punya izin valid sebelum broadcast", note: "Kalau aktif, grup yang belum diberi izin tidak akan dibroadcast. Disarankan aktif." },
    campaign_managed_required:           { label: "Wajib status 'Managed' sebelum broadcast", note: "Grup harus sudah punya akun owner yang terkonfirmasi join sebelum bisa dibroadcast." },
    campaign_session_target_limit:       { label: "Maksimal grup per sesi broadcast", note: "Satu sesi broadcast maksimal kirim ke 50 grup. Naikkan kalau punya banyak grup dan akun." },
    campaign_session_per_sender_limit:   { label: "Maksimal grup per akun per siklus", note: "Tiap akun hanya kirim ke maksimal 5 grup per putaran siklus. Sebar beban antar akun." },
    campaign_allow_mid_session_enqueue:  { label: "Izinkan grup baru masuk sesi yang sedang berjalan", note: "Kalau nonaktif (disarankan), grup baru masuk sesi broadcast berikutnya — tidak menyela sesi yang sedang jalan." },
    campaign_requeue_sender_missing:     { label: "Antrekan ulang jika akun tidak siap", note: "Kalau akun yang ditugaskan offline, target broadcast dimasukkan ulang ke antrian untuk dicoba nanti." },

    // Orchestrator (teknis, bisa biarkan default)
    orchestrator_interval_seconds:   { label: "Seberapa sering sistem cek semua proses (detik)", note: "Setiap 30 detik, sistem cek apakah ada yang perlu diimport, di-assign, atau dikirim. Jangan terlalu kecil (minimum 15)." },
    orchestrator_import_batch:       { label: "Berapa grup diproses per siklus import", note: "Teknis — biarkan default 10. Naikkan kalau import terasa lambat." },
    orchestrator_permission_batch:   { label: "Berapa grup diberi izin per siklus", note: "Teknis — biarkan default 100." },
    orchestrator_assign_batch:       { label: "Berapa grup di-assign per siklus", note: "Teknis — biarkan default 100." },
    orchestrator_campaign_batch:     { label: "Berapa grup dimasukkan campaign per siklus", note: "Teknis — biarkan default 200." },
    orchestrator_delivery_batch:     { label: "Berapa pesan dikirim per siklus", note: "Teknis — biarkan default 10. Jangan terlalu besar agar tidak spam." },
    orchestrator_recovery_batch:     { label: "Berapa item recovery diproses per siklus", note: "Teknis — biarkan default 25." },

    // Recovery
    recovery_resume_on_restart:                { label: "Lanjutkan proses yang belum selesai saat server restart", note: "Kalau server restart, proses yang belum selesai akan dilanjutkan otomatis. Disarankan aktif." },
    recovery_mark_partial_if_worker_missing:   { label: "Tandai sebagai 'Partial' jika proses hilang tiba-tiba", note: "Kalau proses tiba-tiba hilang tanpa selesai, tandai sebagai parsial agar bisa dipulihkan." },
    recovery_stuck_scrape_threshold:           { label: "Berapa menit scraper dianggap macet", note: "Kalau scraper tidak ada aktivitas selama 30 menit, dianggap macet dan perlu recovery." },
    recovery_stuck_assignment_threshold:       { label: "Berapa menit assign dianggap macet", note: "Kalau proses assign tidak selesai dalam 30 menit, dianggap macet." },
    recovery_stuck_campaign_threshold:         { label: "Berapa menit broadcast dianggap macet", note: "Kalau broadcast tidak selesai dalam 30 menit, dianggap macet." },
};

function _infoSetting(key) {
    return _INFO[key] || { label: key, note: '' };
}


const _SETTINGS_MOVED_TO_AUTOMATION = new Set([
    // status/toggle card otomasi
    'auto_import_enabled','auto_permission_enabled','auto_assign_enabled','auto_join_enabled','auto_campaign_enabled','auto_recovery_enabled',
    'maintenance_mode','pause_all_automation',

    // scraper & import
    'scraper_limit_per_keyword','scraper_min_members','scraper_recommended_score','scraper_max_terms','scraper_delay_keyword_min','scraper_delay_keyword_max',
    'result_min_quality_score','result_username_required','result_allowed_entity_types',

    // assign & join
    'assignment_min_health_score','assignment_min_warming_level','assignment_retry_count','assignment_reassign_count',
    'auto_join_public_only','auto_join_max_per_cycle','auto_join_reserve_quota',

    // broadcast legacy
    'assignment_broadcast_delay_minutes','broadcast_jeda_min','broadcast_jeda_max','broadcast_jadwal_ulang','broadcast_jadwal_aktif',
    'campaign_group_cooldown_hours','campaign_skip_inactive_groups_enabled','campaign_inactive_threshold_days','campaign_skip_if_last_chat_is_ours',
    'campaign_retry_delay_minutes','campaign_valid_permission_required','campaign_managed_required','campaign_session_target_limit',
    'campaign_session_per_sender_limit','campaign_allow_mid_session_enqueue','campaign_requeue_sender_missing',

    // pipeline / orchestrator legacy
    'orchestrator_interval_seconds','orchestrator_import_batch','orchestrator_permission_batch','orchestrator_assign_batch',
    'orchestrator_campaign_batch','orchestrator_delivery_batch','orchestrator_recovery_batch',

    // recovery legacy
    'recovery_resume_on_restart','recovery_mark_partial_if_worker_missing','recovery_stuck_scrape_threshold',
    'recovery_stuck_assignment_threshold','recovery_stuck_campaign_threshold',

    // scoring legacy
    'score_akun_bobot_umur','score_akun_bobot_kesehatan','score_akun_bobot_performa','score_akun_terpercaya','score_akun_baik','score_akun_perhatian',
    'score_grup_bobot_member','score_grup_bobot_riwayat','score_grup_hot','score_grup_normal',

    // key baru popup penilaian akun
    'score_akun_batas_terpercaya','score_akun_batas_baik','score_akun_batas_perlu_perhatian',
    'score_akun_batas_umur_baru_hari','score_akun_batas_umur_berkembang_hari','score_akun_batas_umur_matang_hari',
    'score_akun_nilai_umur_baru','score_akun_nilai_umur_berkembang','score_akun_nilai_umur_matang','score_akun_nilai_umur_lama',
    'score_akun_penalti_flood_ringan','score_akun_penalti_flood_sedang','score_akun_penalti_flood_berat','score_akun_penalti_cooldown',
    'score_akun_penalti_gagal_kirim_terbaru','score_akun_banned_jadi_nol','score_akun_penalti_banned',
    'score_akun_batas_performa_sangat_baik_persen','score_akun_batas_performa_baik_persen','score_akun_batas_performa_cukup_persen',
    'score_akun_nilai_awal_tanpa_riwayat','score_akun_bonus_online','score_akun_bonus_stabil','score_akun_bonus_riwayat_bersih',

    // key baru popup penilaian grup
    'score_grup_bobot_ukuran','score_grup_bobot_aktivitas','score_grup_bobot_akses','score_grup_batas_hot','score_grup_batas_normal','score_grup_batas_skip',
    'score_grup_batas_sangat_kecil_member','score_grup_batas_kecil_member','score_grup_batas_menengah_member','score_grup_batas_besar_member',
    'score_grup_nilai_sangat_kecil','score_grup_nilai_kecil','score_grup_nilai_menengah','score_grup_nilai_besar','score_grup_nilai_sangat_besar',
    'score_grup_batas_riwayat_sangat_baik_persen','score_grup_batas_riwayat_baik_persen','score_grup_batas_riwayat_cukup_persen','score_grup_nilai_awal_tanpa_riwayat',
    'score_grup_batas_aktif_hari','score_grup_bonus_aktif','score_grup_penalti_sepi','score_grup_bonus_publik','score_grup_penalti_private',
    'score_grup_penalti_sulit_dijangkau','score_grup_penalti_gagal_broadcast','score_grup_penalti_hold_berulang','score_grup_penalti_sender_tidak_siap',

    // key baru popup auto broadcast
    'broadcast_enabled','broadcast_jeda_kirim_min_detik','broadcast_jeda_kirim_max_detik','broadcast_masa_tunggu_setelah_assign_menit','broadcast_cooldown_grup_jam',
    'broadcast_tahan_grup_sepi','broadcast_batas_grup_sepi_hari','broadcast_tahan_jika_chat_terakhir_milik_sendiri','broadcast_retry_gagal_enabled',
    'broadcast_retry_delay_detik','broadcast_requeue_jika_sender_tidak_siap','broadcast_target_per_sesi','broadcast_target_per_akun_per_sesi',
    'broadcast_izinkan_grup_baru_masuk_sesi_berjalan','broadcast_batch_delivery','broadcast_hanya_pakai_draft_aktif',
    'broadcast_skip_jika_terakhir_dikirim_hari_ini','broadcast_skip_jika_grup_dalam_hold','broadcast_skip_jika_sender_dalam_cooldown',

    // key baru popup mode otomasi penuh
    'pipeline_enabled','pipeline_pause_semua','pipeline_maintenance_mode','pipeline_interval_detik','pipeline_batch_import','pipeline_batch_permission',
    'pipeline_batch_assign','pipeline_batch_campaign','pipeline_batch_delivery','pipeline_batch_recovery','pipeline_wajib_permission_valid',
    'pipeline_wajib_status_managed_untuk_broadcast','pipeline_sender_pool_default','pipeline_retry_umum_enabled','pipeline_retry_maks_per_item',
    'pipeline_retry_jeda_detik','pipeline_izinkan_proses_paralel','pipeline_hentikan_jika_error_beruntun','pipeline_batas_error_beruntun',
    'pipeline_lanjutkan_proses_setelah_restart','pipeline_tandai_proses_setengah_jalan'
]);

function _isMovedToAutomation(key) {
    return _SETTINGS_MOVED_TO_AUTOMATION.has(key);
}


// ── Render halaman settings ───────────────────────────────
function tampilSettings(data) {
    const map = {};
    data.forEach(item => { map[item.key] = item; });
    const movedCount = data.filter(item => _isMovedToAutomation(item.key)).length;

    const seksi = [

        {
            judul: "🌱 Batas Akun Baru (0–7 hari)",
            desc: "Akun yang baru login perlu diperlakukan hati-hati agar tidak kena ban Telegram.",
            keys: ["w1_maks_kirim","w1_maks_join","w1_jeda_kirim","w1_jeda_join","w1_hari_min","w1_hari_max"]
        },
        {
            judul: "📈 Batas Akun Berkembang (8–30 hari)",
            desc: "Akun yang sudah lebih dari seminggu boleh lebih aktif.",
            keys: ["w2_maks_kirim","w2_maks_join","w2_jeda_kirim","w2_jeda_join","w2_hari_min","w2_hari_max"]
        },
        {
            judul: "✅ Batas Akun Dewasa (31–90 hari)",
            desc: "Akun yang sudah sebulan lebih bisa kirim lebih banyak.",
            keys: ["w3_maks_kirim","w3_maks_join","w3_jeda_kirim","w3_jeda_join","w3_hari_min","w3_hari_max"]
        },
        {
            judul: "⭐ Batas Akun Terpercaya (90+ hari)",
            desc: "Akun tertua dan paling aman — boleh paling aktif.",
            keys: ["w4_maks_kirim","w4_maks_join","w4_jeda_kirim","w4_jeda_join","w4_hari_min","w4_hari_max"]
        },










    ];

    let html = `
        <div class="section-card" style="margin-bottom:20px;border:1px solid #dbeafe;background:#eff6ff">
            <h3 style="margin-bottom:6px">🧭 Beda fungsi menu ini dengan card otomasi</h3>
            <div style="font-size:13px;color:#475569;line-height:1.7">
                Aturan yang sudah tersedia di <strong>card</strong> dan <strong>pop-up pengaturan card otomasi</strong> tidak ditampilkan lagi di menu ini.
                Jadi menu ini sekarang dipakai hanya untuk <strong>pengaturan umum</strong> dan <strong>pengaturan lanjutan</strong> yang memang belum punya card atau pop-up sendiri.
                ${movedCount ? `<br><span style="display:inline-block;margin-top:8px;color:#1d4ed8">${movedCount} aturan otomasi kini hanya dikelola dari card/pop-up agar tidak tertukar fungsi.</span>` : ''}
            </div>
            <div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap">
                <button type="button" class="btn-outline btn-sm" onclick="tampilTab('automation')">🤖 Buka Pusat Kendali Otomasi</button>
            </div>
        </div>
        <form id="form-settings">`;

    for (const seksiItem of seksi) {
        const items = seksiItem.keys.map(k => map[k]).filter(Boolean).filter(item => !_isMovedToAutomation(item.key));
        if (!items.length) continue;

        html += `
            <div class="section-card" style="margin-bottom:20px">
                <h3 style="margin-bottom:4px">${seksiItem.judul}</h3>
                <div style="font-size:12px;color:#888;margin-bottom:14px;line-height:1.6">${seksiItem.desc}</div>
                <div class="settings-grid">
                    ${items.map(item => _renderInput(item)).join("")}
                </div>
            </div>`;
    }

    html += `
        <div style="margin-bottom:24px;display:flex;gap:12px;flex-wrap:wrap;align-items:center">
            <button type="button" class="btn-success"
                    onclick="simpanSettings()"
                    style="padding:12px 28px;font-size:15px">
                💾 Simpan Semua Pengaturan
            </button>
            <span style="font-size:12px;color:#888">Perubahan langsung berlaku setelah disimpan.</span>
        </div>
        <div id="pesan-settings" class="pesan-status"></div>
    </form>`;

    document.getElementById("settings-konten").innerHTML = html;
}

async function simpanSettings() {
    const data = {};
    document.querySelectorAll(".settings-input").forEach(i => {
        data[i.name] = i.value;
    });
    tampilPesan("pesan-settings", "⏳ Menyimpan...", "info");
    try {
        const hasil = await _post("/settings", data);
        tampilPesan("pesan-settings",
            hasil.ok ? "✅ Pengaturan berhasil disimpan!" : "❌ Gagal simpan.",
            hasil.ok ? "berhasil" : "gagal");
    } catch {
        tampilPesan("pesan-settings", "❌ Gagal konek ke backend.", "gagal");
    }
}

function _renderInput(item) {
    const info  = _infoSetting(item.key);
    const label = info.label || item.label || item.key;
    const note  = info.note || '';
    const tipe  = item.tipe || 'text';
    const value = item.value ?? '';

    let inputHtml = '';
    if (tipe === 'boolean') {
        const aktif = String(value) === '1' || String(value).toLowerCase() === 'true';
        inputHtml = `
            <select name="${item.key}" class="settings-input settings-select">
                <option value="1" ${aktif ? 'selected' : ''}>✅ Aktif</option>
                <option value="0" ${!aktif ? 'selected' : ''}>⭕ Nonaktif</option>
            </select>`;
    } else if (tipe === 'number') {
        inputHtml = `<input type="number" name="${item.key}" value="${value}" class="settings-input" min="0">`;
    } else {
        inputHtml = `<input type="text" name="${item.key}" value="${value}" class="settings-input">`;
    }

    return `
        <div class="settings-item">
            <label style="font-weight:600;font-size:13px;color:#1e2a3a">${label}</label>
            ${inputHtml}
            ${note ? `<div class="settings-note">💡 ${note}</div>` : ''}
        </div>`;
}
