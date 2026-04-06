// ============================================================
// automation.js — Pusat Kendali Otomasi + Pop-up Pengaturan Awam
// ============================================================

let AUTOMATION_LAST_STATE = null;
let AUTOMATION_LAST_PAYLOAD = null;
const AUTOMATION_MODAL_STATE = { kind: null, id: null };

const AUTOMATION_MASTER_DEF = {
    id: 'master',
    icon: '⚡',
    title: 'Mode Otomasi Penuh',
    summary: 'Menjalankan alur otomatis dari hasil scraper sampai recovery. Cocok bila Anda ingin sistem bekerja sendiri dengan pengawasan minimal.',
    caraKerja: [
        'Scraper mencari grup baru berdasarkan kata kunci.',
        'Auto Import memilih hasil yang layak masuk ke database grup.',
        'Auto Permission memberi izin awal, lalu Auto Assign memilih akun owner.',
        'Auto Join memasukkan akun ke grup, lalu Auto Broadcast mengirim pesan secara bertahap.',
        'Recovery memantau proses yang macet dan mencoba memulihkannya.',
    ],
    settings: [
        {
            key: 'pipeline_enabled', label: 'Aktifkan mode otomasi penuh', kind: 'boolean', fallback: '1',
            note: 'Saklar utama untuk menyalakan alur otomatis penuh.',
            effect: 'Jika dimatikan, mesin utama berhenti memproses alur otomatis walaupun card lain masih aktif.',
        },
        {
            key: 'pipeline_pause_semua', label: 'Pause semua otomasi', kind: 'boolean', fallback: '0',
            note: 'Menghentikan sementara seluruh alur otomatis tanpa menghapus pengaturan.',
            effect: 'Berguna saat Anda ingin berhenti sebentar untuk mengecek sistem.',
        },
        {
            key: 'pipeline_maintenance_mode', label: 'Mode maintenance', kind: 'boolean', fallback: '0',
            note: 'Dipakai saat Anda sedang melakukan pengecekan atau perbaikan sistem.',
            effect: 'Saat aktif, sistem cenderung tidak menjalankan alur otomatis agar lebih aman.',
        },
        {
            key: 'pipeline_interval_detik', label: 'Jeda antar putaran otomasi (detik)', kind: 'number', fallback: 30,
            note: 'Menentukan seberapa sering sistem memeriksa seluruh tahap otomatis.',
            effect: 'Semakin kecil nilainya, sistem lebih cepat bergerak. Semakin besar nilainya, beban sistem lebih ringan.',
        },
        {
            key: 'pipeline_batch_import', label: 'Batas import per putaran', kind: 'number', fallback: 100,
            note: 'Berapa hasil scraper yang diperiksa dalam satu putaran kerja.',
            effect: 'Mempengaruhi kecepatan hasil scraper masuk ke database grup.',
        },
        {
            key: 'pipeline_batch_permission', label: 'Batas permission per putaran', kind: 'number', fallback: 100,
            note: 'Berapa grup yang diberi keputusan izin dalam satu putaran.',
            effect: 'Semakin besar nilainya, semakin cepat grup baru diputuskan boleh lanjut atau tidak.',
        },
        {
            key: 'pipeline_batch_assign', label: 'Batas assign per putaran', kind: 'number', fallback: 50,
            note: 'Berapa grup yang dicari owner-nya dalam satu putaran.',
            effect: 'Mempengaruhi kecepatan grup mendapatkan akun penanggung jawab.',
        },
        {
            key: 'pipeline_batch_campaign', label: 'Batas grup siap broadcast per putaran', kind: 'number', fallback: 50,
            note: 'Berapa grup yang disiapkan ke antrean broadcast dalam satu putaran.',
            effect: 'Mempengaruhi seberapa cepat grup masuk antrean pengiriman.',
        },
        {
            key: 'pipeline_batch_delivery', label: 'Batas kirim per putaran', kind: 'number', fallback: 20,
            note: 'Berapa grup yang diproses kirim pesan dalam satu putaran worker.',
            effect: 'Mempengaruhi kecepatan pesan keluar ke grup.',
        },
        {
            key: 'pipeline_batch_recovery', label: 'Batas recovery per putaran', kind: 'number', fallback: 30,
            note: 'Berapa kasus macet yang dicoba dipulihkan dalam satu putaran.',
            effect: 'Semakin besar nilainya, antrian masalah lebih cepat dibersihkan.',
        },
        {
            key: 'pipeline_wajib_permission_valid', label: 'Wajib permission valid sebelum lanjut', kind: 'boolean', fallback: '1',
            note: 'Memastikan grup lolos izin lebih dulu sebelum boleh diproses lebih jauh.',
            effect: 'Membuat alur lebih aman dan lebih rapi.',
        },
        {
            key: 'pipeline_wajib_status_managed_untuk_broadcast', label: 'Wajib status siap penuh sebelum broadcast', kind: 'boolean', fallback: '1',
            note: 'Memastikan grup benar-benar siap sebelum boleh dikirimi pesan.',
            effect: 'Mencegah grup yang belum matang dipaksa masuk tahap kirim.',
        },
        {
            key: 'pipeline_sender_pool_default', label: 'Pool akun default', kind: 'text', fallback: 'utama',
            note: 'Kelompok akun default yang dipakai saat sistem memilih pengirim.',
            effect: 'Memudahkan Anda mengarahkan alur ke kelompok akun tertentu.',
        },
        {
            key: 'pipeline_retry_umum_enabled', label: 'Coba ulang otomatis jika gagal ringan', kind: 'boolean', fallback: '1',
            note: 'Menentukan apakah item gagal ringan boleh dicoba lagi secara otomatis.',
            effect: 'Membantu proses tidak berhenti hanya karena gangguan kecil sementara.',
        },
        {
            key: 'pipeline_retry_maks_per_item', label: 'Maksimal retry per item', kind: 'number', fallback: 3,
            note: 'Berapa kali satu item boleh dicoba ulang.',
            effect: 'Semakin besar nilainya, sistem lebih sabar. Semakin kecil nilainya, item lebih cepat dianggap gagal.',
        },
        {
            key: 'pipeline_retry_jeda_detik', label: 'Jeda retry umum (detik)', kind: 'number', fallback: 120,
            note: 'Waktu tunggu sebelum sistem mencoba item gagal lagi.',
            effect: 'Mencegah percobaan ulang yang terlalu rapat.',
        },
        {
            key: 'pipeline_lanjutkan_proses_setelah_restart', label: 'Lanjutkan proses setelah aplikasi hidup lagi', kind: 'boolean', fallback: '1',
            note: 'Saat aplikasi restart, proses yang masih aman akan dilanjutkan.',
            effect: 'Membantu pekerjaan tidak terhenti hanya karena aplikasi sempat mati.',
        },
        {
            key: 'pipeline_tandai_proses_setengah_jalan', label: 'Tandai proses setengah jalan', kind: 'boolean', fallback: '1',
            note: 'Menandai proses yang belum selesai agar mudah dipulihkan.',
            effect: 'Status proses menjadi lebih jujur dan lebih mudah ditinjau ulang.',
        },
    ],
};

const AUTOMATION_SCORE_DEFS = {
    akun: {
        id: 'akun',
        icon: '🏷️',
        title: 'Penilaian Akun',
        summary: 'Popup ini mengatur cara sistem memberi nilai pada akun Telegram. Nilai akun dipakai terutama saat Auto Assign dan saat memilih akun yang lebih aman.',
        caraKerja: [
            'Sistem melihat umur akun, kondisi kesehatan akun, dan performa kirimnya.',
            'Nilai setiap bagian digabung menjadi skor 0 sampai 100.',
            'Semakin tinggi nilainya, semakin besar peluang akun dianggap aman dan layak dipakai.',
            'Penalti menurunkan nilai bila akun terkena flood, cooldown, atau punya riwayat masalah terbaru.',
        ],
        settings: [
            { key: 'score_akun_bobot_umur', label: 'Bobot umur akun', kind: 'number', fallback: 25, note: 'Menentukan seberapa besar umur akun mempengaruhi nilai akhir.', effect: 'Semakin besar nilainya, umur akun semakin berpengaruh.' },
            { key: 'score_akun_bobot_kesehatan', label: 'Bobot kesehatan akun', kind: 'number', fallback: 45, note: 'Menentukan seberapa besar kondisi akun mempengaruhi nilai akhir.', effect: 'Kesehatan akun menjadi faktor utama saat memilih akun.' },
            { key: 'score_akun_bobot_performa', label: 'Bobot performa akun', kind: 'number', fallback: 30, note: 'Menentukan seberapa besar hasil kerja akun sebelumnya mempengaruhi nilai akhir.', effect: 'Akun dengan riwayat kerja lebih baik lebih diutamakan.' },
            { key: 'score_akun_batas_terpercaya', label: 'Batas label Terpercaya', kind: 'number', fallback: 80, note: 'Akun di atas angka ini dianggap sangat aman.', effect: 'Dipakai untuk label hijau pada akun.' },
            { key: 'score_akun_batas_baik', label: 'Batas label Baik', kind: 'number', fallback: 60, note: 'Akun di atas angka ini masih dianggap layak.', effect: 'Dipakai untuk label kuning pada akun.' },
            { key: 'score_akun_batas_perlu_perhatian', label: 'Batas label Perlu Perhatian', kind: 'number', fallback: 40, note: 'Akun di bawah ini perlu diwaspadai.', effect: 'Membantu Anda membedakan akun yang makin berisiko.' },
            { key: 'score_akun_batas_umur_baru_hari', label: 'Batas umur akun baru (hari)', kind: 'number', fallback: 7, note: 'Akun hingga umur ini dianggap sangat baru.', effect: 'Akun baru biasanya dinilai lebih hati-hati.' },
            { key: 'score_akun_batas_umur_berkembang_hari', label: 'Batas umur akun berkembang (hari)', kind: 'number', fallback: 30, note: 'Akun sampai umur ini dianggap mulai berkembang.', effect: 'Membagi tahap umur akun agar penilaian lebih adil.' },
            { key: 'score_akun_batas_umur_matang_hari', label: 'Batas umur akun matang (hari)', kind: 'number', fallback: 90, note: 'Akun di atas batas ini dianggap matang atau lama.', effect: 'Akun matang mendapat nilai umur yang lebih baik.' },
            { key: 'score_akun_nilai_umur_baru', label: 'Nilai umur akun baru', kind: 'number', fallback: 20, note: 'Nilai dasar untuk akun yang masih sangat baru.', effect: 'Menentukan seberapa rendah akun baru dinilai.' },
            { key: 'score_akun_nilai_umur_berkembang', label: 'Nilai umur akun berkembang', kind: 'number', fallback: 50, note: 'Nilai dasar untuk akun yang mulai stabil.', effect: 'Menjadi tahap tengah sebelum akun dianggap matang.' },
            { key: 'score_akun_nilai_umur_matang', label: 'Nilai umur akun matang', kind: 'number', fallback: 75, note: 'Nilai dasar untuk akun yang sudah cukup umur.', effect: 'Membantu akun matang naik peringkat.' },
            { key: 'score_akun_nilai_umur_lama', label: 'Nilai umur akun lama', kind: 'number', fallback: 90, note: 'Nilai dasar untuk akun yang sudah lama.', effect: 'Akun lama lebih diutamakan bila tetap sehat.' },
            { key: 'score_akun_penalti_flood_ringan', label: 'Penalti flood ringan', kind: 'number', fallback: 10, note: 'Pengurangan nilai bila akun mengalami pembatasan ringan.', effect: 'Nilai akun turun sedikit agar sistem lebih berhati-hati.' },
            { key: 'score_akun_penalti_flood_sedang', label: 'Penalti flood sedang', kind: 'number', fallback: 25, note: 'Pengurangan nilai untuk pembatasan sedang.', effect: 'Akun lebih sering dihindari saat assign.' },
            { key: 'score_akun_penalti_flood_berat', label: 'Penalti flood berat', kind: 'number', fallback: 40, note: 'Pengurangan nilai untuk pembatasan berat.', effect: 'Akun cenderung turun jauh prioritasnya.' },
            { key: 'score_akun_penalti_cooldown', label: 'Penalti cooldown', kind: 'number', fallback: 15, note: 'Pengurangan nilai saat akun sedang masa istirahat.', effect: 'Mencegah akun yang belum siap dipaksa dipakai.' },
            { key: 'score_akun_penalti_gagal_kirim_terbaru', label: 'Penalti gagal kirim terbaru', kind: 'number', fallback: 10, note: 'Pengurangan nilai jika ada tanda kegagalan kirim terbaru.', effect: 'Akun yang baru bermasalah akan sedikit ditahan.' },
            { key: 'score_akun_banned_jadi_nol', label: 'Akun banned langsung jadi nol', kind: 'boolean', fallback: '1', note: 'Jika aktif, akun yang banned langsung dianggap tidak layak.', effect: 'Akun banned tidak akan diprioritaskan sama sekali.' },
            { key: 'score_akun_penalti_banned', label: 'Penalti banned', kind: 'number', fallback: 100, note: 'Dipakai bila mode jadi nol dimatikan.', effect: 'Menentukan seberapa besar akun banned tetap diturunkan nilainya.' },
            { key: 'score_akun_batas_performa_sangat_baik_persen', label: 'Batas performa sangat baik (%)', kind: 'number', fallback: 90, note: 'Keberhasilan di atas ini dianggap sangat baik.', effect: 'Akun dengan performa tinggi lebih diutamakan.' },
            { key: 'score_akun_batas_performa_baik_persen', label: 'Batas performa baik (%)', kind: 'number', fallback: 75, note: 'Keberhasilan di atas ini dianggap baik.', effect: 'Membagi kategori performa agar lebih jelas.' },
            { key: 'score_akun_batas_performa_cukup_persen', label: 'Batas performa cukup (%)', kind: 'number', fallback: 50, note: 'Keberhasilan minimum yang masih dianggap cukup.', effect: 'Akun di bawah ini dianggap perlu perhatian.' },
            { key: 'score_akun_nilai_awal_tanpa_riwayat', label: 'Nilai awal akun tanpa riwayat', kind: 'number', fallback: 55, note: 'Nilai awal bagi akun yang belum punya catatan kerja.', effect: 'Akun baru tidak langsung dianggap jelek atau terlalu bagus.' },
            { key: 'score_akun_bonus_online', label: 'Bonus akun aktif', kind: 'number', fallback: 5, note: 'Tambahan kecil bila akun aktif dan siap dipakai.', effect: 'Akun aktif sedikit lebih diutamakan.' },
            { key: 'score_akun_bonus_stabil', label: 'Bonus akun stabil', kind: 'number', fallback: 5, note: 'Tambahan kecil bila akun jarang bermasalah.', effect: 'Akun yang tenang dan stabil naik sedikit nilainya.' },
            { key: 'score_akun_bonus_riwayat_bersih', label: 'Bonus riwayat bersih', kind: 'number', fallback: 10, note: 'Tambahan bila akun punya riwayat kerja bersih.', effect: 'Akun yang aman lebih mudah terpilih.' },
        ],
    },
    grup: {
        id: 'grup',
        icon: '🏷️',
        title: 'Penilaian Grup',
        summary: 'Popup ini mengatur cara sistem memberi nilai pada grup. Nilai grup dipakai terutama saat seleksi awal, pemberian izin, dan saat memutuskan grup mana yang lebih layak diprioritaskan.',
        caraKerja: [
            'Sistem melihat ukuran grup, riwayat keberhasilan, aktivitas grup, dan kemudahan akses grup.',
            'Semua komponen digabung menjadi skor 0 sampai 100.',
            'Semakin tinggi nilainya, semakin besar peluang grup dianggap layak diproses.',
            'Penalti menurunkan nilai bila grup sepi, sering gagal, atau sulit dijangkau.',
        ],
        settings: [
            { key: 'score_grup_bobot_ukuran', label: 'Bobot ukuran grup', kind: 'number', fallback: 35, note: 'Menentukan seberapa besar jumlah anggota mempengaruhi nilai grup.', effect: 'Semakin besar nilainya, ukuran grup semakin menentukan skor.' },
            { key: 'score_grup_bobot_riwayat', label: 'Bobot riwayat grup', kind: 'number', fallback: 30, note: 'Menentukan seberapa besar riwayat keberhasilan mempengaruhi nilai grup.', effect: 'Grup dengan riwayat baik lebih diutamakan.' },
            { key: 'score_grup_bobot_aktivitas', label: 'Bobot aktivitas grup', kind: 'number', fallback: 20, note: 'Menentukan seberapa besar tingkat aktif grup mempengaruhi nilai.', effect: 'Grup aktif lebih mudah naik nilai.' },
            { key: 'score_grup_bobot_akses', label: 'Bobot akses grup', kind: 'number', fallback: 15, note: 'Menentukan seberapa besar kemudahan akses mempengaruhi nilai.', effect: 'Grup publik dan mudah dijangkau lebih diutamakan.' },
            { key: 'score_grup_batas_hot', label: 'Batas label Hot', kind: 'number', fallback: 80, note: 'Grup di atas angka ini dianggap sangat potensial.', effect: 'Dipakai untuk label grup terbaik.' },
            { key: 'score_grup_batas_normal', label: 'Batas label Normal', kind: 'number', fallback: 55, note: 'Grup di atas angka ini masih dianggap layak.', effect: 'Menjadi batas utama grup normal.' },
            { key: 'score_grup_batas_skip', label: 'Batas label Skip', kind: 'number', fallback: 35, note: 'Grup di bawah ini sebaiknya ditahan atau dilewati.', effect: 'Membantu menyingkirkan grup yang terlalu lemah.' },
            { key: 'score_grup_batas_sangat_kecil_member', label: 'Batas grup sangat kecil', kind: 'number', fallback: 100, note: 'Grup di bawah angka ini dianggap sangat kecil.', effect: 'Ukuran grup sangat kecil mendapat nilai rendah.' },
            { key: 'score_grup_batas_kecil_member', label: 'Batas grup kecil', kind: 'number', fallback: 1000, note: 'Grup sampai angka ini dianggap kecil.', effect: 'Membagi ukuran grup agar penilaian lebih halus.' },
            { key: 'score_grup_batas_menengah_member', label: 'Batas grup menengah', kind: 'number', fallback: 5000, note: 'Grup sampai angka ini dianggap menengah.', effect: 'Grup menengah dinilai lebih baik daripada grup kecil.' },
            { key: 'score_grup_batas_besar_member', label: 'Batas grup besar', kind: 'number', fallback: 10000, note: 'Grup sampai angka ini dianggap besar.', effect: 'Grup di atas ini dianggap sangat besar.' },
            { key: 'score_grup_nilai_sangat_kecil', label: 'Nilai grup sangat kecil', kind: 'number', fallback: 20, note: 'Nilai dasar untuk grup yang sangat kecil.', effect: 'Menentukan seberapa rendah grup sangat kecil dinilai.' },
            { key: 'score_grup_nilai_kecil', label: 'Nilai grup kecil', kind: 'number', fallback: 45, note: 'Nilai dasar untuk grup kecil.', effect: 'Menjadi tahap tengah untuk grup kecil.' },
            { key: 'score_grup_nilai_menengah', label: 'Nilai grup menengah', kind: 'number', fallback: 65, note: 'Nilai dasar untuk grup menengah.', effect: 'Grup menengah lebih mudah dianggap layak.' },
            { key: 'score_grup_nilai_besar', label: 'Nilai grup besar', kind: 'number', fallback: 80, note: 'Nilai dasar untuk grup besar.', effect: 'Grup besar lebih mudah diprioritaskan.' },
            { key: 'score_grup_nilai_sangat_besar', label: 'Nilai grup sangat besar', kind: 'number', fallback: 90, note: 'Nilai dasar untuk grup sangat besar.', effect: 'Grup sangat besar mendapat nilai ukuran tertinggi.' },
            { key: 'score_grup_batas_riwayat_sangat_baik_persen', label: 'Batas riwayat sangat baik (%)', kind: 'number', fallback: 90, note: 'Keberhasilan di atas ini dianggap sangat baik.', effect: 'Grup dengan riwayat sangat baik lebih diutamakan.' },
            { key: 'score_grup_batas_riwayat_baik_persen', label: 'Batas riwayat baik (%)', kind: 'number', fallback: 75, note: 'Keberhasilan di atas ini dianggap baik.', effect: 'Membagi kategori riwayat grup.' },
            { key: 'score_grup_batas_riwayat_cukup_persen', label: 'Batas riwayat cukup (%)', kind: 'number', fallback: 50, note: 'Keberhasilan minimum yang masih dianggap cukup.', effect: 'Di bawah ini grup makin sulit diprioritaskan.' },
            { key: 'score_grup_nilai_awal_tanpa_riwayat', label: 'Nilai awal grup tanpa riwayat', kind: 'number', fallback: 50, note: 'Nilai awal untuk grup baru yang belum punya catatan kerja.', effect: 'Grup baru tidak langsung dianggap jelek.' },
            { key: 'score_grup_batas_aktif_hari', label: 'Batas grup aktif (hari)', kind: 'number', fallback: 7, note: 'Jika masih aktif dalam rentang ini, grup dianggap hidup.', effect: 'Grup yang masih aktif mendapat nilai aktivitas lebih baik.' },
            { key: 'score_grup_bonus_aktif', label: 'Bonus grup aktif', kind: 'number', fallback: 10, note: 'Tambahan nilai untuk grup yang aktif.', effect: 'Grup aktif lebih mudah naik peringkat.' },
            { key: 'score_grup_penalti_sepi', label: 'Penalti grup sepi', kind: 'number', fallback: 15, note: 'Pengurangan nilai untuk grup yang jarang aktif.', effect: 'Grup sepi lebih sulit diprioritaskan.' },
            { key: 'score_grup_bonus_publik', label: 'Bonus grup publik', kind: 'number', fallback: 10, note: 'Tambahan nilai untuk grup publik yang mudah dijangkau.', effect: 'Grup publik sedikit lebih diutamakan.' },
            { key: 'score_grup_penalti_private', label: 'Penalti grup private', kind: 'number', fallback: 15, note: 'Pengurangan nilai untuk grup yang tidak punya akses publik.', effect: 'Grup private lebih hati-hati diproses.' },
            { key: 'score_grup_penalti_sulit_dijangkau', label: 'Penalti grup sulit dijangkau', kind: 'number', fallback: 10, note: 'Pengurangan nilai untuk grup yang sering bermasalah saat diproses.', effect: 'Membantu sistem menghindari grup yang merepotkan.' },
            { key: 'score_grup_penalti_gagal_broadcast', label: 'Penalti gagal broadcast', kind: 'number', fallback: 10, note: 'Pengurangan nilai bila grup sering gagal dikirimi pesan.', effect: 'Grup bermasalah turun prioritasnya.' },
            { key: 'score_grup_penalti_hold_berulang', label: 'Penalti hold berulang', kind: 'number', fallback: 10, note: 'Pengurangan nilai bila grup sering ditahan aturan keamanan.', effect: 'Grup yang berulang kali ditahan tidak terlalu diprioritaskan.' },
            { key: 'score_grup_penalti_sender_tidak_siap', label: 'Penalti sender tidak siap', kind: 'number', fallback: 5, note: 'Pengurangan nilai jika grup sering gagal karena akun pengirim tidak siap.', effect: 'Membantu memisahkan grup yang lebih sulit diproses.' },
        ],
    },
};

const AUTOMATION_CARD_DEFS = {
    scraper: {
        id: 'scraper',
        icon: '🧲',
        title: 'Scraper Grup',
        type: 'manual',
        statusKey: null,
        masuk: 'Anda menjalankan pencarian grup dari tab Scraper dan mengisi kata kunci pencarian.',
        keluar: 'Pencarian selesai, hasil tersimpan, dan siap diperiksa oleh Auto Import.',
        fungsi: 'Scraper bertugas mencari calon grup baru dari Telegram. Tahap ini belum memutuskan grup diterima atau ditolak, melainkan hanya mengumpulkan hasil pencarian.',
        caraKerja: [
            'Sistem membaca kata kunci yang Anda masukkan.',
            'Sistem mencari grup yang sesuai dengan kata kunci tersebut.',
            'Hasil pencarian disimpan sebagai calon grup.',
            'Hasil yang sudah selesai akan menunggu pemeriksaan pada tahap Auto Import.',
        ],
        statsHelp: {
            'job total': 'Jumlah seluruh pekerjaan pencarian yang tercatat.',
            'selesai': 'Jumlah pekerjaan pencarian yang sudah selesai dan hasilnya sudah tersedia.',
            'hasil ditemukan': 'Jumlah seluruh hasil grup yang berhasil ditemukan dari job scraper yang tercatat.',
            'siap diperiksa': 'Jumlah hasil scraper yang masih menunggu diperiksa oleh Auto Import.',
        },
        settings: [
            {
                key: 'scraper_limit_per_keyword', label: 'Hasil per kata kunci', kind: 'number', fallback: 30,
                note: 'Menentukan berapa banyak hasil yang diambil untuk setiap kata kunci.',
                effect: 'Semakin besar nilainya, hasil bisa lebih banyak tetapi proses bisa lebih lama.',
            },
            {
                key: 'scraper_min_members', label: 'Minimal anggota grup', kind: 'number', fallback: 0,
                note: 'Menyaring grup yang terlalu kecil sejak awal pencarian.',
                effect: 'Jika angka dinaikkan, grup kecil akan lebih banyak terlewat.',
            },
            {
                key: 'scraper_recommended_score', label: 'Batas rekomendasi awal', kind: 'number', fallback: 30,
                note: 'Dipakai sebagai patokan awal untuk menandai hasil yang terlihat lebih layak.',
                effect: 'Membantu Anda melihat hasil yang lebih potensial tanpa harus membuka semuanya satu per satu.',
            },
            {
                key: 'scraper_max_terms', label: 'Maksimal variasi kata kunci', kind: 'number', fallback: 80,
                note: 'Membatasi banyaknya variasi istilah yang dijalankan dalam satu job.',
                effect: 'Semakin besar nilainya, pencarian lebih luas tetapi beban proses lebih berat.',
            },
            {
                key: 'scraper_delay_keyword_min', label: 'Jeda minimum antar kata kunci (detik)', kind: 'number', fallback: 1,
                note: 'Memberi jeda agar pencarian tidak terlalu rapat.',
                effect: 'Jeda yang lebih longgar biasanya lebih aman, tetapi proses menjadi lebih lama.',
            },
            {
                key: 'scraper_delay_keyword_max', label: 'Jeda maksimum antar kata kunci (detik)', kind: 'number', fallback: 3,
                note: 'Dipakai untuk membuat ritme pencarian tidak terlalu kaku.',
                effect: 'Membantu proses terlihat lebih natural dan tidak terlalu seragam.',
            },
        ],
        primaryButton: { label: '🧲 Buka Tab Scraper', action: "tampilTab('scraper')" },
    },
    import: {
        id: 'import',
        icon: '📥',
        title: 'Auto Import',
        type: 'auto',
        statusKey: 'auto_import_enabled',
        masuk: 'Hasil scraper sudah selesai dan masih ada hasil baru yang belum diproses.',
        keluar: 'Grup lolos seleksi awal lalu masuk ke database grup utama.',
        fungsi: 'Auto Import adalah pintu masuk awal. Tidak semua hasil scraper langsung diterima. Card ini memilih grup yang cukup layak untuk masuk ke sistem.',
        caraKerja: [
            'Sistem membaca hasil scraper yang selesai.',
            'Setiap grup dicek berdasarkan skor, username, dan tipe grup.',
            'Grup yang lolos dimasukkan ke database utama.',
            'Grup yang tidak lolos berhenti di tahap ini dan tidak lanjut ke card berikutnya.',
        ],
        statsHelp: {
            'grup di db': 'Jumlah grup yang sudah berhasil tersimpan di database utama.',
            'hasil menunggu impor': 'Jumlah hasil scraper yang masih tersisa dan belum masuk ke database.',
            'job menunggu': 'Jumlah job scraper selesai yang masih punya hasil belum diproses seluruhnya.',
            'sudah masuk': 'Jumlah hasil scraper yang sudah berhasil dimasukkan ke database.',
        },
        settings: [
            {
                key: 'result_min_quality_score', label: 'Skor minimal grup', kind: 'number', fallback: 30,
                note: 'Ini adalah batas nilai minimum agar grup dianggap layak masuk.',
                effect: 'Jika nilainya dinaikkan, seleksi makin ketat. Jika diturunkan, lebih banyak grup akan diterima.',
            },
            {
                key: 'result_username_required', label: 'Wajib punya username', kind: 'boolean', fallback: '0',
                note: 'Saat aktif, grup harus punya @username publik untuk bisa masuk.',
                effect: 'Grup tanpa username akan tertahan di card ini dan tidak masuk database.',
            },
            {
                key: 'result_allowed_entity_types', label: 'Tipe yang boleh masuk', kind: 'text', fallback: 'group,supergroup',
                note: 'Isi daftar tipe yang boleh diterima, misalnya group,supergroup.',
                effect: 'Tipe yang tidak ada di daftar ini, seperti channel, akan ditolak.',
            },
        ],
        extraActions: [{ label: '🏷️ Skor Grup', onclick: "openAutomationSettingsModal('score','grup')" }],
    },
    permission: {
        id: 'permission',
        icon: '🔐',
        title: 'Auto Permission',
        type: 'auto',
        statusKey: 'auto_permission_enabled',
        masuk: 'Grup sudah ada di database dan status izinnya belum ditentukan.',
        keluar: 'Grup diberi izin otomatis lalu boleh lanjut ke pembagian akun.',
        fungsi: 'Auto Permission memutuskan apakah grup baru boleh diteruskan ke tahap berikutnya. Ibaratnya, ini adalah pemeriksaan izin awal sebelum grup dipakai lebih jauh.',
        caraKerja: [
            'Sistem mengambil grup baru yang belum memiliki izin.',
            'Grup dicek berdasarkan aturan skor, username, dan tipe grup.',
            'Jika lolos, sistem membuat izin otomatis.',
            'Setelah itu grup bisa masuk ke Auto Assign.',
        ],
        statsHelp: {
            'izin aktif': 'Jumlah grup yang saat ini memiliki izin aktif.',
            'menunggu izin': 'Jumlah grup yang belum punya izin atau masih menunggu keputusan izin.',
            'kedaluwarsa': 'Jumlah grup yang izinnya sudah habis masa berlakunya atau tidak lagi aktif.',
        },
        settings: [
            {
                key: 'permission_min_score', label: 'Skor minimal untuk diberi izin', kind: 'number', fallback: 0,
                note: 'Jika Anda ingin lebih selektif, naikkan angka ini.',
                effect: 'Grup dengan skor di bawah angka ini tidak akan diberi izin otomatis.',
            },
            {
                key: 'permission_require_username', label: 'Username wajib untuk diberi izin', kind: 'boolean', fallback: '0',
                note: 'Saat aktif, grup harus punya @username agar boleh lanjut.',
                effect: 'Grup tanpa username akan tertahan pada tahap izin.',
            },
            {
                key: 'permission_exclude_channels', label: 'Abaikan channel', kind: 'boolean', fallback: '1',
                note: 'Channel biasanya tidak dipakai seperti grup diskusi biasa.',
                effect: 'Saat aktif, channel tidak diberi izin otomatis.',
            },
        ],
        extraActions: [{ label: '🏷️ Skor Grup', onclick: "openAutomationSettingsModal('score','grup')" }],
    },
    assign: {
        id: 'assign',
        icon: '🎯',
        title: 'Auto Assign',
        type: 'auto',
        statusKey: 'auto_assign_enabled',
        masuk: 'Grup sudah diizinkan dan belum punya akun penanggung jawab.',
        keluar: 'Grup mendapatkan akun owner yang dianggap paling cocok.',
        fungsi: 'Auto Assign memilih akun Telegram yang akan menangani sebuah grup. Tujuannya agar setiap grup memiliki akun owner sebelum tahap join dan broadcast.',
        caraKerja: [
            'Sistem mencari grup yang siap dibagikan ke akun.',
            'Daftar akun dibandingkan berdasarkan kesehatan, level warming, dan kapasitas.',
            'Akun yang paling cocok dipilih sebagai owner grup.',
            'Jika tidak ada akun yang cocok, grup tetap menunggu atau masuk recovery.',
        ],
        statsHelp: {
            'sudah punya owner': 'Jumlah grup yang sudah mendapatkan owner, baik masih menunggu join maupun sudah lanjut ke tahap berikutnya.',
            'siap dibagi': 'Jumlah grup aktif yang sudah lolos izin tetapi belum mendapat owner.',
            'menunggu join': 'Jumlah grup yang owner-nya sudah dipilih tetapi akun owner belum tercatat masuk ke grup. Angka ini disamakan dengan card Auto Join.',
            'gagal assign': 'Jumlah grup yang status assign terakhirnya gagal dan masih perlu perhatian.',
        },
        settings: [
            {
                key: 'assignment_min_health_score', label: 'Skor kesehatan akun minimum', kind: 'number', fallback: 0,
                note: 'Akun dengan kesehatan di bawah angka ini tidak akan dipilih.',
                effect: 'Semakin tinggi nilainya, semakin sedikit akun yang dianggap layak.',
            },
            {
                key: 'assignment_min_warming_level', label: 'Level warming minimum', kind: 'number', fallback: 1,
                note: 'Mencegah akun yang masih terlalu baru menerima grup terlalu cepat.',
                effect: 'Akun dengan level lebih rendah akan dilewati.',
            },
            {
                key: 'assignment_retry_count', label: 'Berapa kali coba ulang', kind: 'number', fallback: 3,
                note: 'Jumlah percobaan otomatis jika belum ditemukan akun yang cocok.',
                effect: 'Menentukan seberapa sabar sistem mencari owner sebelum dianggap gagal.',
            },
            {
                key: 'assignment_reassign_count', label: 'Batas pindah akun ulang', kind: 'number', fallback: 2,
                note: 'Digunakan saat grup perlu dipindah ke akun lain.',
                effect: 'Mencegah grup berpindah-pindah akun terlalu sering.',
            },
        ],
        extraActions: [{ label: '🏷️ Skor Akun', onclick: "openAutomationSettingsModal('score','akun')" }],
    },
    autojoin: {
        id: 'autojoin',
        icon: '🔗',
        title: 'Auto Join',
        type: 'auto',
        statusKey: 'auto_join_enabled',
        masuk: 'Grup sudah punya owner dan akun owner masih siap dipakai.',
        keluar: 'Akun owner berhasil bergabung ke grup sehingga grup siap dibroadcast.',
        fungsi: 'Auto Join membuat akun yang sudah ditugaskan benar-benar masuk ke grup. Tanpa tahap ini, akun belum bisa mengirim pesan ke grup tersebut.',
        caraKerja: [
            'Sistem memeriksa grup yang sudah punya owner.',
            'Kuota join harian akun dicek lebih dulu.',
            'Jika aman, akun mencoba join ke grup.',
            'Jika berhasil, status grup naik ke tahap siap broadcast.',
        ],
        statsHelp: {
            'berhasil join hari ini': 'Jumlah join yang berhasil dicatat hari ini dari data database.',
            'menunggu join': 'Jumlah grup yang sudah punya owner tetapi masih menunggu giliran join.',
            'gagal join hari ini': 'Jumlah percobaan join yang gagal hari ini.',
        },
        settings: [
            {
                key: 'auto_join_public_only', label: 'Hanya grup publik', kind: 'boolean', fallback: '1',
                note: 'Saat aktif, sistem hanya mencoba grup yang punya username publik.',
                effect: 'Grup private tidak dicoba pada Auto Join otomatis.',
            },
            {
                key: 'auto_join_max_per_cycle', label: 'Maksimal join per siklus', kind: 'number', fallback: 2,
                note: 'Membatasi banyaknya join dalam satu putaran kerja.',
                effect: 'Semakin kecil nilainya, proses lebih pelan tetapi biasanya lebih aman.',
            },
            {
                key: 'auto_join_reserve_quota', label: 'Sisa kuota join harian yang disimpan', kind: 'number', fallback: 2,
                note: 'Menyisakan cadangan agar akun tidak kehabisan kuota join harian.',
                effect: 'Akun akan berhenti join lebih cepat bila cadangan ini ingin dijaga.',
            },
        ],
    },
    campaign: {
        id: 'campaign',
        icon: '📡',
        title: 'Auto Broadcast',
        type: 'auto',
        statusKey: 'auto_campaign_enabled',
        masuk: 'Akun sudah join ke grup, draft aktif tersedia, dan grup dinilai aman untuk dikirimi pesan.',
        keluar: 'Pesan berhasil dikirim atau grup masuk masa istirahat setelah pengiriman.',
        fungsi: 'Auto Broadcast mengirim pesan otomatis ke grup yang sudah siap. Sistem tetap memeriksa jeda, masa tunggu, dan kondisi grup agar pengiriman lebih aman.',
        caraKerja: [
            'Sistem mengambil grup yang sudah siap broadcast.',
            'Draft aktif dipakai sebagai isi pesan.',
            'Sebelum kirim, sistem memeriksa aturan tahan, masa tunggu, dan status akun pengirim.',
            'Jika lolos, pesan dikirim lalu grup masuk masa istirahat.',
        ],
        statsHelp: {
            'terkirim hari ini': 'Jumlah pengiriman yang berhasil dicatat hari ini dari jalur broadcast otomatis.',
            'mulai istirahat': 'Jumlah grup yang mulai masuk cooldown hari ini setelah pesan berhasil dikirim.',
            'gagal kirim hari ini': 'Jumlah pengiriman yang gagal dicatat hari ini dari jalur broadcast otomatis.',
        },
        settings: [
            { key: 'broadcast_enabled', label: 'Aktifkan Auto Broadcast', kind: 'boolean', fallback: '0', note: 'Saklar utama untuk tahap kirim pesan otomatis.', effect: 'Jika dimatikan, card broadcast tidak berjalan walaupun grup sudah siap.' },
            { key: 'broadcast_masa_tunggu_setelah_assign_menit', label: 'Masa tunggu setelah assign/join (menit)', kind: 'number', fallback: 0, note: 'Memberi jeda sebelum grup baru langsung dikirimi pesan.', effect: 'Semakin besar nilainya, semakin lama grup menunggu sebelum boleh dikirim.' },
            { key: 'broadcast_jeda_kirim_min_detik', label: 'Jeda kirim minimum (detik)', kind: 'number', fallback: 3, note: 'Batas jeda tercepat antar pengiriman pesan.', effect: 'Mencegah sistem mengirim terlalu rapat.' },
            { key: 'broadcast_jeda_kirim_max_detik', label: 'Jeda kirim maksimum (detik)', kind: 'number', fallback: 7, note: 'Batas jeda terlama antar pengiriman pesan.', effect: 'Membuat ritme kirim lebih natural dan tidak seragam.' },
            { key: 'broadcast_cooldown_grup_menit', label: 'Masa istirahat grup setelah kirim (menit)', kind: 'number', fallback: 1, note: 'Setelah berhasil kirim, grup ditahan sebentar sebelum boleh dikirim lagi.', effect: 'Semakin kecil nilainya, grup lebih cepat kembali siap.' },
            { key: 'broadcast_cooldown_grup_jam', label: 'Cadangan masa istirahat (jam)', kind: 'number', fallback: 0, note: 'Dipakai bila pengaturan menit diisi 0. Untuk mode cepat, biarkan 0.', effect: 'Gunakan hanya bila ingin cooldown lebih lama dari hitungan menit.' },
            { key: 'broadcast_tahan_grup_sepi', label: 'Tahan grup yang sepi', kind: 'boolean', fallback: '1', note: 'Saat aktif, grup yang lama tidak aktif akan ditahan dulu.', effect: 'Grup sepi tidak langsung ikut broadcast.' },
            { key: 'broadcast_batas_grup_sepi_hari', label: 'Batas hari grup dianggap sepi', kind: 'number', fallback: 14, note: 'Dipakai hanya jika aturan tahan grup sepi dinyalakan.', effect: 'Menentukan sejak kapan grup dianggap tidak aktif.' },
            { key: 'broadcast_tahan_jika_chat_terakhir_milik_sendiri', label: 'Tahan jika pesan terakhir milik kita', kind: 'boolean', fallback: '1', note: 'Mencegah grup terasa diserang pesan berturut-turut dari akun sendiri.', effect: 'Grup akan ditunda bila chat terakhir masih dari akun sistem.' },
            { key: 'broadcast_retry_gagal_enabled', label: 'Coba ulang jika kirim gagal', kind: 'boolean', fallback: '1', note: 'Menentukan apakah target gagal boleh dicoba lagi.', effect: 'Kegagalan ringan tidak langsung dianggap selesai total.' },
            { key: 'broadcast_retry_delay_detik', label: 'Jeda coba ulang jika kirim gagal (detik)', kind: 'number', fallback: 60, note: 'Mengatur jarak waktu sebelum sistem mencoba ulang target yang gagal.', effect: 'Membantu menghindari percobaan ulang yang terlalu rapat.' },
            { key: 'broadcast_requeue_jika_sender_tidak_siap', label: 'Antrekan ulang jika akun tidak siap', kind: 'boolean', fallback: '1', note: 'Jika akun pengirim belum siap, target bisa dikembalikan ke antrean.', effect: 'Mencegah target langsung gagal permanen hanya karena akun sedang tidak siap.' },
            { key: 'broadcast_target_per_sesi', label: 'Maksimal grup per sesi broadcast', kind: 'number', fallback: 200, note: 'Membatasi jumlah grup yang ditargetkan dalam satu sesi kerja.', effect: 'Membantu mengatur ritme broadcast agar tetap terkendali.' },
            { key: 'broadcast_target_per_akun_per_sesi', label: 'Maksimal grup per akun per sesi', kind: 'number', fallback: 20, note: 'Membatasi beban satu akun dalam satu sesi kerja.', effect: 'Membuat distribusi pengiriman antar akun lebih merata.' },
            { key: 'broadcast_izinkan_grup_baru_masuk_sesi_berjalan', label: 'Izinkan grup baru masuk sesi yang sedang berjalan', kind: 'boolean', fallback: '1', note: 'Menentukan apakah grup yang baru siap boleh langsung ikut sesi saat ini.', effect: 'Jika dimatikan, grup baru menunggu sesi berikutnya.' },
            { key: 'broadcast_batch_delivery', label: 'Batch kirim per putaran', kind: 'number', fallback: 40, note: 'Jumlah target kirim yang diproses per putaran worker.', effect: 'Mempengaruhi kecepatan sesi broadcast bergerak.' },
            { key: 'broadcast_hanya_pakai_draft_aktif', label: 'Hanya pakai draft aktif', kind: 'boolean', fallback: '1', note: 'Memastikan sistem hanya mengirim pesan yang sedang dipilih sebagai draft aktif.', effect: 'Mengurangi risiko salah kirim konten.' },
        ],
        extraActions: [
            { label: '🏷️ Grup', onclick: "openAutomationSettingsModal('score','grup')" },
            { label: '🏷️ Akun', onclick: "openAutomationSettingsModal('score','akun')" },
        ],
    },

    recovery: {
        id: 'recovery',
        icon: '🛠️',
        title: 'Recovery',
        type: 'auto',
        statusKey: 'auto_recovery_enabled',
        masuk: 'Ada grup, assignment, atau campaign yang macet di tahap sebelumnya.',
        keluar: 'Sistem berhasil mencoba memperbaiki item yang macet atau menandainya untuk ditinjau.',
        fungsi: 'Recovery memantau proses yang macet lalu mencoba memulihkannya. Dengan begitu Anda tidak harus mencari masalah satu per satu secara manual.',
        caraKerja: [
            'Sistem mendeteksi item yang terlalu lama diam di satu tahap.',
            'Item tersebut ditandai sebagai butuh recovery.',
            'Recovery mencoba melanjutkan atau menandai hasilnya.',
            'Jika tidak berhasil, item tetap terlihat sebagai masalah aktif.',
        ],
        statsHelp: {
            'perlu recovery': 'Jumlah item yang saat ini dinilai perlu dipulihkan.',
            'assign macet': 'Jumlah kasus pembagian akun yang terlihat macet.',
            'job macet': 'Jumlah job atau proses lain yang terlalu lama diam.',
        },
        settings: [
            {
                key: 'recovery_resume_on_restart', label: 'Lanjutkan recovery saat sistem hidup lagi', kind: 'boolean', fallback: '1',
                note: 'Berguna bila aplikasi sempat dimatikan lalu dijalankan lagi.',
                effect: 'Item yang tertinggal akan dicek kembali saat sistem hidup.',
            },
            {
                key: 'recovery_mark_partial_if_worker_missing', label: 'Tandai parsial jika worker tidak ada', kind: 'boolean', fallback: '1',
                note: 'Mencegah item terlihat seolah sukses penuh padahal proses pendukung tidak tersedia.',
                effect: 'Hasil recovery lebih jujur dan mudah diperiksa ulang.',
            },
            {
                key: 'recovery_stuck_scrape_threshold', label: 'Batas scrape dianggap macet (menit)', kind: 'number', fallback: 30,
                note: 'Setelah melewati waktu ini, job scraper dianggap terlalu lama berhenti.',
                effect: 'Menentukan kapan job scraper masuk daftar recovery.',
            },
            {
                key: 'recovery_stuck_assignment_threshold', label: 'Batas assign dianggap macet (menit)', kind: 'number', fallback: 30,
                note: 'Digunakan untuk assignment yang tidak bergerak terlalu lama.',
                effect: 'Menentukan kapan assignment masuk recovery.',
            },
            {
                key: 'recovery_stuck_campaign_threshold', label: 'Batas broadcast dianggap macet (menit)', kind: 'number', fallback: 30,
                note: 'Dipakai untuk antrean broadcast yang terlalu lama diam.',
                effect: 'Menentukan kapan proses broadcast dianggap perlu dipulihkan.',
            },
        ],
    },
};

function _autoEscapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function _autoSafeText(value, fallback = '-') {
    const text = value == null ? '' : String(value).trim();
    return text || fallback;
}

function _autoBool(value, fallback = false) {
    if (value == null || value === '') return fallback;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') return value !== 0;
    return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

function _autoSettingValue(settingsMap, key, fallback = '') {
    if (settingsMap && Object.prototype.hasOwnProperty.call(settingsMap, key)) {
        return settingsMap[key]?.value ?? settingsMap[key];
    }
    return fallback;
}

async function _autoGetV2(path) {
    const r = await fetch(`${API_V2}${path}`);
    const data = await _readResponse(r);
    return data.data ?? data;
}

function _buildSettingsMap(rows) {
    const out = {};
    (rows || []).forEach((row) => { out[row.key] = row; });
    return out;
}

function _formatTimeText(text) {
    if (!text) return '';
    const normalized = String(text).replace(' ', 'T');
    const d = new Date(normalized);
    if (Number.isNaN(d.getTime())) return String(text);
    return d.toLocaleString('id-ID', { hour: '2-digit', minute: '2-digit', day: '2-digit', month: 'short' });
}


function _sameLocalDayFromText(text, baseDate = new Date()) {
    if (!text) return false;
    const normalized = String(text).replace(' ', 'T');
    const d = new Date(normalized);
    if (Number.isNaN(d.getTime())) return false;
    return d.getFullYear() === baseDate.getFullYear()
        && d.getMonth() === baseDate.getMonth()
        && d.getDate() === baseDate.getDate();
}

function _automationMessageBox() {
    let el = document.getElementById('automation-inline-message');
    if (!el) {
        el = document.createElement('div');
        el.id = 'automation-inline-message';
        el.style.marginTop = '12px';
        const panel = document.getElementById('automation-master-panel');
        if (panel) panel.appendChild(el);
    }
    return el;
}

function _showAutomationMessage(text, type = 'info') {
    const el = _automationMessageBox();
    if (!el) return;
    const colors = {
        info: ['#eff6ff', '#bfdbfe', '#1d4ed8'],
        success: ['#f0fdf4', '#bbf7d0', '#166534'],
        error: ['#fff5f5', '#fecaca', '#b91c1c'],
    };
    const [bg, border, color] = colors[type] || colors.info;
    el.innerHTML = `<div style="background:${bg};border:1px solid ${border};color:${color};padding:10px 12px;border-radius:10px;font-size:12px">${_autoEscapeHtml(text)}</div>`;
    clearTimeout(_showAutomationMessage._timer);
    _showAutomationMessage._timer = setTimeout(() => { if (el) el.innerHTML = ''; }, 3200);
}

function _hideLegacyAutomationSections() {
    document.getElementById('automation-summary-grid')?.style.setProperty('display', 'none');
    document.getElementById('list-automation')?.style.setProperty('display', 'none');
    document.getElementById('automation-search')?.closest('div')?.style.setProperty('display', 'none');
}

function _engineStatusBadge(active, manual = false) {
    if (manual) return `<span class="badge-mesin manual">Manual</span>`;
    return `<span class="badge-mesin ${active ? 'on' : 'off'}">${active ? 'Aktif' : 'Nonaktif'}</span>`;
}

function _renderStats(stats) {
    return `
        <div class="engine-stats automation-stats-grid">
            ${stats.map((item) => `
                <div class="engine-stat ${item.color || ''}" ${item.tooltip ? `title="${_autoEscapeHtml(item.tooltip)}"` : ''}>
                    <span class="es-num">${_autoSafeText(item.value, '0')}</span>
                    <span class="es-label">${_autoEscapeHtml(item.label)}</span>
                </div>
            `).join('')}
        </div>
    `;
}

function _renderActivityList(items, limit = 3, emptyText = 'Belum ada aktivitas yang tercatat.') {
    const rows = (items || []).slice(0, limit);
    if (!rows.length) return `<div class="engine-activity-empty">${_autoEscapeHtml(emptyText)}</div>`;
    return `<ul class="engine-activity-list">${rows.map((item) => `<li title="${_autoEscapeHtml(item)}">${_autoEscapeHtml(item)}</li>`).join('')}</ul>`;
}

function _renderCard(def, state) {
    const active = def.type === 'manual' ? false : _autoBool(_autoSettingValue(state.settingsMap, def.statusKey, '0'));
    const actions = [
        def.type === 'manual'
            ? `<button class="btn-primary btn-sm" onclick="${def.primaryButton?.action || "tampilTab('scraper')"}">${def.primaryButton?.label || '🧲 Buka Tab Scraper'}</button>`
            : `<button class="btn-outline btn-sm" onclick="toggleAutomationEngine('${def.id}')">⚡ ${active ? 'Matikan' : 'Aktifkan'}</button>`,
        `<button class="btn-outline btn-sm" onclick="openAutomationSettingsModal('card','${def.id}')" title="Buka pengaturan">⚙️ Atur</button>`,
        ...((def.extraActions || []).map((btn) => `<button class="btn-outline btn-sm" onclick="${btn.onclick}" title="${_autoEscapeHtml(btn.label)}">${btn.label}</button>`))
    ];
    const actionCols = Math.max(actions.length, 1);
    return `
        <div class="engine-card automation-card ${active ? 'active' : ''}" id="automation-card-${def.id}">
            <div class="engine-card-header">
                <div class="engine-card-icon">${def.icon}</div>
                <div class="engine-card-title">
                    <div class="engine-name">${_autoEscapeHtml(def.title)}</div>
                    <div class="engine-interval">${def.type === 'manual' ? 'Tahap awal · dijalankan manual' : 'Tahap otomatis'}</div>
                </div>
                ${_engineStatusBadge(active, def.type === 'manual')}
            </div>

            <div class="automation-flow-box compact">
                <div class="automation-flow-row">
                    <span class="automation-flow-label">Masuk jika</span>
                    <span class="automation-flow-value">${_autoEscapeHtml(def.masuk)}</span>
                </div>
                <div class="automation-flow-row">
                    <span class="automation-flow-label">Keluar jika</span>
                    <span class="automation-flow-value">${_autoEscapeHtml(def.keluar)}</span>
                </div>
            </div>

            ${_renderStats(state.stats[def.id] || [])}

            <div class="automation-mini-activity">
                <div class="automation-mini-title">Aktivitas terbaru</div>
                ${_renderActivityList(state.activity[def.id] || [], 2)}
            </div>

            <div class="engine-actions automation-card-actions" style="grid-template-columns: repeat(${actionCols}, minmax(0, 1fr));">
                ${actions.join('')}
            </div>
        </div>
    `;
}

function _renderMasterPanel(payload) {
    const panel = document.getElementById('automation-master-panel');
    if (!panel) return;
    const checklistHtml = `
        <div style="margin-top:14px;padding:10px 14px;background:#fffbeb;border-radius:8px;font-size:12px;line-height:2" id="automation-checklist">
            <strong style="color:#92400e">📋 Syarat otomasi penuh:</strong><br>
            <span id="chk-akun">${payload.accountsOnline > 0 ? `✅ ${payload.accountsOnline} akun Telegram online` : '❌ Belum ada akun Telegram online'}</span><br>
            <span id="chk-draft">${payload.hasDraft ? `✅ Draft aktif tersedia: \"${_autoEscapeHtml(payload.activeDraftTitle)}\"` : '❌ Belum ada draft aktif'}</span><br>
            <span id="chk-scraper">${(payload.jobs || []).length > 0 ? '✅ Scraper sudah pernah dijalankan' : '⬜ Scraper belum pernah dijalankan'}</span>
        </div>
    `;

    panel.innerHTML = `
        <div class="automation-master-shell">
            <div>
                <div style="font-weight:700;font-size:15px;margin-bottom:4px">⚡ Mode Otomasi Penuh</div>
                <div style="font-size:12px;color:#64748b;max-width:720px">
                    Aktifkan mode ini bila Anda ingin alur otomatis berjalan dari hasil scraper sampai recovery. Tekan tombol pengaturan untuk memahami cara kerja dan dampak tiap pengaturan dengan bahasa yang lebih mudah.
                </div>
            </div>
            <div class="automation-master-actions">
                <button id="btn-aktifkan-semua" class="btn-success" style="padding:10px 18px;font-weight:700" onclick="toggleSemuaOtomasi(true)">▶️ Aktifkan Semua</button>
                <button id="btn-matikan-semua" class="btn-danger" style="padding:10px 18px;font-weight:700" onclick="toggleSemuaOtomasi(false)">⏹️ Matikan Semua</button>
                <button class="btn-outline btn-sm" onclick="openAutomationSettingsModal('master')">⚙️ Pengaturan Mode Penuh</button>
                <button class="btn-outline btn-sm" onclick="openAutomationSettingsModal('score','akun')">🏷️ Penilaian Akun</button>
                <button class="btn-outline btn-sm" onclick="openAutomationSettingsModal('score','grup')">🏷️ Penilaian Grup</button>
                <button class="btn-outline btn-sm" onclick="muatStatusOtomasi()" title="Refresh status">🔄 Refresh</button>
            </div>
        </div>
        ${checklistHtml}
    `;
}

function _buildStats(payload) {
    const jobs = payload.jobs || [];
    const permissions = payload.permissions || {};
    const assignments = payload.assignments || {};
    const orchestrator = payload.orchestrator || {};
    const recovery = payload.recovery || {};
    const groupState = orchestrator.group_state_counts || {};
    const joinStats = orchestrator.auto_join_stats || {};
    const history = payload.history || [];

    const jobDone = jobs.filter((job) => ['done', 'completed', 'selesai'].includes(String(job.status || '').toLowerCase())).length;
    const jobRunning = jobs.filter((job) => ['running', 'queued', 'paused', 'berjalan'].includes(String(job.status || '').toLowerCase())).length;
    const jobFailed = jobs.filter((job) => ['failed', 'error', 'stopped', 'gagal'].includes(String(job.status || '').toLowerCase())).length;
    const totalImported = jobs.reduce((acc, job) => acc + Number(job.total_imported || 0), 0);
    const pendingJobs = jobs.filter((job) => Number(job.total_saved || 0) > Number(job.total_imported || 0)).length;
    const pendingImportResults = jobs.reduce((acc, job) => acc + Math.max(0, Number(job.total_saved || 0) - Number(job.total_imported || 0)), 0);
    const totalFoundResults = jobs.reduce((acc, job) => acc + Number(job.total_saved || 0), 0);

    const joinSuccessToday = history.filter((row) => ['join_success', 'join'].includes(String(row.status || '').toLowerCase()) && _sameLocalDayFromText(row.waktu)).length;
    // join_failed: hanya gagal umum (bukan invalid/floodwait) agar tidak misleading
    const joinFailedToday = history.filter((row) => String(row.status || '').toLowerCase() === 'join_failed' && _sameLocalDayFromText(row.waktu)).length;
    const joinInvalidToday = history.filter((row) => String(row.status || '').toLowerCase() === 'join_invalid_target' && _sameLocalDayFromText(row.waktu)).length;
    const joinFloodwaitToday = history.filter((row) => String(row.status || '').toLowerCase() === 'join_floodwait' && _sameLocalDayFromText(row.waktu)).length;
    const sendSuccessToday = history.filter((row) => ['send_success', 'berhasil'].includes(String(row.status || '').toLowerCase()) && _sameLocalDayFromText(row.waktu)).length;
    const sendFailedToday = history.filter((row) => ['send_failed', 'gagal'].includes(String(row.status || '').toLowerCase()) && _sameLocalDayFromText(row.waktu)).length;
    const sendBlockedToday = history.filter((row) => String(row.status || '').toLowerCase() === 'send_blocked' && _sameLocalDayFromText(row.waktu)).length;
    const cooldownStartedToday = history.filter((row) => ['cooldown_started'].includes(String(row.status || '').toLowerCase()) && _sameLocalDayFromText(row.waktu)).length;
    const groupsCoolingDown = Math.max(Number(groupState.cooldown || 0), 0);
    // Gabungkan join gagal: floodwait + invalid + umum
    const joinMasalahToday = joinFailedToday + joinFloodwaitToday;

    return {
        scraper: [
            { label: 'job total', value: jobs.length, color: '' },
            { label: 'selesai', value: jobDone, color: 'green' },
            { label: 'hasil ditemukan', value: totalFoundResults, color: 'amber' },
            { label: 'siap diperiksa', value: pendingImportResults, color: jobFailed > 0 ? 'red' : 'green' },
        ],
        import: [
            { label: 'hasil menunggu impor', value: pendingImportResults, color: 'amber' },
            { label: 'job menunggu', value: pendingJobs, color: 'amber' },
            { label: 'sudah masuk', value: totalImported, color: 'green' },
        ],
        permission: [
            { label: 'izin aktif', value: permissions.valid_count || 0, color: 'green' },
            { label: 'menunggu izin', value: (permissions.pending_count || 0) + (permissions.missing_permission_count || 0), color: 'amber' },
            { label: 'kedaluwarsa', value: permissions.expired_count || 0, color: 'red' },
        ],
        assign: [
            { label: 'sudah punya owner', value: (assignments.assigned_count || 0) + (assignments.managed_count || 0), color: 'green' },
            { label: 'siap dibagi', value: assignments.unassigned_count || 0, color: 'amber' },
            { label: 'menunggu join', value: joinStats.waiting || 0, color: 'amber' },
            { label: 'gagal assign', value: assignments.failed_count || 0, color: 'red' },
        ],
        autojoin: (() => {
            // Prioritas 1: pakai data dari orchestrator._STATE (sudah dihitung akurat di backend)
            const joinedToday  = joinStats.joined_today  != null ? Number(joinStats.joined_today)  : joinSuccessToday;
            const failedToday  = joinStats.failed_today  != null ? Number(joinStats.failed_today)  : joinMasalahToday;
            const waiting      = Number(joinStats.waiting || 0);
            return [
                { label: 'berhasil join hari ini', value: joinedToday,  color: 'green',  tooltip: 'Jumlah grup yang berhasil di-join hari ini' },
                { label: 'menunggu join',           value: waiting,      color: 'amber',  tooltip: 'Grup sudah di-assign owner tapi owner belum join' },
                { label: 'gagal/floodwait',         value: failedToday,  color: failedToday > 20 ? 'red' : 'amber', tooltip: 'Total join gagal dan kena FloodWait hari ini' },
            ];
        })(),
        campaign: (() => {
            // Prioritas 1: pakai ringkasan dari backend (sudah query DB langsung, akurat)
            const ringkasan = payload.ringkasan || {};
            const terkirim  = ringkasan.berhasil  != null ? Number(ringkasan.berhasil)  : sendSuccessToday;
            const cooldown  = ringkasan.cooldown_started != null ? Number(ringkasan.cooldown_started) : (cooldownStartedToday || groupsCoolingDown);
            const gagal     = ringkasan.gagal     != null ? Number(ringkasan.gagal)     : (sendFailedToday + sendBlockedToday);
            return [
                { label: 'terkirim hari ini',  value: terkirim, color: 'green', tooltip: 'Jumlah pesan yang berhasil dikirim hari ini' },
                { label: 'sedang istirahat',   value: cooldown,  color: 'amber', tooltip: 'Grup yang sudah dikirim dan sedang dalam masa cooldown' },
                { label: 'gagal/blocked',      value: gagal,     color: gagal > terkirim ? 'red' : 'amber', tooltip: 'Total gagal kirim dan diblokir hari ini' },
            ];
        })(),
        recovery: [
            { label: 'perlu recovery', value: recovery.recovery_needed_count || 0, color: 'red' },
            { label: 'assign macet', value: recovery.stuck_assignments_count || 0, color: 'amber' },
            { label: 'job macet', value: recovery.stuck_jobs_count || 0, color: 'amber' },
        ],
    };
}

function _buildActivity(payload) {
    const jobs = payload.jobs || [];
    const auditItems = payload.auditItems || [];
    const history = payload.history || [];
    const permissionItems = payload.permissionItems || [];
    const byAction = (actions) => auditItems
        .filter((item) => actions.includes(item.action))
        .slice(0, 5)
        .map((item) => `${_formatTimeText(item.timestamp)} — ${item.message}`);

    const scraperActivity = jobs.slice(0, 5).map((job) => {
        const status = String(job.status || '').toLowerCase();
        const saved = Number(job.total_saved || 0);
        const imported = Number(job.total_imported || 0);
        const remaining = Math.max(0, saved - imported);
        let info = '';
        if (['done', 'completed', 'selesai'].includes(status)) {
            info = `${saved} hasil`;
            if (remaining > 0) info += ` · ${remaining} siap diperiksa`;
            else if (imported > 0) info += ` · ${imported} sudah dipindahkan`;
            else info += ' · belum ada hasil yang dipindahkan';
        } else if (['running', 'queued', 'paused', 'berjalan'].includes(status)) {
            const processed = Number(job.processed_keywords || 0);
            const total = Number(job.total_keywords || 0);
            info = `${job.status || 'berjalan'} · ${processed}/${total || processed} kata kunci`;
        } else if (['failed', 'error', 'stopped', 'gagal'].includes(status)) {
            info = `${job.status || 'gagal'}${job.last_error ? ` · ${job.last_error}` : ''}`;
        } else {
            info = `${job.status || 'unknown'} · ${saved} hasil`;
        }
        return `${_formatTimeText(job.dibuat || job.started_at || job.finished_at)} — ${job.job_name || job.keyword_preview || 'Job scraper'}: ${info}`;
    });

    const assignmentItems = payload.assignmentItems || [];
    const assignActivity = assignmentItems
        .slice(0, 5)
        .map((row) => {
            const status = String(row.status || '').toLowerCase();
            const owner = row.owner_name || row.assigned_account_id || 'Tanpa owner';
            let label = '❓ status tidak dikenal';
            if (status === 'managed') label = '✅ siap broadcast';
            else if (status === 'assigned') label = '⏳ menunggu join';
            else if (status === 'failed') label = '❌ gagal assign';
            else if (status === 'retry_wait') label = '🔄 menunggu percobaan ulang';
            else if (status === 'reassign_pending') label = '🔀 menunggu pindah owner';
            else if (status === 'released') label = '🔓 owner dilepas';
            return `${_formatTimeText(row.updated_at || row.created_at)} — ${row.group_name || row.username || 'Grup'} → ${owner}: ${label}`;
        });

    const joinHistory = history
        .filter((row) => ['join_success', 'join', 'join_failed', 'join_invalid_target', 'join_floodwait', 'join_blacklisted'].includes(String(row.status || '').toLowerCase()))
        .slice(0, 5)
        .map((row) => {
            const status = String(row.status || '').toLowerCase();
            let label = 'berhasil join';
            if (status === 'join_failed') label = '❌ gagal join';
            else if (status === 'join_invalid_target') label = '🚫 username tidak valid';
            else if (status === 'join_floodwait') label = '⏳ flood wait';
            else if (status === 'join_blacklisted') label = '⛔ diblacklist';
            else if (['join_success', 'join'].includes(status)) label = '✅ berhasil join';
            const extra = row.pesan_error ? ` · ${String(row.pesan_error).slice(0, 50)}` : '';
            return `${_formatTimeText(row.waktu)} — ${row.nama_grup || row.grup_id || 'Grup'}: ${label}${extra}`;
        });

    const deliveryHistory = history
        .filter((row) => ['send_success', 'send_failed', 'send_blocked', 'cooldown_started', 'berhasil', 'gagal'].includes(String(row.status || '').toLowerCase()))
        .slice(0, 5)
        .map((row) => {
            const status = String(row.status || '').toLowerCase();
            let label = '✅ pesan terkirim';
            if (['send_failed', 'gagal'].includes(status)) label = '❌ gagal kirim';
            else if (status === 'send_blocked') label = '⛔ diblokir';
            else if (status === 'cooldown_started') label = '⏸️ mulai istirahat';
            const extra = row.pesan_error ? ` · ${String(row.pesan_error).slice(0, 50)}` : '';
            return `${_formatTimeText(row.waktu)} — ${row.nama_grup || row.grup_id || 'Grup'}: ${label}${extra}`;
        });

    const importFallback = jobs
        .filter((job) => Number(job.total_saved || 0) > 0 || Number(job.total_imported || 0) > 0)
        .slice(0, 5)
        .map((job) => {
            const saved = Number(job.total_saved || 0);
            const imported = Number(job.total_imported || 0);
            const remaining = Math.max(0, saved - imported);
            let info = `${imported} masuk`;
            if (remaining > 0) info += `, ${remaining} menunggu`;
            return `${_formatTimeText(job.finished_at || job.started_at || job.dibuat)} — ${job.job_name || job.keyword_preview || 'Job scraper'}: ${info}`;
        });

    const permissionActivity = permissionItems
        .slice(0, 5)
        .map((row) => {
            const status = String(row.status || '').toLowerCase();
            const label = status === 'valid' ? 'izin aktif' : status === 'pending' ? 'menunggu izin' : status === 'expired' ? 'izin kedaluwarsa' : status === 'revoked' ? 'izin dicabut' : `status ${status || 'unknown'}`;
            return `${_formatTimeText(row.approved_at || row.created_at || row.updated_at)} — ${row.group_name || row.username || 'Grup'}: ${label}`;
        });
    const permissionSummaryFallback = [];
    if (!permissionActivity.length) {
        if (Number(payload.permissions?.valid_count || 0) > 0) permissionSummaryFallback.push(`Saat ini ${payload.permissions.valid_count} grup memiliki izin aktif.`);
        if (Number((payload.permissions?.pending_count || 0) + (payload.permissions?.missing_permission_count || 0)) > 0) permissionSummaryFallback.push(`Ada ${Number(payload.permissions.pending_count || 0) + Number(payload.permissions.missing_permission_count || 0)} grup yang masih menunggu izin.`);
        if (Number(payload.permissions?.expired_count || 0) > 0) permissionSummaryFallback.push(`Ada ${payload.permissions.expired_count} izin yang sudah kedaluwarsa.`);
    }

    return {
        scraper: scraperActivity,
        import: byAction(['stage_import']).length ? byAction(['stage_import']) : importFallback,
        permission: byAction(['stage_permission']).length ? byAction(['stage_permission']) : (permissionActivity.length ? permissionActivity : permissionSummaryFallback),
        assign: assignActivity.length ? assignActivity : byAction(['stage_assignment']),
        autojoin: joinHistory.length ? joinHistory : byAction(['stage_auto_join', 'stage_sync_join']),
        campaign: deliveryHistory.length ? deliveryHistory : byAction(['stage_delivery', 'stage_campaign_prepare']),
        recovery: (() => {
            const recoveryItems = byAction(['recovery_execute', 'recovery_scan', 'recovery_item_recovered', 'recovery_item_failed']);
            if (recoveryItems.length) return recoveryItems;
            // Fallback: tampilkan ringkasan dari data recovery
            const lines = [];
            const recov = payload.recovery || {};
            if (recov.recovered_count > 0) lines.push(`✅ ${recov.recovered_count} item berhasil dipulihkan`);
            if (recov.stuck_assignments_count > 0) lines.push(`⚠️ ${recov.stuck_assignments_count} assignment macet menunggu perbaikan`);
            if (recov.stuck_jobs_count > 0) lines.push(`⚠️ ${recov.stuck_jobs_count} job macet terdeteksi`);
            if (recov.recovery_needed_count > 0) lines.push(`🔍 ${recov.recovery_needed_count} item perlu ditinjau`);
            if (!lines.length) lines.push('✅ Tidak ada item yang memerlukan recovery saat ini');
            return lines;
        })(),
    };
}

function _renderAutomationCards(state) {
    const grid = document.getElementById('automation-engine-grid');
    if (!grid) return;
    grid.innerHTML = Object.values(AUTOMATION_CARD_DEFS).map((def) => _renderCard(def, state)).join('');
    _renderThrottlePanel();
}

async function _renderThrottlePanel() {
    let panel = document.getElementById('automation-throttle-panel');
    if (!panel) return;
    try {
        const [akun, throttle] = await Promise.all([
            fetch('/api/akun').then(r => r.json()),
            fetch('/api/v2/broadcast-queue/throttle-status').then(r => r.json()),
        ]);
        const now = new Date();
        const fs = (d) => { if(!d) return null; const dt=new Date(d.replace(' ','T')),ms=dt-now; if(ms<=0) return 'sekarang'; const m=Math.floor(ms/60000),j=Math.floor(m/60); return j>0?j+'j '+(m%60)+'m lagi':m+'m lagi'; };
        const fw = (d) => { try{return new Date(d.replace(' ','T')).toLocaleTimeString('id-ID',{hour:'2-digit',minute:'2-digit'});}catch{return '-';} };
        function cellJadwal(a) {
            const parts=[],now2=new Date();
            const lastB=a.last_broadcast_at,nextB=a.next_broadcast_at,grupB=a.last_broadcast_group;
            const nextBOk=!nextB||new Date(nextB.replace(' ','T'))<=now2;
            if(lastB) parts.push('<div style="margin-bottom:3px"><span style="font-size:10px;background:#eff6ff;color:#2563eb;padding:1px 5px;border-radius:4px;font-weight:600">📤 Kirim</span> terakhir <strong>'+fw(lastB)+'</strong>'+(grupB?' <span style="color:#64748b;font-size:10px">→ '+grupB+'</span>':'')+'</div>');
            if(nextB&&!nextBOk) parts.push('<div style="margin-bottom:3px"><span style="font-size:10px;background:#fef3c7;color:#92400e;padding:1px 5px;border-radius:4px;font-weight:600">⏳ Next kirim</span> <strong>'+fw(nextB)+'</strong> <em style="color:#aaa;font-size:10px">('+fs(nextB)+')</em></div>');
            else parts.push('<div style="margin-bottom:3px;color:#16a34a;font-size:11px"><span style="font-size:10px;background:#f0fdf4;color:#16a34a;padding:1px 5px;border-radius:4px">📤 Kirim</span> 🟢 Siap kirim</div>');
            const lastJ=a.last_join_at,nextJ=a.next_join_at,grupJ=a.last_join_group;
            const nextJOk=!nextJ||new Date(nextJ.replace(' ','T'))<=now2;
            if(lastJ) parts.push('<div style="margin-bottom:3px"><span style="font-size:10px;background:#f0fdf4;color:#16a34a;padding:1px 5px;border-radius:4px;font-weight:600">🔗 Join</span> terakhir <strong>'+fw(lastJ)+'</strong>'+(grupJ?' <span style="color:#64748b;font-size:10px">→ '+grupJ+'</span>':'')+'</div>');
            if(nextJ&&!nextJOk) parts.push('<div><span style="font-size:10px;background:#fef3c7;color:#92400e;padding:1px 5px;border-radius:4px;font-weight:600">⏳ Next join</span> <strong>'+fw(nextJ)+'</strong> <em style="color:#aaa;font-size:10px">('+fs(nextJ)+')</em></div>');
            else parts.push('<div style="color:#16a34a;font-size:11px"><span style="font-size:10px;background:#f0fdf4;color:#16a34a;padding:1px 5px;border-radius:4px">🔗 Join</span> 🟢 Siap join</div>');
            return parts.join('');
        }
        const rows=akun.map(a=>{
            const nextJOk=!a.next_join_at||new Date((a.next_join_at||'').replace(' ','T'))<=now;
            const nextBOk=!a.next_broadcast_at||new Date((a.next_broadcast_at||'').replace(' ','T'))<=now;
            const ok=nextJOk&&nextBOk;
            const btn=!ok?'<button class="btn-outline btn-sm" onclick="resetJoinThrottle(\''+a.phone+'\')">⚡ Reset</button>':'<span style="color:#aaa;font-size:11px">-</span>';
            const sbg=a.status==='banned'?'#fee2e2':a.status==='active'?'#f0fdf4':'#f1f5f9';
            const sc=a.status==='banned'?'#991b1b':a.status==='active'?'#166534':'#64748b';
            return '<tr style="border-bottom:1px solid #f1f5f9"><td style="padding:8px 6px;font-weight:600">'+(a.nama||'-')+'</td><td style="padding:8px 6px;color:#64748b;font-size:11px">'+a.phone+'</td><td style="padding:8px 6px"><span style="font-size:11px;padding:2px 7px;border-radius:10px;background:'+sbg+';color:'+sc+'">'+a.status+'</span></td><td style="padding:8px 6px;line-height:1.8">'+cellJadwal(a)+'</td><td style="padding:8px 6px">'+btn+'</td></tr>';
        }).join('');
        const bd=throttle.data||{},bok=!bd.next_allowed_at||new Date(bd.next_allowed_at)<=now;
        const bst=bok?'<div style="color:#16a34a;font-size:11px">🟢 Siap kirim</div>':'<div><span style="font-size:10px;background:#fef3c7;color:#92400e;padding:1px 5px;border-radius:4px;font-weight:600">⏳ Next kirim</span> <strong>'+fw(bd.next_allowed_at)+'</strong> <em style="color:#aaa;font-size:10px">('+fs(bd.next_allowed_at)+')</em></div>';
        const bbtn=!bok?'<button class="btn-outline btn-sm" onclick="resetBroadcastThrottle()">⚡ Reset</button>':'<span style="color:#aaa;font-size:11px">-</span>';
        const rbroad='<tr style="border-top:2px solid #e2e8f0;background:#f8fafc"><td style="padding:8px 6px;font-weight:600">Auto Broadcast</td><td style="padding:8px 6px;color:#64748b;font-size:11px">semua akun</td><td style="padding:8px 6px"><span style="font-size:11px;padding:2px 7px;border-radius:10px;background:#eff6ff;color:#1d4ed8">global</span></td><td style="padding:8px 6px">'+bst+'</td><td style="padding:8px 6px">'+bbtn+'</td></tr>';
        panel.innerHTML='<div class="engine-card" style="max-width:100%;overflow:auto"><div class="engine-card-header"><div class="engine-card-icon">⏱️</div><div class="engine-card-title"><div class="engine-name">Status Throttle</div><div class="engine-interval">Jadwal join & broadcast berikutnya — klik Reset untuk paksa jalan sekarang</div></div></div><table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:8px"><thead><tr style="background:#f8fafc;color:#64748b;font-size:11px;border-bottom:1px solid #e2e8f0"><th style="text-align:left;padding:6px">Nama</th><th style="text-align:left;padding:6px">Nomor</th><th style="text-align:left;padding:6px">Status</th><th style="text-align:left;padding:6px">Jadwal Berikutnya</th><th style="text-align:left;padding:6px">Aksi</th></tr></thead><tbody>'+rows+rbroad+'</tbody></table></div>';
    } catch(e) { panel.innerHTML='<div style="color:#888;font-size:12px;padding:8px">Gagal: '+e.message+'</div>'; }
}

function _ensureAutomationModal() {
    let modal = document.getElementById('automation-settings-modal');
    if (modal) return modal;
    modal = document.createElement('div');
    modal.id = 'automation-settings-modal';
    modal.className = 'automation-modal';
    modal.innerHTML = `
        <div class="automation-modal-backdrop" onclick="closeAutomationSettingsModal()"></div>
        <div class="automation-modal-dialog" role="dialog" aria-modal="true" aria-labelledby="automation-modal-title">
            <div class="automation-modal-header">
                <div>
                    <div class="automation-modal-title" id="automation-modal-title">Pengaturan Otomasi</div>
                    <div class="automation-modal-subtitle" id="automation-modal-subtitle">Memuat...</div>
                </div>
                <button type="button" class="automation-modal-close" onclick="closeAutomationSettingsModal()">✕</button>
            </div>
            <div class="automation-modal-body" id="automation-modal-body"></div>
            <div class="automation-modal-footer" id="automation-modal-footer"></div>
        </div>
    `;
    document.body.appendChild(modal);
    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') closeAutomationSettingsModal();
    });
    return modal;
}

function _renderModalSettingControl(item, settingsMap) {
    const value = _autoSettingValue(settingsMap, item.key, item.fallback);
    if (item.kind === 'boolean') {
        return `
            <label class="automation-setting-switch">
                <input type="checkbox" data-modal-setting-key="${item.key}" ${_autoBool(value, _autoBool(item.fallback, false)) ? 'checked' : ''}>
                <span class="automation-setting-switch-ui"></span>
                <span class="automation-setting-switch-text">${_autoBool(value, _autoBool(item.fallback, false)) ? 'Aktif' : 'Nonaktif'}</span>
            </label>
        `;
    }
    return `
        <input
            class="engine-setting-input automation-modal-input"
            type="${item.kind === 'number' ? 'number' : 'text'}"
            step="${item.kind === 'number' ? '1' : 'any'}"
            data-modal-setting-key="${item.key}"
            value="${_autoEscapeHtml(value ?? '')}"
        >
    `;
}

function _renderMetricsSection(def, state) {
    const stats = state?.stats?.[def.id] || [];
    const helpMap = def.statsHelp || {};
    if (!stats.length) return '';
    return `
        <div class="automation-modal-section">
            <div class="automation-modal-section-title">Arti angka di card</div>
            <div class="automation-metric-help-list">
                ${stats.map((item) => `
                    <div class="automation-metric-help-item">
                        <div class="automation-metric-help-head">
                            <span class="automation-metric-value">${_autoSafeText(item.value, '0')}</span>
                            <span class="automation-metric-label">${_autoEscapeHtml(item.label)}</span>
                        </div>
                        <div class="automation-metric-help-copy">${_autoEscapeHtml(helpMap[item.label.toLowerCase()] || helpMap[item.label] || 'Indikator ini membantu Anda melihat kondisi card saat ini.')}</div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;
}

function _renderSettingsSection(def, state) {
    if (!def.settings?.length) return '';
    return `
        <div class="automation-modal-section">
            <div class="automation-modal-section-title">Pengaturan yang berpengaruh</div>
            <div class="automation-modal-section-desc">Semua pengaturan di bawah ini benar-benar berpengaruh pada cara card bekerja. Penjelasannya dibuat sesederhana mungkin agar mudah dipahami.</div>
            <div class="automation-setting-list modal">
                ${def.settings.map((item) => `
                    <div class="automation-setting-card">
                        <div class="automation-setting-copy">
                            <div class="automation-setting-name">${_autoEscapeHtml(item.label)}</div>
                            <div class="automation-setting-note">${_autoEscapeHtml(item.note)}</div>
                            <div class="automation-setting-effect">Dampak: ${_autoEscapeHtml(item.effect)}</div>
                        </div>
                        <div class="automation-setting-control">${_renderModalSettingControl(item, state.settingsMap)}</div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;
}

function _renderModalBody(kind, id) {
    const state = AUTOMATION_LAST_STATE;
    const def = kind === 'master' ? AUTOMATION_MASTER_DEF : (kind === 'score' ? AUTOMATION_SCORE_DEFS[id] : AUTOMATION_CARD_DEFS[id]);
    if (!def || !state) return { title: 'Pengaturan Otomasi', subtitle: 'Data belum siap', body: '<div class="automation-empty-box">Data otomasi belum siap dimuat.</div>', footer: '' };

    const active = def.type === 'manual'
        ? null
        : _autoBool(_autoSettingValue(state.settingsMap, def.statusKey, '0'));

    const title = `${def.icon} ${def.title}`;
    const subtitle = kind === 'master'
        ? 'Pengaturan dasar yang mempengaruhi jalannya alur otomasi penuh.'
        : kind === 'score'
            ? 'Menjelaskan cara sistem memberi nilai agar keputusan card lebih mudah dipahami.'
            : (def.type === 'manual' ? 'Tahap manual' : `Status saat ini: ${active ? 'Aktif' : 'Nonaktif'}`);

    const body = `
        <div class="automation-modal-summary-box">
            <div class="automation-modal-summary-title">Fungsi</div>
            <div class="automation-modal-summary-copy">${_autoEscapeHtml(def.summary || def.fungsi)}</div>
        </div>

        ${kind === 'card' ? `
            <div class="automation-modal-section">
                <div class="automation-modal-section-title">Alur card ini</div>
                <div class="automation-flow-box modal">
                    <div class="automation-flow-row">
                        <span class="automation-flow-label">Masuk jika</span>
                        <span class="automation-flow-value">${_autoEscapeHtml(def.masuk)}</span>
                    </div>
                    <div class="automation-flow-row">
                        <span class="automation-flow-label">Keluar jika</span>
                        <span class="automation-flow-value">${_autoEscapeHtml(def.keluar)}</span>
                    </div>
                </div>
            </div>
        ` : ''}

        <div class="automation-modal-section">
            <div class="automation-modal-section-title">Cara kerja</div>
            <ol class="automation-help-list">
                ${(def.caraKerja || []).map((item) => `<li>${_autoEscapeHtml(item)}</li>`).join('')}
            </ol>
        </div>

        ${kind === 'card' ? _renderMetricsSection(def, state) : ''}
        ${_renderSettingsSection(def, state)}

        ${kind === 'card' ? `
            <div class="automation-modal-section">
                <div class="automation-modal-section-title">Aktivitas terbaru</div>
                ${_renderActivityList(state.activity[def.id] || [], 5)}
            </div>
        ` : ''}
    `;

    const footerButtons = [];
    if (kind === 'master') {
        footerButtons.push(`<button class="btn-success" onclick="toggleSemuaOtomasi(true)">▶️ Aktifkan Semua</button>`);
        footerButtons.push(`<button class="btn-danger" onclick="toggleSemuaOtomasi(false)">⏹️ Matikan Semua</button>`);
    } else if (kind === 'score') {
        footerButtons.push(`<button class="btn-primary" onclick="saveAutomationModalSettings()">💾 Simpan Pengaturan</button>`);
    } else if (def.type === 'manual') {
        footerButtons.push(`<button class="btn-primary" onclick="closeAutomationSettingsModal(); ${def.primaryButton?.action || "tampilTab('scraper')"}">${def.primaryButton?.label || '🧲 Buka Tab Scraper'}</button>`);
    } else {
        footerButtons.push(`<button class="btn-outline" onclick="toggleAutomationEngine('${def.id}')">⚡ ${active ? 'Matikan Card' : 'Aktifkan Card'}</button>`);
    }
    if (kind !== 'score' && def.settings?.length) {
        footerButtons.push(`<button class="btn-primary" onclick="saveAutomationModalSettings()">💾 Simpan Pengaturan</button>`);
    }
    footerButtons.push(`<button class="btn-outline" onclick="closeAutomationSettingsModal()">Tutup</button>`);

    return { title, subtitle, body, footer: footerButtons.join('') };
}

function openAutomationSettingsModal(kind, id = '') {
    _ensureAutomationModal();
    AUTOMATION_MODAL_STATE.kind = kind;
    AUTOMATION_MODAL_STATE.id = id || '';
    const modal = document.getElementById('automation-settings-modal');
    const { title, subtitle, body, footer } = _renderModalBody(kind, id);
    document.getElementById('automation-modal-title').textContent = title;
    document.getElementById('automation-modal-subtitle').textContent = subtitle;
    document.getElementById('automation-modal-body').innerHTML = body;
    document.getElementById('automation-modal-footer').innerHTML = footer;
    modal.classList.add('open');

    modal.querySelectorAll('input[type="checkbox"][data-modal-setting-key]').forEach((input) => {
        const text = input.closest('.automation-setting-switch')?.querySelector('.automation-setting-switch-text');
        const sync = () => { if (text) text.textContent = input.checked ? 'Aktif' : 'Nonaktif'; };
        input.addEventListener('change', sync);
        sync();
    });
}

function closeAutomationSettingsModal() {
    const modal = document.getElementById('automation-settings-modal');
    if (modal) modal.classList.remove('open');
    AUTOMATION_MODAL_STATE.kind = null;
    AUTOMATION_MODAL_STATE.id = '';
}


function _normalizeAutomationPayload(payload) {
    const normalized = { ...(payload || {}) };
    const intMinZero = [
        'scraper_limit_per_keyword','scraper_min_members','scraper_recommended_score','scraper_max_terms',
        'result_min_quality_score','permission_min_score','assignment_min_health_score','assignment_min_warming_level',
        'assignment_retry_count','assignment_reassign_count','auto_join_max_per_cycle','auto_join_reserve_quota',
        'assignment_broadcast_delay_minutes','broadcast_jeda_min','broadcast_jeda_max','campaign_group_cooldown_hours',
        'broadcast_jeda_kirim_min_detik','broadcast_jeda_kirim_max_detik','broadcast_masa_tunggu_setelah_assign_menit','broadcast_cooldown_grup_menit','broadcast_cooldown_grup_jam','broadcast_batas_grup_sepi_hari','broadcast_retry_delay_detik','broadcast_target_per_sesi','broadcast_target_per_akun_per_sesi','broadcast_batch_delivery',
        'pipeline_interval_detik','pipeline_batch_import','pipeline_batch_permission','pipeline_batch_assign','pipeline_batch_campaign','pipeline_batch_delivery','pipeline_batch_recovery','pipeline_retry_maks_per_item','pipeline_retry_jeda_detik',
        'campaign_inactive_threshold_days','campaign_retry_delay_minutes','orchestrator_interval_seconds',
        'orchestrator_import_batch','orchestrator_permission_batch','orchestrator_assign_batch','orchestrator_campaign_batch',
        'orchestrator_delivery_batch','orchestrator_recovery_batch','recovery_stuck_scrape_threshold',
        'recovery_stuck_assignment_threshold','recovery_stuck_campaign_threshold'
    ];
    intMinZero.forEach((key) => {
        if (!(key in normalized)) return;
        const value = Number(normalized[key]);
        if (Number.isFinite(value)) normalized[key] = String(Math.max(0, Math.trunc(value)));
        else delete normalized[key];
    });

    Object.keys(normalized).forEach((key) => {
        if (!(key.startsWith('score_akun_') || key.startsWith('score_grup_'))) return;
        if (key.endsWith('_jadi_nol')) {
            normalized[key] = ['1','true','yes','on'].includes(String(normalized[key]).toLowerCase()) ? '1' : '0';
            return;
        }
        const value = Number(normalized[key]);
        if (Number.isFinite(value)) normalized[key] = String(Math.max(0, Math.trunc(value)));
    });

    ['scraper_delay_keyword_min', 'scraper_delay_keyword_max'].forEach((key) => {
        if (!(key in normalized)) return;
        const value = Number(normalized[key]);
        if (Number.isFinite(value)) normalized[key] = String(Math.max(0.2, value));
        else delete normalized[key];
    });

    if ('broadcast_jeda_min' in normalized || 'broadcast_jeda_max' in normalized) {
        const min = Number(normalized.broadcast_jeda_min ?? 15);
        const max = Number(normalized.broadcast_jeda_max ?? 40);
        if (Number.isFinite(min) && Number.isFinite(max)) {
            normalized.broadcast_jeda_min = String(Math.max(0, Math.trunc(min)));
            normalized.broadcast_jeda_max = String(Math.max(Math.trunc(min), Math.trunc(max)));
        }
    }

    if ('broadcast_jeda_kirim_min_detik' in normalized || 'broadcast_jeda_kirim_max_detik' in normalized) {
        const min = Number(normalized.broadcast_jeda_kirim_min_detik ?? 20);
        const max = Number(normalized.broadcast_jeda_kirim_max_detik ?? 45);
        if (Number.isFinite(min) && Number.isFinite(max)) {
            normalized.broadcast_jeda_kirim_min_detik = String(Math.max(0, Math.trunc(min)));
            normalized.broadcast_jeda_kirim_max_detik = String(Math.max(Math.trunc(min), Math.trunc(max)));
        }
    }

    if ('scraper_delay_keyword_min' in normalized || 'scraper_delay_keyword_max' in normalized) {
        const min = Number(normalized.scraper_delay_keyword_min ?? 1);
        const max = Number(normalized.scraper_delay_keyword_max ?? 3);
        if (Number.isFinite(min) && Number.isFinite(max)) {
            normalized.scraper_delay_keyword_min = String(Math.max(0.2, min));
            normalized.scraper_delay_keyword_max = String(Math.max(Math.max(0.2, min), max));
        }
    }

    if ('result_allowed_entity_types' in normalized) {
        const cleaned = String(normalized.result_allowed_entity_types || '')
            .split(',')
            .map((part) => part.trim().toLowerCase())
            .filter(Boolean)
            .join(',');
        normalized.result_allowed_entity_types = cleaned || 'group,supergroup';
    }
    return normalized;
}

async function saveAutomationModalSettings() {
    const kind = AUTOMATION_MODAL_STATE.kind;
    const id = AUTOMATION_MODAL_STATE.id;
    const def = kind === 'master' ? AUTOMATION_MASTER_DEF : (kind === 'score' ? AUTOMATION_SCORE_DEFS[id] : AUTOMATION_CARD_DEFS[id]);
    if (!def?.settings?.length) return;
    const payload = {};
    def.settings.forEach((item) => {
        const input = document.querySelector(`[data-modal-setting-key="${item.key}"]`);
        if (!input) return;
        payload[item.key] = item.kind === 'boolean' ? (input.checked ? '1' : '0') : input.value;
    });
    try {
        await _post('/settings', _normalizeAutomationPayload(payload));
        _showAutomationMessage(`Pengaturan ${def.title} berhasil disimpan.`, 'success');
        await muatStatusOtomasi();
        openAutomationSettingsModal(kind, id);
    } catch (e) {
        _showAutomationMessage(`Gagal menyimpan pengaturan ${def.title}: ${e.message}`, 'error');
    }
}

async function muatAutomation() {
    _hideLegacyAutomationSections();
    _ensureAutomationModal();
    await muatStatusOtomasi();
    _renderThrottlePanel();
}

async function muatStatusOtomasi() {
    _hideLegacyAutomationSections();
    const safe = (promise, fallback) => promise.then((r) => r ?? fallback).catch(() => fallback);
    const [settingsRows, status, jobs, groups, permissions, permissionList, assignments, assignmentList, orchestrator, campaigns, broadcastQueue, recovery, audit, history, ringkasan, draft, accounts] = await Promise.all([
        safe(_get('/settings'), []),
        safe(_get('/automation/status'), {}),
        safe(_get('/scraper/jobs?limit=20'), []),
        safe(_get('/grup'), []),
        safe(_autoGetV2('/permissions/summary'), {}),
        safe(_autoGetV2('/permissions?page_size=5'), { items: [] }),
        safe(_autoGetV2('/assignments/summary'), {}),
        safe(_autoGetV2('/assignments?page_size=5'), { items: [] }),
        safe(_autoGetV2('/orchestrator/status'), {}),
        safe(_autoGetV2('/campaigns/summary'), {}),
        safe(_autoGetV2('/broadcast-queue/summary'), {}),
        safe(_autoGetV2('/recovery/summary'), {}),
        safe(_autoGetV2('/logs?page_size=50'), { items: [] }),
        safe(_get('/riwayat'), []),
        safe(_get('/riwayat/ringkasan'), {}),
        safe(_get('/draft/aktif'), null),
        safe(_get('/akun'), []),
    ]);

    const settingsMap = _buildSettingsMap(settingsRows);
    if (status.auto_permission != null && !settingsMap.auto_permission_enabled) settingsMap.auto_permission_enabled = { value: status.auto_permission ? '1' : '0' };
    if (status.auto_import != null && !settingsMap.auto_import_enabled) settingsMap.auto_import_enabled = { value: status.auto_import ? '1' : '0' };
    if (status.auto_assign != null && !settingsMap.auto_assign_enabled) settingsMap.auto_assign_enabled = { value: status.auto_assign ? '1' : '0' };
    if (status.auto_join != null && !settingsMap.auto_join_enabled) settingsMap.auto_join_enabled = { value: status.auto_join ? '1' : '0' };
    if (status.auto_campaign != null && !settingsMap.auto_campaign_enabled) settingsMap.auto_campaign_enabled = { value: status.auto_campaign ? '1' : '0' };
    if (status.auto_recovery != null && !settingsMap.auto_recovery_enabled) settingsMap.auto_recovery_enabled = { value: status.auto_recovery ? '1' : '0' };

    const payload = {
        settingsMap,
        jobs,
        groups,
        permissions,
        permissionItems: permissionList.items || [],
        assignments,
        assignmentItems: assignmentList.items || [],
        orchestrator,
        campaigns,
        broadcastQueue,
        recovery,
        auditItems: audit.items || [],
        history,
        ringkasan,
        hasDraft: !!(draft && draft.isi),
        activeDraftTitle: draft?.judul || 'Tanpa judul',
        accountsOnline: (accounts || []).filter((a) => a.online).length,
    };

    const state = {
        settingsMap,
        stats: _buildStats(payload),
        activity: _buildActivity(payload),
    };

    AUTOMATION_LAST_PAYLOAD = payload;
    AUTOMATION_LAST_STATE = state;

    _renderMasterPanel(payload);
    _renderAutomationCards(state);

    if (AUTOMATION_MODAL_STATE.kind) {
        openAutomationSettingsModal(AUTOMATION_MODAL_STATE.kind, AUTOMATION_MODAL_STATE.id);
    }
}

async function toggleSemuaOtomasi(aktif) {
    try {
        await _post('/settings', {
            auto_import_enabled: aktif ? '1' : '0',
            auto_permission_enabled: aktif ? '1' : '0',
            auto_assign_enabled: aktif ? '1' : '0',
            auto_join_enabled: aktif ? '1' : '0',
            auto_campaign_enabled: aktif ? '1' : '0',
            auto_recovery_enabled: aktif ? '1' : '0',
            pause_all_automation: aktif ? '0' : '1',
        });
        _showAutomationMessage(aktif ? 'Semua card otomatis diaktifkan.' : 'Semua card otomatis dimatikan.', 'success');
        await muatStatusOtomasi();
    } catch (e) {
        _showAutomationMessage(`Gagal mengubah semua card: ${e.message}`, 'error');
    }
}

async function toggleAutomationEngine(cardId) {
    const def = AUTOMATION_CARD_DEFS[cardId];
    if (!def || !def.statusKey) return;
    const current = _autoBool(_autoSettingValue(AUTOMATION_LAST_STATE?.settingsMap || {}, def.statusKey, '0'));
    try {
        await _post('/settings', { [def.statusKey]: current ? '0' : '1' });
        _showAutomationMessage(`${def.title} ${current ? 'dimatikan' : 'diaktifkan'}.`, 'success');
        await muatStatusOtomasi();
    } catch (e) {
        _showAutomationMessage(`Gagal mengubah ${def.title}: ${e.message}`, 'error');
    }
}

// Legacy alias agar tombol lama tidak error
async function toggleMesin(mesin) {
    const map = { import: 'import', permission: 'permission', assign: 'assign', autojoin: 'autojoin', campaign: 'campaign', recovery: 'recovery' };
    return toggleAutomationEngine(map[mesin] || mesin);
}
async function toggleAutoJoin() { return toggleAutomationEngine('autojoin'); }

window.muatAutomation = muatAutomation;
window.muatStatusOtomasi = muatStatusOtomasi;
window.toggleSemuaOtomasi = toggleSemuaOtomasi;
window.toggleAutomationEngine = toggleAutomationEngine;
window.openAutomationSettingsModal = openAutomationSettingsModal;
window.closeAutomationSettingsModal = closeAutomationSettingsModal;
window.saveAutomationModalSettings = saveAutomationModalSettings;
window.toggleMesin = toggleMesin;
window.toggleAutoJoin = toggleAutoJoin;
