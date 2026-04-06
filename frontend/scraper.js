let scraperJobAktif = null;
let scraperPoller = null;

async function _terapkanDefaultScraperDariSettings() {
    try {
        const rows = await _get('/settings');
        const map = {};
        (rows || []).forEach((row) => { map[row.key] = row.value; });
        const applyValue = (id, key) => {
            const el = document.getElementById(id);
            if (!el) return;
            const value = map[key];
            if (value == null || value === '') return;
            el.value = String(value);
        };
        applyValue('scraper-limit', 'scraper_limit_per_keyword');
        applyValue('scraper-min-members', 'scraper_min_members');
        applyValue('scraper-recommended-score', 'scraper_recommended_score');
        applyValue('scraper-max-terms', 'scraper_max_terms');
        applyValue('scraper-delay-min', 'scraper_delay_keyword_min');
        applyValue('scraper-delay-max', 'scraper_delay_keyword_max');
    } catch {}
}

async function muatTabScraper() {
    await _isiSelectAkun("scraper-akun");
    await _terapkanDefaultScraperDariSettings();
    await muatJobsScraper();
    if (!document.getElementById("scraper-preview-list")?.dataset.loaded) {
        await previewScraperKeywords();
    }
}

function _scraperOptions() {
    return {
        limit_per_keyword: parseInt(document.getElementById("scraper-limit").value || "30", 10),
        min_members: parseInt(document.getElementById("scraper-min-members").value || "0", 10),
        recommended_score: parseInt(document.getElementById("scraper-recommended-score").value || "55", 10),
        require_public_username: document.getElementById("scraper-require-username").checked,
        include_basic_groups: document.getElementById("scraper-include-basic").checked,
        include_supergroups: document.getElementById("scraper-include-super").checked,
        include_channels: document.getElementById("scraper-include-channels").checked,
        expand_terms: document.getElementById("scraper-expand-terms").checked,
        smart_expand: document.getElementById("scraper-expand-terms").checked,
        enrich_details: document.getElementById("scraper-enrich").checked,
        include_base: document.getElementById("scraper-include-base").checked,
        suffix_letters: document.getElementById("scraper-suffix-letters").checked,
        prefix_letters: document.getElementById("scraper-prefix-letters").checked,
        suffix_start: document.getElementById("scraper-suffix-start").value.trim() || "a",
        suffix_end: document.getElementById("scraper-suffix-end").value.trim() || "z",
        prefix_start: document.getElementById("scraper-prefix-start").value.trim() || "a",
        prefix_end: document.getElementById("scraper-prefix-end").value.trim() || "f",
        number_suffix: document.getElementById("scraper-number-enabled").checked,
        number_start: parseInt(document.getElementById("scraper-number-start").value || "1", 10),
        number_end: parseInt(document.getElementById("scraper-number-end").value || "20", 10),
        year_suffix: document.getElementById("scraper-year-enabled").checked,
        years_text: document.getElementById("scraper-years-text").value.trim(),
        derived_terms_enabled: document.getElementById("scraper-derived-enabled").checked,
        derived_terms_text: document.getElementById("scraper-derived-terms").value.trim(),
        custom_terms_enabled: document.getElementById("scraper-custom-enabled").checked,
        custom_terms_text: document.getElementById("scraper-custom-terms").value.trim(),
        max_terms: parseInt(document.getElementById("scraper-max-terms").value || "80", 10),
        delay_keyword_min: parseFloat(document.getElementById("scraper-delay-min").value || "0.8"),
        delay_keyword_max: parseFloat(document.getElementById("scraper-delay-max").value || "1.6"),
    };
}

async function previewScraperKeywords() {
    const keywords = document.getElementById("scraper-keywords").value.trim();
    if (!keywords) {
        document.getElementById("scraper-preview-summary").innerHTML = "Masukkan keyword dasar untuk melihat preview.";
        document.getElementById("scraper-preview-counts").innerHTML = "";
        document.getElementById("scraper-preview-list").innerHTML = `<div class="empty-state"><div class="icon">🧲</div><p>Belum ada keyword dasar.</p></div>`;
        return;
    }
    try {
        const res = await _post("/scraper/preview", { keywords, options: _scraperOptions() });
        const counts = res.source_counts || {};
        const tierCounts = res.tier_counts || {};
        const summaryParts = [`Total keyword siap jalan: <strong>${res.total || 0}</strong>`];
        if (res.truncated) summaryParts.push(`dibatasi oleh maksimal keyword generated`);
        if (tierCounts.high || tierCounts.medium || tierCounts.low) {
            summaryParts.push(`prioritas high ${tierCounts.high || 0} · medium ${tierCounts.medium || 0} · low ${tierCounts.low || 0}`);
        }
        document.getElementById("scraper-preview-summary").innerHTML = summaryParts.join(' · ');
        document.getElementById("scraper-preview-counts").innerHTML = Object.entries(counts)
            .filter(([, val]) => Number(val || 0) > 0)
            .map(([key, val]) => `<div class="stat-box mini"><div class="stat-value">${val}</div><div class="stat-label">${_labelSourceKey(key)}</div></div>`)
            .join("") || `<div class="hint">Belum ada variasi tambahan.</div>`;
        const previewItems = Array.isArray(res.keyword_items) && res.keyword_items.length ? res.keyword_items : (res.keywords || []).map(k => ({ keyword: k, tier: 'medium', source: 'base', priority: 50 }));
        document.getElementById("scraper-preview-list").innerHTML = previewItems
            .map(item => `<span class="keyword-chip tier-${escapeHtml(item.tier || 'medium')}" title="${escapeHtml(_labelSourceKey(item.source || 'base'))} · priority ${Number(item.priority || 50)}">${escapeHtml(item.keyword || '')}</span>`)
            .join("") || `<span class="keyword-chip muted">Tidak ada keyword valid</span>`;
        document.getElementById("scraper-preview-list").dataset.loaded = "1";
    } catch (err) {
        tampilPesan("pesan-scraper", `❌ ${err.message || 'Gagal membuat preview keyword.'}`, "gagal");
    }
}

function _labelSourceKey(key) {
    const labels = {
        base: 'Dasar',
        smart: 'Smart',
        suffix_letters: 'Suffix Huruf',
        prefix_letters: 'Prefix Huruf',
        numbers: 'Angka',
        years: 'Tahun',
        derived: 'Turunan',
        custom: 'Custom'
    };
    return labels[key] || key;
}

async function jalankanScraper() {
    const phone = document.getElementById("scraper-akun").value;
    const keywords = document.getElementById("scraper-keywords").value.trim();
    if (!phone) {
        tampilPesan("pesan-scraper", "⚠️ Pilih akun scraper dulu.", "gagal");
        return;
    }
    if (!keywords) {
        tampilPesan("pesan-scraper", "⚠️ Isi minimal satu keyword.", "gagal");
        return;
    }

    tampilPesan("pesan-scraper", "⏳ Menjalankan scraper...", "info");
    try {
        const res = await _post("/scraper/start", {
            phone,
            keywords,
            options: _scraperOptions(),
        });
        scraperJobAktif = res.job?.id;
        tampilPesan("pesan-scraper", `✅ Job scraper #${scraperJobAktif} dimulai.`, "berhasil");
        await muatJobsScraper();
        if (scraperJobAktif) {
            document.getElementById("scraper-job-select").value = String(scraperJobAktif);
            await muatHasilScraper();
            await muatKeywordRunsScraper();
        }
        mulaiPollingScraper();
    } catch (err) {
        tampilPesan("pesan-scraper", `❌ ${err.message || 'Gagal menjalankan scraper.'}`, "gagal");
    }
}

async function muatJobsScraper() {
    setLoading("scraper-jobs", "Memuat job scraper...");
    try {
        const jobs = await _get("/scraper/jobs?limit=20");
        const wrap = document.getElementById("scraper-jobs");
        const sel  = document.getElementById("scraper-job-select");
        const ctrl = document.getElementById("scraper-job-controls");

        if (!jobs.length) {
            wrap.innerHTML = `<div class="empty-state"><div class="icon">🧲</div><p>Belum ada job scraper.</p></div>`;
            sel.innerHTML  = `<option value="">-- Belum ada job --</option>`;
            if (ctrl) ctrl.style.display = "none";
            return;
        }

        if (!scraperJobAktif) scraperJobAktif = jobs[0].id;
        sel.innerHTML = jobs.map(job =>
            `<option value="${job.id}">#${job.id} · ${job.phone} · ${job.status}</option>`
        ).join("");
        sel.value = String(scraperJobAktif);

        // Tampil panel kontrol sesuai status job aktif
        const jobAktif   = jobs.find(j => j.id === scraperJobAktif) || jobs[0];
        const statusAktif = jobAktif?.status;
        const adaKontrol  = ["running", "paused", "queued"].includes(statusAktif);

        if (ctrl) {
            ctrl.style.display = adaKontrol ? "block" : "none";
            const namaEl = document.getElementById("scraper-job-nama-aktif");
            if (namaEl) namaEl.textContent = `#${jobAktif.id} (${statusAktif})`;
            const btnPause  = document.getElementById("btn-job-pause");
            const btnResume = document.getElementById("btn-job-resume");
            const btnStop   = document.getElementById("btn-job-stop");
            const btnRetry  = document.getElementById("btn-job-retry-failed");
            if (btnPause)  btnPause.style.display  = statusAktif === "running" ? "block" : "none";
            if (btnResume) btnResume.style.display = statusAktif === "paused"  ? "block" : "none";
            if (btnStop)   btnStop.style.display   = adaKontrol  ? "block" : "none";
            if (btnRetry)  btnRetry.style.display  = "block";
        }

        wrap.innerHTML = jobs.map(job => {
            const done  = job.processed_keywords || 0;
            const total = job.total_keywords || 0;
            const pct   = total ? Math.min(100, Math.round((done / total) * 100)) : 0;
            const kelas = job.status === "failed" ? "full" : "ok";
            return `
                <div class="scraper-job-card ${scraperJobAktif === job.id ? 'aktif' : ''}" onclick="gantiJobScraper('${job.id}')">
                    <div class="job-card-top">
                        <div>
                            <strong>#${job.id}</strong>
                            <small>${job.phone}</small>
                        </div>
                        <span class="badge badge-${_jobBadge(job.status)}">${job.status}</span>
                    </div>
                    <div class="limit-bar-track"><div class="limit-bar-fill ${kelas}" style="width:${pct}%"></div></div>
                    <div class="job-card-meta">
                        <small>${done}/${total} keyword</small>
                        <small>${job.total_saved || 0} hasil unik</small>
                    </div>
                    <div class="job-card-meta muted">
                        <small>${job.total_imported || 0} diimpor</small>
                        <small>${job.total_found || 0} ditemukan</small>
                    </div>
                </div>`;
        }).join("");
    } catch (err) {
        document.getElementById("scraper-jobs").innerHTML = `<div class="empty-state"><div class="icon">⚠️</div><p>${err.message || 'Gagal memuat job.'}</p></div>`;
    }
}

function _jobBadge(status) {
    const map = { running: 'aktif', done: 'berhasil', paused: 'skip', stopped: 'offline', failed: 'gagal', queued: 'normal' };
    return map[status] || 'normal';
}

async function gantiJobScraper(jobId) {
    scraperJobAktif = parseInt(jobId, 10);
    const sel = document.getElementById("scraper-job-select");
    if (sel) sel.value = String(scraperJobAktif);
    await muatJobsScraper();
    await muatHasilScraper();
    await muatKeywordRunsScraper();
}

async function muatKeywordRunsScraper() {
    if (!scraperJobAktif) return;
    setLoading("scraper-keyword-runs", "Memuat detail keyword...");
    try {
        const rows = await _get(`/scraper/jobs/${scraperJobAktif}/keywords`);
        const wrap = document.getElementById("scraper-keyword-runs");
        if (!rows.length) {
            wrap.innerHTML = `<div class="empty-state"><div class="icon">🧷</div><p>Belum ada detail keyword.</p></div>`;
            return;
        }
        wrap.innerHTML = `
            <div class="keyword-run-list">
                ${rows.map(r => `
                    <div class="keyword-run-item ${r.status}">
                        <div>
                            <strong>${escapeHtml(r.keyword)}</strong>
                            <div class="hint">${r.found_count || 0} ditemukan · ${r.saved_count || 0} unik tersimpan · kualitas ${Number(r.quality_score || 0)} · percobaan ${Number(r.attempt_count || 0)}/${Number(r.max_attempts || 1)}</div>
                            <div class="hint">sumber ${escapeHtml(_labelSourceKey(r.source || 'base'))} · tier ${escapeHtml((r.tier || 'medium').toUpperCase())} · priority ${Number(r.priority || 50)}</div>
                            ${r.error_message ? `<div class="job-error">${escapeHtml(r.error_message)}</div>` : ``}
                            ${r.last_error_code ? `<div class="hint">error code: ${escapeHtml(r.last_error_code)}</div>` : ``}
                        </div>
                        <span class="badge badge-${_jobBadge(r.status)}">${r.status}</span>
                    </div>`).join("")}
            </div>`;
    } catch (err) {
        document.getElementById("scraper-keyword-runs").innerHTML = `<div class="empty-state"><div class="icon">⚠️</div><p>${err.message || 'Gagal memuat keyword run.'}</p></div>`;
    }
}

async function muatHasilScraper() {
    if (!scraperJobAktif) return;
    setLoading("scraper-results", "Memuat hasil scrape...");
    try {
        const onlyNew = document.getElementById("scraper-filter-new").checked ? 1 : 0;
        const onlyRecommended = document.getElementById("scraper-filter-recommended").checked ? 1 : 0;
        const includeImported = document.getElementById("scraper-filter-imported").checked ? 1 : 0;

        const [job, results] = await Promise.all([
            _get(`/scraper/jobs/${scraperJobAktif}`),
            _get(`/scraper/jobs/${scraperJobAktif}/results?new=${onlyNew}&recommended=${onlyRecommended}&include_imported=${includeImported}`),
        ]);
        document.getElementById("scraper-job-summary").innerHTML = `
            Job #${job.id} · status <strong>${job.status}</strong> · ${job.processed_keywords || 0}/${job.total_keywords || 0} keyword ·
            ditemukan ${job.total_found || 0} · tersimpan ${job.total_saved || 0} · diimpor ${job.total_imported || 0}
            ${job.error_message ? `<br><span class="job-error">Catatan: ${escapeHtml(job.error_message)}</span>` : ""}`;

        const wrap = document.getElementById("scraper-results");
        if (!results.length) {
            wrap.innerHTML = `<div class="empty-state"><div class="icon">📭</div><p>Tidak ada hasil yang cocok dengan filter saat ini.</p></div>`;
            return;
        }

        wrap.innerHTML = `
            <div class="scraper-results-header">
                <label><input type="checkbox" id="scraper-check-all" onchange="toggleSemuaScrape(this.checked)"> Pilih semua</label>
                <div class="hint">${results.length} hasil</div>
            </div>
            <div class="scraper-results-grid">
                ${results.map(r => `
                    <label class="scrape-card ${r.imported ? 'imported' : ''} ${r.recommended ? 'recommended' : ''}">
                        <div class="scrape-card-top">
                            <input class="scrape-result-check" type="checkbox" value="${r.id}">
                            <div>
                                <strong>${escapeHtml(r.nama || '-')}</strong>
                                <small>${r.tipe || '-'} · ${Number(r.jumlah_member || 0).toLocaleString()} member</small>
                            </div>
                            <div class="scrape-badges">
                                ${r.recommended ? `<span class="badge badge-aktif">⭐ Rekomendasi</span>` : ``}
                                ${r.already_in_db ? `<span class="badge badge-skip">Sudah ada</span>` : `<span class="badge badge-berhasil">Baru</span>`}
                                ${r.imported ? `<span class="badge badge-offline">Diimpor</span>` : ``}
                            </div>
                        </div>
                        <div class="scrape-meta">
                            <div><strong>Score:</strong> ${r.relevance_score || 0}</div>
                            <div><strong>Keyword:</strong> ${escapeHtml(r.sumber_keyword || '-')}</div>
                            <div><strong>Username:</strong> ${r.username ? '@' + escapeHtml(r.username) : '-'}</div>
                            <div><strong>Link:</strong> ${r.link ? `<a href="${r.link}" target="_blank">${escapeHtml(r.link)}</a>` : '-'}</div>
                            <div><strong>Catatan:</strong> ${escapeHtml(r.catatan || '-')}</div>
                        </div>
                        ${r.deskripsi ? `<div class="scrape-desc">${escapeHtml(r.deskripsi)}</div>` : ``}
                    </label>`).join("")}
            </div>`;
    } catch (err) {
        document.getElementById("scraper-results").innerHTML = `<div class="empty-state"><div class="icon">⚠️</div><p>${err.message || 'Gagal memuat hasil.'}</p></div>`;
    }
}

function toggleSemuaScrape(checked) {
    document.querySelectorAll('.scrape-result-check').forEach(el => { el.checked = checked; });
}

function hasilScrapeTerpilih() {
    return [...document.querySelectorAll('.scrape-result-check:checked')].map(el => parseInt(el.value, 10));
}

async function imporPilihanScraper() {
    if (!scraperJobAktif) {
        alert('Pilih job scraper dulu dari daftar di kanan.');
        return;
    }
    const result_ids = hasilScrapeTerpilih();
    if (!result_ids.length) {
        alert('Centang minimal satu hasil scrape terlebih dahulu, atau klik "Pilih semua".');
        return;
    }
    await imporScraper('selected', result_ids);
}

async function imporRekomendasiScraper() {
    if (!scraperJobAktif) {
        alert('Pilih job scraper dulu dari daftar di kanan.');
        return;
    }
    await imporScraper('recommended', []);
}

async function imporSemuaScraper() {
    if (!scraperJobAktif) {
        alert('Pilih job scraper dulu dari daftar di kanan.');
        return;
    }
    await imporScraper('all', []);
}

async function imporScraper(mode, result_ids) {
    // Tampilkan loading dulu
    tampilPesan('pesan-scraper', '⏳ Sedang mengimpor...', 'info');
    try {
        const res = await _post('/scraper/import', {
            job_id: scraperJobAktif,
            mode,
            result_ids,
        });

        const jumlah = res.imported || 0;
        const skipped = res.skipped_channels || 0;

        // Backend bisa kirim field "info" kalau ada masalah spesifik
        if (res.info && jumlah === 0) {
            tampilPesan('pesan-scraper', `⚠️ ${res.info}`, 'info');
        } else if (res.error && jumlah === 0) {
            tampilPesan('pesan-scraper', `❌ ${res.error}`, 'gagal');
        } else if (jumlah === 0 && mode === 'recommended') {
            // Bantu user mengerti kenapa kosong
            tampilPesan('pesan-scraper',
                `⚠️ Tidak ada grup yang diimpor. Kemungkinan semua hasil score-nya di bawah threshold rekomendasi, atau sudah pernah diimpor sebelumnya. Coba klik "Impor Semua" untuk mengimpor tanpa filter score.`,
                'info');
        } else if (jumlah === 0 && mode === 'all') {
            tampilPesan('pesan-scraper',
                `⚠️ Tidak ada grup baru yang diimpor. Semua hasil scrape kemungkinan sudah pernah diimpor sebelumnya. Centang filter "Tampilkan yang sudah diimpor" untuk melihatnya.`,
                'info');
        } else if (jumlah === 0 && mode === 'selected') {
            tampilPesan('pesan-scraper',
                `⚠️ Tidak ada grup yang diimpor. Pastikan hasil yang dipilih belum pernah diimpor sebelumnya.`,
                'info');
        } else {
            tampilPesan('pesan-scraper',
                `✅ ${jumlah} grup berhasil diimpor ke database utama!` +
                (skipped ? ` (${skipped} channel dilewati karena bukan grup)` : '') +
                ` Cek tab Analisis Grup untuk melihatnya.`,
                'berhasil');
        }

        await muatHasilScraper();
        await muatJobsScraper();
    } catch (err) {
        tampilPesan('pesan-scraper', `❌ Gagal impor: ${err.message || 'Terjadi kesalahan.'}`, 'gagal');
    }
}

async function pauseJobScraper() {
    if (!scraperJobAktif) return;
    try {
        await _post(`/scraper/jobs/${scraperJobAktif}/pause`, {});
        tampilPesan('pesan-scraper', '⏸️ Job dipause.', 'info');
        await muatJobsScraper();
        await muatHasilScraper();
        await muatKeywordRunsScraper();
    } catch (err) {
        tampilPesan('pesan-scraper', `❌ ${err.message || 'Gagal pause job.'}`, 'gagal');
    }
}

async function resumeJobScraper() {
    if (!scraperJobAktif) return;
    try {
        await _post(`/scraper/jobs/${scraperJobAktif}/resume`, {});
        tampilPesan('pesan-scraper', '▶️ Job dilanjutkan.', 'berhasil');
        mulaiPollingScraper();
        await muatJobsScraper();
        await muatHasilScraper();
        await muatKeywordRunsScraper();
    } catch (err) {
        tampilPesan('pesan-scraper', `❌ ${err.message || 'Gagal resume job.'}`, 'gagal');
    }
}

async function stopJobScraper() {
    if (!scraperJobAktif) return;
    if (!confirm('Stop job scraper ini?')) return;
    try {
        await _post(`/scraper/jobs/${scraperJobAktif}/stop`, {});
        tampilPesan('pesan-scraper', '⏹️ Job dihentikan.', 'info');
        await muatJobsScraper();
        await muatHasilScraper();
        await muatKeywordRunsScraper();
    } catch (err) {
        tampilPesan('pesan-scraper', `❌ ${err.message || 'Gagal menghentikan job.'}`, 'gagal');
    }
}


async function retryFailedJobScraper() {
    if (!scraperJobAktif) return;
    try {
        await _post(`/scraper/jobs/${scraperJobAktif}/retry-failed`, {});
        tampilPesan('pesan-scraper', '🔁 Keyword failed dimasukkan ulang ke antrean.', 'berhasil');
        mulaiPollingScraper();
        await muatJobsScraper();
        await muatHasilScraper();
        await muatKeywordRunsScraper();
    } catch (err) {
        tampilPesan('pesan-scraper', `❌ ${err.message || 'Gagal retry keyword failed.'}`, 'gagal');
    }
}

function mulaiPollingScraper() {
    if (scraperPoller) clearInterval(scraperPoller);
    scraperPoller = setInterval(async () => {
        if (!scraperJobAktif || document.getElementById('tab-scraper').style.display === 'none') return;
        try {
            const job = await _get(`/scraper/jobs/${scraperJobAktif}`);
            await muatJobsScraper();
            await muatHasilScraper();
            await muatKeywordRunsScraper();
            if (![ 'running', 'queued', 'paused' ].includes(job.status)) {
                clearInterval(scraperPoller);
                scraperPoller = null;
            }
        } catch {}
    }, 5000);
}

function escapeHtml(text) {
    return String(text || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}
