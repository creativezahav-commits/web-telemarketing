# Dashboard State Grup

Dashboard ini menampilkan grup ke dalam 7 lane utama agar operator mudah melihat bottleneck pipeline broadcast:

1. **Stabilization** — grup baru di-assign dan sedang masa tunggu.
2. **Eligible** — grup siap masuk sesi broadcast berikutnya.
3. **Queued** — grup sudah masuk queue/sesi dan menunggu atau sedang diproses.
4. **Cooldown** — grup baru selesai dikirim dan sedang jeda aman.
5. **Hold** — grup ditahan oleh guard, izin, assignment, atau status operasional lain.
6. **Failed** — assignment atau delivery gagal tetapi belum masuk recovery utama.
7. **Recovery** — ada recovery item aktif yang perlu tindakan.

## Aturan prioritas state

Setiap grup hanya masuk satu lane utama dengan prioritas berikut:

1. `recovery`
2. `failed`
3. `queued`
4. `cooldown`
5. `stabilization`
6. `eligible`
7. `hold`

Dengan prioritas ini, grup tidak tampil ganda di dua lane berbeda.

## Data yang ditampilkan per grup

- nama grup / username
- owner akun
- assignment status
- broadcast status
- send guard status
- last chat
- last kirim
- ready at / waktu siap berikutnya
- target campaign aktif terakhir
- status recovery jika ada
- keyword sumber dan score

## Endpoint backend

- `GET /api/v2/overview/group-states`

Parameter:
- `search`
- `focus_state`
- `limit_per_state`
- `include_archived=1`

## Tombol cepat di dashboard

- **Refresh** → muat ulang lane.
- **Run Orchestrator** → jalankan satu siklus orkestrator manual.
- **Scan Recovery** → deteksi entitas recovery baru.

## Kapan lane perlu diperhatikan

- **Stabilization tinggi**: banyak grup baru assigned tetapi belum lolos ke eligible.
- **Queued tinggi tanpa sesi aktif**: queue tertahan, cek engine campaign/draft.
- **Hold tinggi**: guard atau rule menahan terlalu banyak grup.
- **Recovery tinggi**: perlu tindakan pemulihan manual atau tuning rule.
