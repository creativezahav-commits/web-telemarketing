# Final Automation Policy

## Alur final
Scraper -> Import -> Permission -> Assignment -> Stabilization -> Campaign Session Queue -> Delivery -> Cooldown/Hold -> Recovery

## State grup
- `ready_assign`: grup valid dan siap dipilih owner.
- `assigned`: owner sudah dipilih, tetapi owner belum terkonfirmasi join grup.
- `managed`: owner valid dan sudah cocok dengan grup.
- `stabilization_wait`: grup baru di-assign atau baru selesai recovery assignment; ditahan sementara sebelum masuk sesi broadcast.
- `broadcast_eligible`: grup siap dimasukkan ke sesi broadcast berikutnya.
- `queued`: grup sudah masuk sesi campaign aktif/berikutnya.
- `cooldown`: grup baru saja berhasil dikirimi dan sedang menunggu jeda aman.
- `hold_waiting_response`: chat terakhir masih pesan kita, tahan agar hemat kuota.
- `hold_inactive`: grup terlalu sepi/tidak aktif, tahan dari broadcast.
- `blocked`: pengiriman diblok secara operasional/izin.
- `failed`: target atau delivery gagal dan menunggu recovery/retry.

## Policy assignment
- Assignment hanya memakai akun yang lolos health, warming, cooldown, dan kapasitas harian.
- Jika grup sudah memiliki owner yang masih join grup, status dipromosikan ke `managed`.
- Grup yang baru `managed` **tidak langsung** masuk broadcast. Sistem memberi `broadcast_ready_at` berdasarkan `assignment_broadcast_delay_minutes`.

## Policy broadcast
- Broadcast memakai **session-based campaign**.
- Satu sesi campaign adalah satu baris `campaign` dengan metadata `session_key`, `session_status`, dan `session_target_limit`.
- Sistem hanya memproses satu sesi aktif sekaligus pada stage delivery.
- Grup baru yang lolos assign akan masuk sesi berikutnya bila `campaign_allow_mid_session_enqueue = 0`.
- Per akun ada batas batch per siklus delivery melalui `campaign_session_per_sender_limit`.

## Policy sender
- Sender utama adalah `owner_phone`.
- Jika owner offline/tidak siap, orchestrator boleh memilih fallback sender online yang lebih cocok.
- Prioritas fallback:
  1. owner yang sedang online
  2. akun online yang sudah join grup
  3. akun online lain dengan ranking terbaik

## Policy hemat kuota
- Jika `last_chat <= last_kirim`, grup masuk `hold_waiting_response`.
- Jika `idle_days >= campaign_inactive_threshold_days`, grup masuk `hold_inactive`.
- Setelah sukses kirim, grup masuk `cooldown` selama `campaign_group_cooldown_hours`.

## Policy recovery
- `scrape_job`: resume bila worker hilang atau job macet.
- `assignment`: cari kandidat baru, lalu kembalikan grup ke `stabilization_wait`.
- `campaign`: target failed di-requeue ke sesi yang sama dan sesi ditandai queued lagi.

## Setting penting
- `assignment_broadcast_delay_minutes`
- `campaign_session_target_limit`
- `campaign_session_per_sender_limit`
- `campaign_allow_mid_session_enqueue`
- `campaign_group_cooldown_hours`
- `campaign_requeue_sender_missing`
- `campaign_skip_inactive_groups_enabled`
- `campaign_skip_if_last_chat_is_ours`
