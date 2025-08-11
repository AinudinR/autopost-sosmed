# main.py - Versi Cerdas
import time
import csv
from datetime import datetime, timedelta
import pytz
from keep_alive import keep_alive
from autopost import run_job_check, log

WIB = pytz.timezone('Asia/Jakarta')


def get_upcoming_schedule(csv_path='queue.csv'):
    """Membaca CSV dan mengembalikan daftar waktu posting yang akan datang."""
    now = datetime.now(WIB)
    schedule_times = []
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Lewati baris yang sudah ada statusnya
                if (row.get('Status') or '').strip():
                    continue

                tgl_str = row.get('Tanggal')
                jam_str = row.get('JamWIB', '07:30')

                try:
                    # Gabungkan tanggal dan waktu dari CSV
                    dt_wib = pytz.timezone('Asia/Jakarta').localize(
                        datetime.strptime(f"{tgl_str} {jam_str}",
                                          '%Y-%m-%d %H:%M'))
                    # Hanya tambahkan jika jadwalnya di masa depan
                    if dt_wib > now:
                        schedule_times.append(dt_wib)
                except (ValueError, TypeError):
                    # Lewati baris jika format tanggal/waktu salah
                    continue

    except FileNotFoundError:
        log(f"Peringatan: {csv_path} tidak ditemukan.")

    schedule_times.sort()
    return schedule_times


def main_loop():
    """Loop utama yang cerdas untuk menjadwalkan pengecekan."""
    while True:
        upcoming_jobs = get_upcoming_schedule()
        now = datetime.now(WIB)

        if upcoming_jobs:
            # Ada jadwal yang akan datang
            next_run_time = upcoming_jobs[0]
            log(f"Jadwal terdekat berikutnya adalah pada: {next_run_time.strftime('%Y-%m-%d %H:%M')}"
                )

            # Hitung waktu tidur sampai 1 menit sebelum jadwal
            sleep_duration = (next_run_time - now).total_seconds() - 60

            if sleep_duration > 0:
                log(f"Akan tidur selama {timedelta(seconds=sleep_duration)}.")
                time.sleep(sleep_duration)

            # Waktunya bangun dan jalankan pengecekan pekerjaan
            log("Waktunya pengecekan pekerjaan!")
            run_job_check()

            # Tunggu sebentar setelah pekerjaan selesai untuk menjadwalkan ulang
            log("Menunggu 2 menit setelah pengecekan sebelum mencari jadwal berikutnya."
                )
            time.sleep(120)

        else:
            # Tidak ada jadwal lagi, cek lagi dalam 6 jam
            log("Tidak ada jadwal ditemukan. Akan dicek kembali dalam 6 jam.")
            time.sleep(6 * 3600)


if __name__ == "__main__":
    log("Memulai Autopost Scheduler Cerdas...")
    keep_alive()  # Menjalankan server web agar Repl tetap aktif
    log("Server keep-alive sudah berjalan.")

    try:
        main_loop()
    except Exception as e:
        log(f"Terjadi error fatal pada loop utama: {e}")
        log("Akan mencoba restart dalam 5 menit.")
        time.sleep(300)
