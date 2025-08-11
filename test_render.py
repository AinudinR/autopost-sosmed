# Isi baru untuk file test_render.py
import csv
import os
from autopost import log, generate_tts, render_video


def run_render_test():
    csv_path = 'queue.csv'
    log("Memulai tes render video DENGAN MUSIK...")

    # 1. Ambil data dari baris pertama queue.csv
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            first_row = next(reader)
    except (FileNotFoundError, StopIteration):
        log(f"KRITIS: Tidak bisa membaca baris pertama dari {csv_path}.")
        return

    text = (first_row.get('Teks') or 'Ini adalah teks contoh.').strip()
    bg = (first_row.get('BG') or 'assets/bg1.mp4').strip()
    # BARIS PENTING: Ambil path musik dari kolom 'Music'
    music = (first_row.get('Music') or '').strip()

    log(f"Menggunakan teks: '{text[:40]}...'")
    log(f"Menggunakan background: {bg}")
    if music:
        log(f"Menggunakan musik: {music}")
    else:
        log("Tidak ada musik yang ditentukan untuk baris ini.")

    # 2. Siapkan path output
    output_dir = 'out'
    os.makedirs(output_dir, exist_ok=True)
    audio_path = os.path.join(output_dir, 'test_audio.mp3')
    video_path = os.path.join(output_dir,
                              'test_video_with_music.mp4')  # Nama file baru

    # 3. Buat audio
    log("Membuat audio (TTS)...")
    if not generate_tts(text, audio_path):
        log("Gagal membuat audio. Tes dihentikan.")
        return

    # 4. Buat video
    log("Membuat video (render)...")
    try:
        # Kirim path musik ke fungsi render
        render_video(bg, audio_path, text, video_path, music_path=music)
        log("=" * 30)
        log(f"SUKSES! Video tes berhasil dibuat di: {video_path}")
        log("Silakan download untuk melihat hasilnya.")
        log("=" * 30)
    except Exception as e:
        log(f"Gagal membuat video: {e}")


if __name__ == "__main__":
    run_render_test()
