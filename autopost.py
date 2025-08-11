import os, csv, math, json, random
from datetime import datetime
import pytz
import requests

# --- Pillow compatibility shim (Pillow >=10) ---
from PIL import Image as PILImage
if not hasattr(PILImage, "ANTIALIAS"):
    try:
        PILImage.ANTIALIAS = PILImage.Resampling.LANCZOS
    except Exception:
        PILImage.ANTIALIAS = getattr(PILImage, "LANCZOS", 1)

# moviepy, google api, & telegram
from moviepy.editor import *
from moviepy.audio.fx.all import volumex, audio_loop
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import telegram

# gTTS fallback
from gtts import gTTS

WIB = pytz.timezone('Asia/Jakarta')
VOICE_AZURE = 'id-ID-GadisNeural'


# ===================== UTIL =====================
def log(msg):
    print(
        f"[{datetime.now(WIB).strftime('%Y-%m-%d %H:%M:%S')}] [autopost] {msg}"
    )


def ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


# ===================== VOICE PICKER =====================
def pick_eleven_voice() -> str:
    ids = (os.getenv("ELEVENLABS_VOICE_IDS") or "").strip()
    if ids:
        arr = [v.strip() for v in ids.split(",") if v.strip()]
        if arr:
            choice = random.choice(arr)
            log(f"ElevenLabs voice picked: {choice}")
            return choice
    single = (os.getenv("ELEVENLABS_VOICE_ID") or "").strip()
    if single:
        log(f"ElevenLabs voice picked (single): {single}")
        return single
    default_voice = "21m00Tcm4TlvDq8ikWAM"
    log(f"ElevenLabs voice picked (default): {default_voice}")
    return default_voice


# ===================== TTS ======================
def tts_elevenlabs(text: str, out_path: str) -> bool:
    api_key = (os.getenv("ELEVENLABS_API_KEY") or "").strip()
    if not api_key: return False
    voice_id = pick_eleven_voice()
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    headers = {"xi-api-key": api_key, "Content-Type": "application/json"}
    data = {
        "text": text,
        "voice_settings": {
            "stability": 0.65,
            "similarity_boost": 0.8
        }
    }
    try:
        r = requests.post(url, headers=headers, json=data, timeout=60)
        if r.status_code == 200:
            with open(out_path, "wb") as f:
                f.write(r.content)
            log("TTS ElevenLabs OK")
            return True
        else:
            log(f"TTS ElevenLabs gagal: {r.status_code} {r.text[:200]}")
            return False
    except Exception as e:
        log(f"TTS ElevenLabs error: {e}")
        return False


def tts_azure(text: str, out_path: str) -> bool:
    key = (os.getenv('AZURE_TTS_KEY') or os.getenv('AZURE_SPEECH_KEY')
           or "").strip()
    region = (os.getenv('AZURE_TTS_REGION') or os.getenv('AZURE_SPEECH_REGION')
              or "").strip()
    if not key or not region: return False
    url = f"https://{region}.tts.speech.microsoft.com/cognitiveservices/v1"
    ssml = f'''<speak version="1.0" xml:lang="id-ID"><voice name="{VOICE_AZURE}">{text}</voice></speak>'''
    headers = {
        'Ocp-Apim-Subscription-Key': key,
        'Content-Type': 'application/ssml+xml',
        'X-Microsoft-OutputFormat': 'audio-24khz-96kbitrate-mono-mp3'
    }
    try:
        r = requests.post(url,
                          headers=headers,
                          data=ssml.encode('utf-8'),
                          timeout=60)
        r.raise_for_status()
        with open(out_path, 'wb') as f:
            f.write(r.content)
        log("TTS Azure OK")
        return True
    except Exception as e:
        log(f"TTS Azure gagal: {e}")
        return False


def tts_gtts(text: str, out_path: str) -> bool:
    try:
        gTTS(text=text, lang='id').save(out_path)
        log("TTS gTTS OK")
        return True
    except Exception as e:
        log(f"TTS gTTS error: {e}")
        return False


def generate_tts(text: str, out_path: str) -> bool:
    if tts_elevenlabs(text, out_path): return True
    if tts_azure(text, out_path): return True
    return tts_gtts(text, out_path)


# ===================== RENDER ====================
def render_video(bg_path: str,
                 narration_path: str,
                 main_text: str,
                 out_path: str,
                 music_path: str = None,
                 duration_max: int = 60):
    ensure_dir(out_path)
    narration_audio = AudioFileClip(narration_path)
    base_duration = min(duration_max, max(5, narration_audio.duration + 1.2))

    if not os.path.exists(bg_path):
        log(f'BG tidak ditemukan: {bg_path}, pakai latar hitam')
        clip = ColorClip(size=(1080, 1920),
                         color=(0, 0, 0),
                         duration=base_duration)
    elif bg_path.lower().endswith(('.mp4', '.mov', '.mkv', '.webm')):
        bg_clip = VideoFileClip(bg_path).without_audio()
        if bg_clip.duration < base_duration:
            loops = math.ceil(base_duration / max(0.1, bg_clip.duration))
            bg_clip = concatenate_videoclips([bg_clip] * loops)
        clip = bg_clip.subclip(0, base_duration)
        clip = clip.fx(vfx.resize, height=1920).fx(vfx.crop,
                                                   width=1080,
                                                   height=1920,
                                                   x_center=clip.w / 2,
                                                   y_center=clip.h / 2)
    else:
        clip = ImageClip(bg_path).set_duration(base_duration)
        clip = clip.fx(vfx.resize, height=1920).fx(vfx.crop,
                                                   width=1080,
                                                   height=1920,
                                                   x_center=clip.w / 2,
                                                   y_center=clip.h / 2)

    words = main_text.split()
    lines = []
    current_line = ""
    max_len = 30
    for word in words:
        if len(current_line + " " + word) <= max_len:
            current_line += " " + word
        else:
            lines.append(current_line.strip())
            current_line = word
    lines.append(current_line.strip())

    text_clips = []
    if lines:
        duration_per_line = narration_audio.duration / len(lines)
        for i, line in enumerate(lines):
            start_time = i * duration_per_line
            txt_clip = TextClip(line,
                                fontsize=24,
                                font='Montserrat-Bold',
                                color='white',
                                method='caption',
                                align='center',
                                size=(950, None))
            bg_box = ColorClip(size=(txt_clip.w + 60, txt_clip.h + 40),
                               color=(0, 0, 0)).set_opacity(0.5)
            text_with_bg = CompositeVideoClip(
                [bg_box, txt_clip.set_position('center')])
            final_clip = text_with_bg.set_position(
                ('center', 0.2),
                relative=True).set_start(start_time).set_duration(
                    duration_per_line).fx(vfx.fadein,
                                          0.3).fx(vfx.fadeout, 0.3)
            final_clip = final_clip.resize(lambda t: 1 + 0.02 * t)
            text_clips.append(final_clip)

    final_audio = narration_audio
    if music_path and os.path.exists(music_path):
        try:
            music = AudioFileClip(music_path).fx(volumex, 0.2)
            if music.duration < narration_audio.duration:
                music = music.fx(audio_loop, duration=narration_audio.duration)
            else:
                music = music.subclip(0, narration_audio.duration)

            final_audio = CompositeAudioClip([narration_audio, music])
            log(f"Musik latar '{music_path}' berhasil ditambahkan.")
        except Exception as e:
            log(f"Peringatan: Gagal memproses musik latar '{music_path}': {e}")

    composed = CompositeVideoClip([clip] + text_clips).set_audio(final_audio)
    composed = composed.resize((1080, 1920))
    composed.write_videofile(out_path,
                             fps=30,
                             codec='libx264',
                             audio_codec='aac',
                             preset='medium',
                             threads=4)


# ===================== UPLOADERS ====================
def upload_youtube_scheduled(file_path: str, title: str, description: str,
                             publish_dt_wib: datetime):
    client_id = (os.environ.get('GOOGLE_CLIENT_ID') or '').strip()
    client_secret = (os.environ.get('GOOGLE_CLIENT_SECRET') or '').strip()
    refresh_token = (os.environ.get('GOOGLE_REFRESH_TOKEN') or '').strip()
    if not all([client_id, client_secret, refresh_token]):
        raise RuntimeError('Google OAuth secrets belum lengkap.')
    publish_utc = publish_dt_wib.astimezone(pytz.utc)
    creds = Credentials(None,
                        refresh_token=refresh_token,
                        token_uri='https://oauth2.googleapis.com/token',
                        client_id=client_id,
                        client_secret=client_secret)
    yt = build('youtube', 'v3', credentials=creds)
    body = {
        'snippet': {
            'title': (title or 'Shorts')[:100],
            'description': description or '',
            'tags': ['motivasi', 'shorts', 'inspirasi']
        },
        'status': {
            'privacyStatus': 'private',
            'publishAt': publish_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'selfDeclaredMadeForKids': False
        }
    }
    media = MediaFileUpload(file_path,
                            chunksize=-1,
                            resumable=True,
                            mimetype='video/*')
    request = yt.videos().insert(part=','.join(body.keys()),
                                 body=body,
                                 media_body=media)
    response = None
    while response is None:
        status, response = request.next_chunk()
    vid = response.get('id')
    log(f'YouTube uploaded video id: {vid}')
    return vid


def upload_telegram(file_path: str, caption: str):
    bot_token = (os.environ.get('TELEGRAM_BOT_TOKEN') or '').strip()
    channel_id = (os.environ.get('TELEGRAM_CHANNEL_ID') or '').strip()
    if not bot_token or not channel_id:
        log("Peringatan: Kunci rahasia Telegram belum lengkap, skip upload Telegram."
            )
        return False
    try:
        bot = telegram.Bot(token=bot_token)
        with open(file_path, 'rb') as video_file:
            bot.send_video(chat_id=channel_id,
                           video=video_file,
                           caption=caption,
                           timeout=120)
        log("Berhasil mengunggah ke Telegram.")
        return True
    except Exception as e:
        log(f"Gagal mengunggah ke Telegram: {e}")
        return False


def test_telegram_message():
    bot_token = (os.environ.get('TELEGRAM_BOT_TOKEN') or '').strip()
    channel_id = (os.environ.get('TELEGRAM_CHANNEL_ID') or '').strip()
    if not bot_token or not channel_id:
        log("KRITIS: Kunci rahasia Telegram (TELEGRAM_BOT_TOKEN atau TELEGRAM_CHANNEL_ID) belum diatur di Secrets."
            )
        return False
    try:
        bot = telegram.Bot(token=bot_token)
        message = "Halo! Ini adalah pesan tes dari bot autopost. Jika Anda melihat ini, artinya bot berhasil terhubung ke channel Anda. 👍"
        bot.send_message(chat_id=channel_id, text=message)
        log("Pesan tes berhasil dikirim ke Telegram!")
        return True
    except Exception as e:
        log(f"Gagal mengirim pesan tes ke Telegram: {e}")
        log(f"Pastikan TELEGRAM_BOT_TOKEN sudah benar dan bot sudah menjadi admin di channel dengan ID: {channel_id}"
            )
        return False


# ===================== QUEUE ======================
def parse_wib_time(date_str: str, time_str: str) -> datetime:
    hh, mm = (time_str or '07:30').split(':')
    hh, mm = int(hh), int(mm)
    y, m, d = map(int, date_str.split('-'))
    return WIB.localize(datetime(y, m, d, hh, mm))


def pick_job(csv_path: str = 'queue.csv'):
    """
    Memilih pekerjaan dari file CSV dengan logika yang lebih sederhana.
    Akan menjalankan pekerjaan jika waktunya sudah lewat tapi belum lebih dari 1 jam yang lalu.
    """
    now = datetime.now(WIB)
    if not os.path.exists(csv_path):
        log(f'{csv_path} tidak ditemukan')
        return None
        
    with open(csv_path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            # Check 1: Has it been posted to YouTube already?
            status = (row.get('Status') or '').strip()
            if "POSTED-YT" in status:
                continue

            # Check 2: Is it time to post?
            try:
                scheduled_dt = parse_wib_time(row.get('Tanggal'), row.get('JamWIB'))
                time_difference = (now - scheduled_dt).total_seconds()

                # Jika jadwal sudah lewat (positif) tapi belum lebih dari 1 jam (3600 detik)
                if 0 <= time_difference < 3600:
                    log(f"Menemukan jadwal yang valid: {row.get('Judul')}")
                    return scheduled_dt, row
            except Exception:
                # Lewati baris jika format tanggal/waktu salah atau kosong
                continue
            
    return None # No jobs are ready to be posted right now


def mark_posted(csv_path: str,
                row_to_mark: dict,
                platform: str,
                note: str = ''):
    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    idx = -1
    for i, r in enumerate(rows):
        if (r.get('Tanggal', '').strip() == row_to_mark.get(
                'Tanggal', '').strip() and r.get('Judul', '').strip()
                == row_to_mark.get('Judul', '').strip()):
            idx = i
            break
    if idx == -1:
        log("Gagal menemukan baris untuk ditandai di CSV.")
        return

    status_old = rows[idx].get('Status', '')
    mark = f'POSTED-{platform}'
    if status_old and mark not in status_old:
        rows[idx]['Status'] = status_old + '|' + mark
    elif not status_old:
        rows[idx]['Status'] = mark
    
    if note: rows[idx]['Status'] += f'({note})'

    headers = rows[0].keys() if rows else [
        'Tanggal', 'Judul', 'Teks', 'Hashtag', 'LinkAffiliate', 'BG', 'Music',
        'Status', 'JamWIB'
    ]
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writerows(rows)
    log(f"Baris '{row_to_mark.get('Judul')}' telah ditandai untuk platform: {platform}"
        )


# ===================== MAIN FUNCTION ======================
def run_job_check():
    log("Mengecek jadwal posting...")
    job = pick_job('queue.csv')
    if not job:
        log('Tidak ada jadwal untuk saat ini.')
        return
    dt_wib, row = job
    title = (row.get('Judul') or 'Shorts').strip()
    text = (row.get('Teks') or '').strip()
    hashtags = (row.get('Hashtag') or '').strip()
    link = (row.get('LinkAffiliate') or '').strip()
    bg = (row.get('BG') or 'assets/bg1.jpg').strip()
    music = (row.get('Music') or '').strip()

    yt_caption = f"{text}\n\n{hashtags}\n{link}".strip()
    tg_caption = f"{title}\n\n{text}\n\n{hashtags}".strip()
    log(f"Memulai pekerjaan untuk: '{title}'")
    os.makedirs('out', exist_ok=True)
    audio_path = 'out/narasi.mp3'
    video_path = 'out/video.mp4'
    log('Membuat audio (TTS)...')
    if not generate_tts(text, audio_path):
        log('KRITIS: Gagal membuat audio. Pekerjaan dihentikan.')
        return
    log('Membuat video...')
    try:
        render_video(bg,
                     audio_path,
                     text,
                     video_path,
                     music_path=music,
                     duration_max=60)
    except Exception as e:
        log(f"KRITIS: Gagal membuat video: {e}. Pekerjaan dihentikan.")
        return
    yt_vid_id = None
    try:
        log('Mengunggah ke YouTube (dijadwalkan)...')
        yt_vid_id = upload_youtube_scheduled(video_path, title, yt_caption,
                                             dt_wib)
        mark_posted('queue.csv', row, platform='YT', note=yt_vid_id or '')
    except Exception as e:
        log(f"Peringatan: Gagal mengunggah ke YouTube: {e}")
    try:
        log('Mengunggah ke Telegram...')
        if upload_telegram(video_path, tg_caption):
            mark_posted('queue.csv', row, platform='TG')
    except Exception as e:
        log(f"Peringatan: Gagal mengunggah ke Telegram: {e}")
    log(f"PEKERJAAN SELESAI untuk '{title}'")

# BAGIAN BARU: Menjalankan fungsi utama jika script dieksekusi langsung
if __name__ == "__main__":
    run_job_check()
