import csv, os, io, math, json
from datetime import datetime
import pytz
import requests
from dateutil import tz
from moviepy.editor import *
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

"""
Autopost pipeline:
1) Pilih baris di queue.csv untuk HARI INI dan slot jam terdekat (07:30 / 19:00 WIB default).
2) TTS Azure (id-ID) -> MP3.
3) Render video 9:16 (1080x1920) dari BG (video/foto) + overlay judul + audio.
4) Upload YouTube Shorts sebagai private + scheduled (publishAt sesuai WIB).
5) (Opsional) Panggil Activepieces webhook untuk IG/FB/Threads/TikTok.

Secrets yang dibaca dari environment:
- GOOGLE_CLIENT_ID
- GOOGLE_CLIENT_SECRET
- GOOGLE_REFRESH_TOKEN
- AZURE_SPEECH_KEY
- AZURE_SPEECH_REGION
- (opsional) AP_BASE_URL, AP_SECRET
"""

WIB = pytz.timezone('Asia/Jakarta')
VOICE = 'id-ID-GadisNeural'  # alternatif: 'id-ID-ArdiNeural'

# ---------------- Azure TTS ---------------- #
def tts_azure(text: str, out_path: str):
    key = os.environ.get('AZURE_SPEECH_KEY')
    region = os.environ.get('AZURE_SPEECH_REGION')
    if not key or not region:
        raise RuntimeError('AZURE_SPEECH_KEY/REGION belum di-set di Secrets')
    url = f"https://{region}.tts.speech.microsoft.com/cognitiveservices/v1"
    ssml = f'''<speak version="1.0" xml:lang="id-ID">
      <voice name="{VOICE}">{text}</voice>
    </speak>'''
    headers = {
        'Ocp-Apim-Subscription-Key': key,
        'Content-Type': 'application/ssml+xml',
        'X-Microsoft-OutputFormat': 'audio-24khz-96kbitrate-mono-mp3'
    }
    r = requests.post(url, headers=headers, data=ssml.encode('utf-8'))
    r.raise_for_status()
    with open(out_path, 'wb') as f:
        f.write(r.content)

# --------------- Render Video 9:16 --------------- #
def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def render_video(bg_path: str, narration_path: str, title_text: str, out_path: str, duration_max: int = 60):
    ensure_dir(out_path)
    audio = AudioFileClip(narration_path)
    base_duration = min(duration_max, max(5, audio.duration + 0.8))

    if not os.path.exists(bg_path):
        # fallback warna polos hitam
        clip = ColorClip(size=(1080,1920), color=(0,0,0), duration=base_duration)
    elif bg_path.lower().endswith(('.mp4','.mov','.mkv','.webm')):
        clip = VideoFileClip(bg_path).without_audio()
        if clip.duration < base_duration:
            loops = math.ceil(base_duration / max(0.1, clip.duration))
            clip = concatenate_videoclips([clip] * loops)
        clip = clip.subclip(0, base_duration)
    else:
        img = ImageClip(bg_path).set_duration(base_duration)
        # ken-burns halus
        img = img.fx(vfx.resize, height=1920)
        clip = img.fx(vfx.crop, width=1080, height=1920, x_center=540, y_center=960)

    clip = clip.resize(newsize=(1080,1920))

    # Overlay judul sederhana
    try:
        txt = TextClip(title_text, fontsize=72, font='DejaVu-Sans-Bold', method='caption', size=(1000,None))\
                .set_position(('center', 60)).set_duration(base_duration)
        composed = CompositeVideoClip([clip, txt]).set_audio(audio)
    except Exception:
        # jika font tidak tersedia di runner, jatuhkan tanpa teks
        composed = clip.set_audio(audio)

    composed.write_videofile(out_path, fps=30, codec='libx264', audio_codec='aac', preset='medium', threads=4)

# --------------- YouTube Upload (Scheduled) --------------- #
def upload_youtube_scheduled(file_path: str, title: str, description: str, publish_dt_wib: datetime):
    client_id = os.environ.get('GOOGLE_CLIENT_ID')
    client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
    refresh_token = os.environ.get('GOOGLE_REFRESH_TOKEN')
    if not all([client_id, client_secret, refresh_token]):
        raise RuntimeError('GOOGLE_CLIENT_ID/SECRET/REFRESH_TOKEN belum di-set di Secrets')

    publish_utc = publish_dt_wib.astimezone(pytz.utc)

    creds = Credentials(
        None,
        refresh_token=refresh_token,
        token_uri='https://oauth2.googleapis.com/token',
        client_id=client_id,
        client_secret=client_secret
    )
    yt = build('youtube', 'v3', credentials=creds)

    body = {
        'snippet': {
            'title': title[:100] or 'Shorts',
            'description': description or '',
            'tags': ['motivasi','shorts']
        },
        'status': {
            'privacyStatus': 'private',
            'publishAt': publish_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'selfDeclaredMadeForKids': False
        }
    }

    media = MediaFileUpload(file_path, chunksize=-1, resumable=True, mimetype='video/*')
    request = yt.videos().insert(part=','.join(body.keys()), body=body, media_body=media)

    response = None
    while response is None:
        status, response = request.next_chunk()
    video_id = response.get('id')
    print('YouTube uploaded video id:', video_id)
    return video_id

# --------------- Activepieces Webhook --------------- #
def notify_activepieces(payload: dict):
    base = os.environ.get('AP_BASE_URL')
    if not base:
        print('AP_BASE_URL kosong, lewati notifikasi Activepieces')
        return
    headers = {'Content-Type': 'application/json'}
    secret = os.environ.get('AP_SECRET')
    if secret:
        headers['X-AP-SECRET'] = secret
    try:
        r = requests.post(base, headers=headers, data=json.dumps(payload), timeout=30)
        print('Activepieces status:', r.status_code, r.text[:200])
    except Exception as e:
        print('Activepieces error:', e)

# --------------- Pilih Job dari queue.csv --------------- #
def parse_wib_time(date_str: str, time_str: str) -> datetime:
    hh, mm = (time_str or '07:30').split(':')
    hh, mm = int(hh), int(mm)
    y, m, d = map(int, date_str.split('-'))
    return WIB.localize(datetime(y, m, d, hh, mm))


def pick_job(csv_path: str = 'queue.csv'):
    today = datetime.now(WIB).date()
    now = datetime.now(WIB)
    if not os.path.exists(csv_path):
        print('queue.csv tidak ditemukan')
        return None
    items = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            tgl = (row.get('Tanggal') or '').strip()
            if not tgl:
                continue
            try:
                y, m, d = map(int, tgl.split('-'))
                row_date = datetime(y, m, d).date()
            except Exception:
                continue
            if row_date != today:
                continue
            status = (row.get('Status') or '').strip()
            if status:
                # sudah diposting
                continue
            jam = (row.get('JamWIB') or '07:30').strip()
            try:
                dt = parse_wib_time(tgl, jam)
            except Exception:
                dt = WIB.localize(datetime(today.year, today.month, today.day, 7, 30))
            items.append((dt, row))
    if not items:
        return None
    # pilih yang terdekat dengan waktu sekarang (agar bisa dipanggil 2x/hari)
    items.sort(key=lambda x: abs((x[0]-now).total_seconds()))
    # batas toleransi ±3 jam
    for dt, row in items:
        if abs((dt - now).total_seconds()) <= 3*3600:
            return dt, row
    return None

# --------------- Update status di queue.csv --------------- #
def mark_posted(csv_path: str, row_to_mark: dict, platform: str, note: str = ''):
    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    # cari index baris yang sama (berdasarkan Tanggal+Judul)
    idx = None
    for i, r in enumerate(rows):
        if (r.get('Tanggal','').strip() == row_to_mark.get('Tanggal','').strip() and
            r.get('Judul','').strip() == row_to_mark.get('Judul','').strip()):
            idx = i; break
    if idx is None:
        return
    status_old = rows[idx].get('Status','')
    mark = f"POSTED-{platform}"
    if status_old:
        if mark not in status_old:
            rows[idx]['Status'] = status_old + '|' + mark
    else:
        rows[idx]['Status'] = mark
    if note:
        rows[idx]['Status'] += f"({note})"

    # tulis ulang CSV
    headers = rows[0].keys() if rows else ['Tanggal','Judul','Teks','Hashtag','LinkAffiliate','BG','Status','JamWIB']
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)

# ------------------------------ MAIN ------------------------------ #
if __name__ == '__main__':
    job = pick_job('queue.csv')
    if not job:
        print('Tidak ada job yang cocok saat ini (cek queue.csv)')
        raise SystemExit(0)

    dt_wib, row = job
    title = (row.get('Judul') or 'Shorts').strip()
    text = (row.get('Teks') or '').strip()
    hashtags = (row.get('Hashtag') or '').strip()
    link = (row.get('LinkAffiliate') or '').strip()
    bg = (row.get('BG') or 'assets/bg1.mp4').strip()

    caption = f"{text}

{hashtags}
{link}".strip()

    os.makedirs('out', exist_ok=True)
    os.makedirs('assets', exist_ok=True)

    audio_path = 'out/narasi.mp3'
    video_path = 'out/video.mp4'

    print('TTS...')
    tts_azure(text, audio_path)

    print('Render video...')
    render_video(bg, audio_path, title, video_path, duration_max=60)

    print('Upload YouTube (scheduled)...')
    vid = upload_youtube_scheduled(video_path, title, caption, dt_wib)

    print('Notify Activepieces (opsional)...')
    notify_activepieces({
        'platforms': ['instagram','facebook','threads','tiktok'],
        'title': title,
        'caption': caption,
        # Runner GitHub biasanya sandbox; untuk unggah lintas platform,
        # unggah dulu ke storage publik/Drive lalu pakai URL di flow Activepieces.
        'video_local_path': os.path.abspath(video_path),
        'publish_wib': dt_wib.isoformat()
    })

    try:
        mark_posted('queue.csv', row, platform='YT', note=vid or '')
    except Exception as e:
        print('Gagal update status queue.csv:', e)

    print('DONE')
