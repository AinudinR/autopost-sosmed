#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
autopost.py — End-to-end & robust (WIB-aware)
Baca CSV → (fallback deteksi tanggal/jam) → TTS (Azure) → render 9:16 (MoviePy) → upload YouTube → tandai POSTED → (opsional) kirim Telegram.

ENV yang digunakan:
  - AZURE_SPEECH_KEY
  - AZURE_SPEECH_REGION
  - GOOGLE_CLIENT_ID
  - GOOGLE_CLIENT_SECRET
  - GOOGLE_REFRESH_TOKEN
  - TELEGRAM_BOT_TOKEN        (opsional; aktif kalau --telegram dipakai & env tersedia)
  - TELEGRAM_CHAT_ID          (opsional; ID chat target, mis. -1001234567890)
  - AP_BASE_URL, AP_SECRET    (opsional; untuk webhook notify)

Dependensi:
  pip install moviepy azure-cognitiveservices-speech google-api-python-client google-auth google-auth-oauthlib
"""

from __future__ import annotations
import argparse
import csv
import os
import re
import sys
import json
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

WIB = ZoneInfo("Asia/Jakarta")

# ====== CSV header yang diharapkan + alias ======
EXPECTED_HEADERS = [
    "Tanggal", "Judul", "Teks", "Hashtag",
    "LinkAffiliate", "BG", "Music", "Status", "JamWIB"
]

ALIAS_TO_ID = {
    "date": "Tanggal",
    "title": "Judul",
    "description": "Teks",
    "hashtags": "Hashtag",
    "link": "LinkAffiliate",
    "bg": "BG",
    "music": "Music",
    "status": "Status",
    "time": "JamWIB",
}

# ====== Logging ======
def log(msg: str) -> None:
    now = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[autopost] {now} | {msg}", flush=True)

# ====== Util ======
def strip_all(row: Dict[str, str]) -> Dict[str, str]:
    return {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

def detect_header_by_first_cell(first_line: str) -> bool:
    """
    Jika sel pertama pola YYYY-MM-DD → kemungkinan data (tidak ada header).
    """
    try:
        cells = next(csv.reader([first_line]))
    except Exception:
        return True
    if not cells:
        return True
    first = (cells[0] or "").strip()
    return not bool(re.match(r"^\d{4}-\d{2}-\d{2}$", first))

def sniff_delimiter_and_header(sample_text: str) -> Tuple[str, Optional[bool]]:
    """
    Coba deteksi delimiter & keberadaan header dari cuplikan awal file.
    Kembalikan (delimiter, has_header|None).
    """
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=[",",";","\t","|"])
        delim = dialect.delimiter
    except Exception:
        delim = ","
    has_header = None
    try:
        has_header = csv.Sniffer().has_header(sample_text)
    except Exception:
        pass
    return delim, has_header

def read_queue_rows(csv_path: str) -> Iterable[Dict[str, str]]:
    """
    Baca CSV dengan deteksi delimiter + header.
    Kompatibel CSV tanpa header (baris pertama data) & alias kolom Inggris.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    with open(csv_path, "r", encoding="utf-8") as f:
        sample = f.read(4096)
    delim, sniffer_header = sniff_delimiter_and_header(sample)

    with open(csv_path, newline="", encoding="utf-8") as f:
        first_line = f.readline().rstrip("\n")
        manual_header = detect_header_by_first_cell(first_line)
        f.seek(0)
        # putuskan ada header atau tidak
        has_header = sniffer_header if sniffer_header is not None else manual_header

        reader = csv.DictReader(f, delimiter=delim) if has_header \
                 else csv.DictReader(f, fieldnames=EXPECTED_HEADERS, delimiter=delim)

        for row in reader:
            row = strip_all(row)
            normalized = dict(row)
            for en, idn in ALIAS_TO_ID.items():
                if idn not in normalized and en in normalized and normalized[en] not in (None, ""):
                    normalized[idn] = normalized[en]
            yield normalized

def normalize_row(row: Dict[str, str]) -> Dict[str, str]:
    return {k: row.get(k, "") for k in EXPECTED_HEADERS}

# ====== Fallback cari tanggal/jam di kolom manapun ======
DATE_PATTERNS = [
    r"\b\d{4}-\d{2}-\d{2}\b",   # 2025-08-15
    r"\b\d{4}/\d{2}/\d{2}\b",   # 2025/08/15
    r"\b\d{2}-\d{2}-\d{4}\b",   # 15-08-2025
    r"\b\d{2}/\d{2}/\d{4}\b",   # 15/08/2025
]
TIME_PATTERN = r"\b([01]\d|2[0-3]):[0-5]\d\b"  # HH:MM

def _find_first(patterns, text: str) -> Optional[str]:
    if isinstance(patterns, str):
        m = re.search(patterns, text or "")
        return m.group(0) if m else None
    for pat in patterns:
        m = re.search(pat, text or "")
        if m:
            return m.group(0)
    return None

def smart_pick_date_time(row: dict) -> Tuple[str, str]:
    """Jika 'Tanggal'/'JamWIB' kosong, cari pattern tanggal & jam di nilai kolom lain."""
    tanggal = (row.get("Tanggal") or "").strip()
    jam = (row.get("JamWIB") or "").strip()

    if not tanggal:
        for v in row.values():
            hit = _find_first(DATE_PATTERNS, str(v or ""))
            if hit:
                tanggal = hit
                break

    if not jam:
        for v in row.values():
            hit = _find_first(TIME_PATTERN, str(v or ""))
            if hit:
                jam = hit
                break

    return tanggal, jam

# ====== Parsing tanggal fleksibel ======
def parse_wib_time(tanggal_str: str, jam_str: str) -> datetime:
    """Terima beberapa format umum."""
    tanggal_str = (tanggal_str or "").strip()
    jam_str = (jam_str or "").strip()
    candidates = [
        ("%Y-%m-%d %H:%M", f"{tanggal_str} {jam_str}"),
        ("%Y/%m/%d %H:%M", f"{tanggal_str.replace('-', '/')} {jam_str}"),
        ("%d-%m-%Y %H:%M", f"{tanggal_str} {jam_str}"),
        ("%d/%m/%Y %H:%M", f"{tanggal_str.replace('-', '/')} {jam_str}"),
    ]
    last_err = None
    for fmt, s in candidates:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=WIB)
        except Exception as e:
            last_err = e
    raise last_err or ValueError(f"Format tanggal tidak dikenali: '{tanggal_str}' '{jam_str}'")

# ====== Pemilihan job due ======
@dataclass
class Candidate:
    scheduled_dt: datetime
    row: Dict[str, str]
    lateness_sec: float

def pick_job(csv_path: str, max_late_seconds: int, *, debug: bool = False) -> Optional[Tuple[datetime, Dict[str, str]]]:
    now = datetime.now(WIB)
    rows = list(read_queue_rows(csv_path))
    candidates: List[Candidate] = []

    for idx_raw, raw in enumerate(rows):
        row = normalize_row(raw)
        status = (row.get("Status") or "")
        if status.startswith("POSTED"):
            if debug: log(f"skip[{idx_raw}]: sudah POSTED")
            continue

        tanggal = (row.get("Tanggal") or "").strip()
        jam = (row.get("JamWIB") or "").strip()

        # fallback cari di kolom lain jika kosong
        if not tanggal or not jam:
            tgl_fallback, jam_fallback = smart_pick_date_time(row)
            if not tanggal:
                tanggal = tgl_fallback or ""
            if not jam:
                jam = jam_fallback or ""

        if not tanggal or not jam:
            if debug: log(f"skip[{idx_raw}]: tanggal/jam kosong -> {tanggal} | {jam}")
            continue

        try:
            scheduled_dt = parse_wib_time(tanggal, jam)
        except Exception as e:
            if debug: log(f"skip[{idx_raw}]: parse gagal -> {tanggal} {jam} ({e})")
            continue

        lateness = (now - scheduled_dt).total_seconds()
        if lateness >= 0 and lateness <= max_late_seconds:
            pass
        else:
            if debug:
                if lateness < 0:
                    log(f"skip[{idx_raw}]: belum due ({-int(lateness)} dtk lebih awal)")
                else:
                    log(f"skip[{idx_raw}]: telat {int(lateness)} dtk > max {max_late_seconds}")

        if lateness >= 0 and lateness <= max_late_seconds:
            candidates.append(Candidate(scheduled_dt, row, lateness))

    if not candidates:
        log("Tidak ada jadwal due dalam batas keterlambatan.")
        return None

    candidates.sort(key=lambda c: c.lateness_sec)
    chosen = candidates[0]
    log(f"Terpilih: {chosen.row.get('Judul','(tanpa judul)')} @ {chosen.scheduled_dt.strftime('%Y-%m-%d %H:%M %Z')} | telat {int(chosen.lateness_sec)} dtk")
    return chosen.scheduled_dt, chosen.row

# ====== Update CSV (tulis dengan header baku) ======
def _sniff_delim_for_write(csv_path: str) -> str:
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            sample = f.read(4096)
        delim, _ = sniff_delimiter_and_header(sample)
        return delim or ","
    except Exception:
        return ","

def write_rows_with_header(csv_path: str, rows: List[Dict[str, str]]) -> None:
    delim = _sniff_delim_for_write(csv_path)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EXPECTED_HEADERS, delimiter=delim)
        w.writeheader()
        for r in rows:
            w.writerow(normalize_row(r))

def mark_posted(csv_path: str, target_row: Dict[str, str], platform: str, note: str = "") -> None:
    rows = list(read_queue_rows(csv_path))
    if not rows:
        log("CSV kosong saat mark_posted.")
        return
    tgt = normalize_row(target_row)
    tgt_tgl = (tgt.get("Tanggal") or "").strip()
    tgt_judul = (tgt.get("Judul") or "").strip()
    idx = -1
    for i, raw in enumerate(rows):
        r = normalize_row(raw)
        if (r.get("Tanggal","").strip() == tgt_tgl) and (r.get("Judul","").strip() == tgt_judul):
            idx = i
            break
    if idx == -1:
        log("Gagal menemukan baris cocok untuk POSTED (cocokkan Tanggal & Judul).")
        return
    mark = f"POSTED-{platform}" + (f"({note})" if note else "")
    old = rows[idx].get("Status", "").strip()
    rows[idx]["Status"] = old if old and mark in old else (mark if not old else old + "|" + mark)
    write_rows_with_header(csv_path, rows)
    log(f"Ditandai POSTED: {rows[idx].get('Judul','(tanpa judul)')} -> {rows[idx]['Status']}")

# ====== RENDER PIPELINE ======
def tts_azure(text: str, out_wav: str, voice: str = "id-ID-ArdiNeural") -> None:
    import azure.cognitiveservices.speech as speechsdk
    key = os.environ.get("AZURE_SPEECH_KEY", "").strip()
    region = os.environ.get("AZURE_SPEECH_REGION", "").strip()
    if not key or not region:
        raise RuntimeError("AZURE_SPEECH_KEY/REGION tidak ditemukan di env.")
    speech_config = speechsdk.SpeechConfig(subscription=key, region=region)
    speech_config.speech_synthesis_voice_name = voice
    audio_config = speechsdk.audio.AudioOutputConfig(filename=out_wav)
    synthesizer = speechsdk.SpeechSynthesizer(speech_config=speech_config, audio_config=audio_config)
    result = synthesizer.speak_text_async(text).get()
    if result.reason != speechsdk.ResultReason.SynthesizingAudioCompleted:
        raise RuntimeError(f"TTS gagal: {result.reason}")

def sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w\s-]", "", name, flags=re.UNICODE)
    name = re.sub(r"\s+", "_", name).strip("_")
    return name[:80] if len(name) > 80 else name

def wrap_text(text: str, width: int = 28) -> str:
    words = text.split()
    lines = []
    cur = []
    count = 0
    for w in words:
        add = len(w) + (1 if cur else 0)
        if count + add > width:
            lines.append(" ".join(cur))
            cur = [w]
            count = len(w)
        else:
            cur.append(w)
            count += add
    if cur:
        lines.append(" ".join(cur))
    return "\n".join(lines)

def render_video(row: Dict[str, str], out_dir: str = "out") -> str:
    from moviepy.editor import (
        VideoFileClip, AudioFileClip, CompositeAudioClip, CompositeVideoClip,
        TextClip, ColorClip
    )
    os.makedirs(out_dir, exist_ok=True)

    title = row.get("Judul") or "Tanpa Judul"
    desc  = row.get("Teks") or ""
    bg_path = row.get("BG") or ""
    music_path = row.get("Music") or ""

    # 9:16 1080x1920
    W, H = 1080, 1920

    # Background
    bg_clip = None
    if bg_path and os.path.exists(bg_path):
        try:
            bg_clip = VideoFileClip(bg_path).without_audio()
            bg_clip = bg_clip.resize(height=H)
            if bg_clip.w < W:
                bg_clip = bg_clip.resize(width=W)
            bg_clip = bg_clip.crop(width=W, height=H, x_center=bg_clip.w/2, y_center=bg_clip.h/2)
        except Exception as e:
            log(f"BG video gagal dibuka: {e}")
    if bg_clip is None:
        bg_clip = ColorClip(size=(W, H), color=(10, 10, 10)).set_duration(60)

    # TTS
    tmpdir = tempfile.mkdtemp(prefix="autopost_")
    tts_wav = os.path.join(tmpdir, "tts.wav")
    tts_azure(desc or title, tts_wav)
    voice = AudioFileClip(tts_wav)

    # Durasi minimal
    base_dur = max(voice.duration + 0.5, 10.0)
    bg_clip = bg_clip.set_duration(base_dur)

    # Music (opsional)
    music_clip = None
    if music_path and os.path.exists(music_path):
        try:
            music_clip = AudioFileClip(music_path).volumex(0.08).set_duration(base_dur)
        except Exception as e:
            log(f"Music gagal dibuka: {e}")
            music_clip = None

    # Audio final
    if music_clip is not None:
        audio = CompositeAudioClip([music_clip, voice.volumex(1.0)])
    else:
        audio = voice
    bg_clip = bg_clip.set_audio(audio)

    # Overlay Title + Desc
    wrapped = wrap_text(desc if desc else title, width=28)
    try:
        title_clip = TextClip(title, fontsize=72, font="Arial-Bold", color="white",
                              stroke_color="black", stroke_width=2, method="caption",
                              size=(W-120, None))
    except Exception:
        title_clip = TextClip(title, fontsize=72, color="white")
    title_clip = title_clip.set_position(("center", 80)).set_duration(base_dur)

    try:
        body_clip = TextClip(wrapped, fontsize=54, font="Arial", color="white",
                             stroke_color="black", stroke_width=1, method="caption",
                             size=(W-140, None), align="West")
    except Exception:
        body_clip = TextClip(wrapped, fontsize=54, color="white")
    body_clip = body_clip.set_position(("center", 240)).set_duration(base_dur)

    final = CompositeVideoClip([bg_clip, title_clip, body_clip], size=(W, H)).set_duration(base_dur)

    out_name = f"{sanitize_filename(row.get('Tanggal',''))}_{sanitize_filename(title)}.mp4"
    out_path = os.path.join(out_dir, out_name)
    log(f"Render: {out_path}")
    final.write_videofile(out_path, fps=30, codec="libx264", audio_codec="aac", threads=4, verbose=False, logger=None)

    try:
        voice.close()
    except Exception:
        pass
    return out_path

# ====== YouTube Upload ======
def build_youtube_service() -> "googleapiclient.discovery.Resource":
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    client_id = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
    refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN", "").strip()
    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError("Env GOOGLE_CLIENT_ID/SECRET/REFRESH_TOKEN kosong.")

    token_uri = "https://oauth2.googleapis.com/token"
    creds = Credentials(
        None,
        refresh_token=refresh_token,
        token_uri=token_uri,
        client_id=client_id,
        client_secret=client_secret,
        scopes=[
            "https://www.googleapis.com/auth/youtube.upload",
            "https://www.googleapis.com/auth/youtube",
        ],
    )
    return build("youtube", "v3", credentials=creds, cache_discovery=False)

def parse_tags(hashtags: str) -> List[str]:
    tags = []
    for tok in re.split(r"[,\s]+", hashtags or ""):
        tok = tok.strip()
        if not tok:
            continue
        if tok.startswith("#"):
            tok = tok[1:]
        if tok:
            tags.append(tok[:30])
    return tags[:10]

def upload_youtube_short(file_path: str, title: str, description: str, hashtags: str) -> str:
    from googleapiclient.http import MediaFileUpload
    yt = build_youtube_service()

    desc_full = description.strip()
    if hashtags:
        desc_full = (desc_full + "\n\n" + hashtags).strip()

    body = {
        "snippet": {
            "title": title[:100],
            "description": desc_full[:4900],
            "tags": parse_tags(hashtags),
            "categoryId": "22",
        },
        "status": {
            "privacyStatus": "public",
            "selfDeclaredMadeForKids": False,
        },
    }
    media = MediaFileUpload(file_path, mimetype="video/mp4", chunksize=-1, resumable=True)

    log("Upload ke YouTube...")
    request = yt.videos().insert(part="snippet,status", body=body, media_body=media)
    response = None
    while response is None:
        status, response = request.next_chunk()
    video_id = response.get("id")
    if not video_id:
        raise RuntimeError(f"Gagal upload: response={response}")
    log(f"Upload sukses: https://youtube.com/watch?v={video_id}")
    return video_id

# ====== Telegram Notify (opsional) ======
def telegram_send_message(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return
    try:
        import urllib.request, urllib.parse
        api = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
        with urllib.request.urlopen(api, data=data, timeout=30) as resp:
            _ = resp.read()
        log("Telegram: pesan terkirim")
    except Exception as e:
        log(f"Telegram: kirim pesan gagal ({e})")

def telegram_send_video(file_path: str, caption: str = "") -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return
    # Batas ukuran file untuk bot Telegram (umum): ~50 MB (bervariasi tergantung update).
    try:
        size = os.path.getsize(file_path)
    except Exception:
        size = 0
    if size > 49 * 1024 * 1024:  # >49MB: kirim teks saja agar aman
        telegram_send_message(caption or os.path.basename(file_path))
        return
    try:
        import urllib.request
        boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"
        data = []
        def add_field(name, value):
            data.append(f"--{boundary}\r\n".encode())
            data.append(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
            data.append(f"{value}\r\n".encode())
        def add_file(name, filename, content):
            data.append(f"--{boundary}\r\n".encode())
            data.append(f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'.encode())
            data.append(b"Content-Type: video/mp4\r\n\r\n")
            data.append(content)
            data.append(b"\r\n")

        add_field("chat_id", chat_id)
        if caption:
            add_field("caption", caption)
        with open(file_path, "rb") as f:
            content = f.read()
        add_file("video", os.path.basename(file_path), content)
        data.append(f"--{boundary}--\r\n".encode())
        body = b"".join(data)

        req = urllib.request.Request(
            url=f"https://api.telegram.org/bot{token}/sendVideo",
            data=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            _ = resp.read()
        log("Telegram: video terkirim")
    except Exception as e:
        log(f"Telegram: kirim video gagal ({e})")

# ====== Optional backend notify ======
def notify_backend(payload: dict) -> None:
    base = os.environ.get("AP_BASE_URL", "").strip()
    secret = os.environ.get("AP_SECRET", "").strip()
    if not base or not secret:
        return
    try:
        import urllib.request
        req = urllib.request.Request(
            url=base.rstrip("/") + "/notify",
            data=json.dumps({**payload, "secret": secret}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            log(f"Notify status: {resp.status}")
    except Exception as e:
        log(f"Notify gagal: {e}")

# ====== MAIN ======
def main() -> None:
    p = argparse.ArgumentParser(description="Autopost end-to-end (WIB-aware & robust CSV)")
    p.add_argument("--csv", default="queue.csv")
    p.add_argument("--platform", default="YT", help="Label platform untuk Status")
    p.add_argument("--max-late", type=float, default=24.0, help="Catch-up (jam)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-upload", action="store_true", help="Hanya render, tidak upload")
    p.add_argument("--voice", default="id-ID-ArdiNeural")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--telegram", action="store_true", help="Kirim notifikasi Telegram bila env tersedia")
    args = p.parse_args()

    csv_path = args.csv
    max_late_seconds = int(args.max_late * 3600)

    log(f"Start | CSV={csv_path} | platform={args.platform} | max_late={args.max_late}h | dry={args.dry_run} | skip_upload={args.skip_upload}")
    job = pick_job(csv_path, max_late_seconds=max_late_seconds, debug=args.debug)
    if not job:
        log("Selesai: tidak ada job due.")
        return

    scheduled_dt, row = job
    row = normalize_row(row)
    title = row.get("Judul") or "Tanpa Judul"
    desc = row.get("Teks") or ""
    hashtags = row.get("Hashtag") or ""
    link = row.get("LinkAffiliate") or ""
    if link and link not in desc:
        desc = (desc + "\n\n" + link).strip()

    # Render
    try:
        out_path = render_video(row)
    except Exception as e:
        log(f"RENDER ERROR: {e}")
        raise

    video_id = ""
    if not args.dry_run and not args.skip_upload:
        try:
            video_id = upload_youtube_short(out_path, title=title, description=desc, hashtags=hashtags)
        except Exception as e:
            log(f"UPLOAD ERROR: {e}")
            raise
    else:
        log("[SKIP UPLOAD] Melewati upload (dry-run/skip-upload).")

    # Mark POSTED
    if not args.dry_run:
        note = video_id or os.path.basename(out_path)
        try:
            mark_posted(csv_path, row, platform=args.platform, note=note)
        except Exception as e:
            log(f"MARK POSTED ERROR: {e}")
    else:
        log("[DRY RUN] Tidak menandai POSTED.")

    # Telegram (opsional)
    if args.telegram:
        yt_link = f"https://youtube.com/watch?v={video_id}" if video_id else ""
        caption = f"{title}\n{yt_link}".strip()
        # kirim video bila kecil, kalau terlalu besar kirim teks saja
        try:
            telegram_send_video(out_path, caption=caption)
        except Exception as e:
            log(f"Telegram video gagal ({e}), kirim pesan saja.")
            telegram_send_message(caption)

    # Optional backend notify
    notify_backend({
        "title": title,
        "video_id": video_id,
        "scheduled": scheduled_dt.isoformat(),
        "output": out_path,
    })

    log("Selesai.")

if __name__ == "__main__":
    sys.exit(main())
