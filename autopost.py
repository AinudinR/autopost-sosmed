#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
autopost.py (patched, robust)

Fungsi utama:
- Membaca queue.csv yang bisa TANPA HEADER atau DENGAN HEADER.
- Kompatibel dua skema nama kolom:
    Indonesia: Tanggal, Judul, Teks, Hashtag, LinkAffiliate, BG, Music, Status, JamWIB
    Inggris  : date, title, description, hashtags, link, bg, music, status, time
- Timezone WIB (Asia/Jakarta) konsisten.
- "Catch-up": mengeksekusi jadwal yang SUDAH JATUH TEMPO (<= now) walau telat (default 24 jam).
- Menandai baris yang sukses dengan awalan "POSTED-<PLATFORM>" di kolom Status.
- CLI argumen:
    --csv PATH         : path CSV (default: queue.csv)
    --platform NAME    : label platform untuk penandaan status, default "RUN"
    --note TEXT        : catatan tambahan di status, mis. ID video
    --max-late HOURS   : batas keterlambatan (jam), default 24
    --dry-run          : tidak menandai POSTED, hanya cetak rencana eksekusi

Contoh pakai:
    python autopost.py --platform YT
    python autopost.py --csv data/queue.csv --platform TIKTOK --dry-run
"""

from __future__ import annotations
import argparse
import csv
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:
    raise SystemExit("Python 3.9+ with zoneinfo required.")

WIB = ZoneInfo("Asia/Jakarta")

# Header baku yang selalu kita tulis ulang saat update
EXPECTED_HEADERS = [
    "Tanggal", "Judul", "Teks", "Hashtag",
    "LinkAffiliate", "BG", "Music", "Status", "JamWIB"
]

# Pemetaan alias (Inggris -> Indonesia) untuk normalisasi internal
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


def log(msg: str) -> None:
    now = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[autopost] {now} | {msg}")


def strip_all(row: Dict[str, str]) -> Dict[str, str]:
    return {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}


def detect_header(first_line: str) -> bool:
    """
    Deteksi apakah baris pertama adalah data atau header.
    Jika sel pertama adalah pola YYYY-MM-DD -> kemungkinan data, bukan header.
    """
    try:
        cells = next(csv.reader([first_line]))
    except Exception:
        return True  # fallback: anggap header

    if not cells:
        return True

    first = (cells[0] or "").strip()
    return not bool(re.match(r"^\d{4}-\d{2}-\d{2}$", first))


def read_queue_rows(csv_path: str) -> Iterable[Dict[str, str]]:
    """
    Iterator dict row yang siap dipakai.
    - Jika tidak ada header, gunakan EXPECTED_HEADERS sebagai fieldnames.
    - Jika ada header, pakai header tersebut, tapi tetap normalisasi alias untuk downstream.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    with open(csv_path, newline="", encoding="utf-8") as f:
        peek = f.readline().rstrip("\n")
        has_header = detect_header(peek)
        f.seek(0)

        if has_header:
            reader = csv.DictReader(f)
        else:
            reader = csv.DictReader(f, fieldnames=EXPECTED_HEADERS)

        for row in reader:
            row = strip_all(row)
            # Normalisasi: tambahkan key Indonesia jika yang tersedia adalah alias Inggris
            normalized = dict(row)
            for en, idn in ALIAS_TO_ID.items():
                if idn not in normalized and en in normalized and normalized[en] not in (None, ""):
                    normalized[idn] = normalized[en]
            yield normalized


def normalize_row(row: Dict[str, str]) -> Dict[str, str]:
    """
    Pastikan row memiliki semua kunci EXPECTED_HEADERS (bisa kosong).
    """
    out = {k: row.get(k, "") for k in EXPECTED_HEADERS}
    return out


def parse_wib_time(tanggal_str: str, jam_str: str) -> datetime:
    """
    Parse tanggal (YYYY-MM-DD) dan jam (HH:MM) → datetime timezone-aware (WIB).
    """
    tanggal_str = (tanggal_str or "").strip()
    jam_str = (jam_str or "").strip()
    dt = datetime.strptime(f"{tanggal_str} {jam_str}", "%Y-%m-%d %H:%M")
    return dt.replace(tzinfo=WIB)


@dataclass
class Candidate:
    scheduled_dt: datetime
    row: Dict[str, str]
    lateness_sec: float


def pick_job(csv_path: str, max_late_seconds: int) -> Optional[Tuple[datetime, Dict[str, str]]]:
    """
    Pilih 1 job yang due (scheduled_dt <= now), belum POSTED,
    dengan lateness <= max_late_seconds. Ambil yang lateness terkecil (paling dekat).
    """
    now = datetime.now(WIB)
    rows = list(read_queue_rows(csv_path))
    candidates: List[Candidate] = []

    for raw in rows:
        row = normalize_row(raw)
        status = (row.get("Status") or "")
        if status.startswith("POSTED"):
            continue

        tanggal = row.get("Tanggal", "")
        jam = row.get("JamWIB", "")
        if not tanggal or not jam:
            continue

        try:
            scheduled_dt = parse_wib_time(tanggal, jam)
        except Exception:
            continue

        lateness = (now - scheduled_dt).total_seconds()
        # due jika <= now dan tidak melebihi batas keterlambatan
        if lateness >= 0 and lateness <= max_late_seconds:
            candidates.append(Candidate(scheduled_dt, row, lateness))

    if not candidates:
        log("Tidak ada jadwal due dalam batas keterlambatan.")
        # Tambahkan sedikit debug untuk membantu diagnosa
        if rows:
            upcoming = []
            past = []
            for raw in rows:
                row = normalize_row(raw)
                tanggal = row.get("Tanggal", "")
                jam = row.get("JamWIB", "")
                if not tanggal or not jam:
                    continue
                try:
                    dt = parse_wib_time(tanggal, jam)
                except Exception:
                    continue
                diff = (dt - now).total_seconds()
                if diff >= 0:
                    upcoming.append((dt, row.get("Judul", "")))
                else:
                    past.append((dt, row.get("Judul", "")))
            if upcoming:
                soon = min(upcoming, key=lambda x: x[0])
                log(f"Jadwal terdekat di masa depan: {soon[0].strftime('%Y-%m-%d %H:%M')} | {soon[1]}")
            if past:
                last = max(past, key=lambda x: x[0])
                log(f"Jadwal terbaru yang terlewat: {last[0].strftime('%Y-%m-%d %H:%M')} | {last[1]}")
        return None

    candidates.sort(key=lambda c: c.lateness_sec)
    chosen = candidates[0]
    log(f"Terpilih: {chosen.row.get('Judul','(tanpa judul)')} @ {chosen.scheduled_dt.strftime('%Y-%m-%d %H:%M %Z')} | telat {int(chosen.lateness_sec)} dtk")
    return chosen.scheduled_dt, chosen.row


def write_rows_with_header(csv_path: str, rows: List[Dict[str, str]]) -> None:
    """
    Tulis ulang CSV memakai header EXPECTED_HEADERS.
    """
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EXPECTED_HEADERS)
        w.writeheader()
        for r in rows:
            w.writerow(normalize_row(r))


def mark_posted(csv_path: str, target_row: Dict[str, str], platform: str, note: str = "") -> None:
    """
    Tandai baris pada CSV sebagai POSTED-<platform>(note).
    Pencocokan dilakukan via (Tanggal, Judul) atau (Tanggal, title) jika ada alias.
    """
    rows = list(read_queue_rows(csv_path))
    if not rows:
        log("CSV kosong saat mark_posted.")
        return

    # normalisasi target
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
        log("Gagal menemukan baris yang cocok untuk ditandai (cocokkan Tanggal & Judul).")
        return

    mark = f"POSTED-{platform}" + (f"({note})" if note else "")
    old = rows[idx].get("Status", "").strip()
    if not old:
        rows[idx]["Status"] = mark
    else:
        # hindari duplikasi mark yang sama
        rows[idx]["Status"] = old if mark in old else (old + "|" + mark)

    write_rows_with_header(csv_path, rows)
    log(f"Ditandai POSTED: {rows[idx].get('Judul','(tanpa judul)')} -> {rows[idx]['Status']}")


def simulate_publish(row: Dict[str, str], platform: str) -> str:
    """
    Placeholder proses publish. Di sini kamu hubungkan ke renderer/uploader.
    Kembalikan string 'note' (mis. videoId) untuk dicatat di Status.
    """
    # Contoh ambil nilai yang sering dipakai
    judul = row.get("Judul") or ""
    teks = row.get("Teks") or ""
    hashtag = row.get("Hashtag") or ""
    link = row.get("LinkAffiliate") or ""
    bg = row.get("BG") or ""
    music = row.get("Music") or ""

    log(f"[SIMULATE] Publish ke {platform}: '{judul}'")
    log(f"[SIMULATE] BG={bg} | Music={music}")
    log(f"[SIMULATE] Link={link}")
    # TODO: panggil pipeline asli di sini
    # Kembalikan ID/URL sebagai catatan
    return "SIM-OK"


def main() -> None:
    ap = argparse.ArgumentParser(description="Autopost runner (WIB-aware, catch-up).")
    ap.add_argument("--csv", default="queue.csv", help="Path ke CSV antrian (default: queue.csv)")
    ap.add_argument("--platform", default="RUN", help="Label platform untuk Status (default: RUN)")
    ap.add_argument("--note", default="", help="Catatan tambahan untuk Status (mis. videoId)")
    ap.add_argument("--max-late", type=float, default=24.0, help="Batas catch-up dalam jam (default: 24)")
    ap.add_argument("--dry-run", action="store_true", help="Jangan menandai POSTED, hanya simulasi")
    args = ap.parse_args()

    csv_path = args.csv
    platform = args.platform.strip() or "RUN"
    note = args.note.strip()
    max_late_seconds = int(args.max_late * 3600)

    log(f"Start | CSV={csv_path} | PLATFORM={platform} | MAX_LATE={args.max_late}h | DRY_RUN={args.dry_run}")
    now = datetime.now(WIB)
    log(f"NOW (WIB) = {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    job = pick_job(csv_path, max_late_seconds=max_late_seconds)
    if not job:
        log("Selesai: tidak ada job due.")
        return

    scheduled_dt, row = job
    # Eksekusi/publish sebenarnya
    try:
        note_ret = simulate_publish(row, platform=platform)
        final_note = note_ret or note
        if not args.dry_run:
            mark_posted(csv_path, row, platform=platform, note=final_note)
        else:
            log("[DRY-RUN] Lewati penandaan POSTED")
    except Exception as e:
        log(f"ERROR saat publish: {e}")
        raise

    log("Selesai.")
    

if __name__ == "__main__":
    main()
