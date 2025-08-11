    import os
    from datetime import datetime
    import pytz
    
    # Tes 1: Mencetak teks sederhana
    print("--- SCRIPT TES DIAGNOSTIK DIMULAI ---")
    
    # Tes 2: Memeriksa apakah library 'pytz' berfungsi
    try:
        WIB = pytz.timezone('Asia/Jakarta')
        print(f"Berhasil membuat objek timezone. Waktu sekarang: {datetime.now(WIB)}")
    except Exception as e:
        print(f"ERROR: Gagal membuat objek timezone: {e}")
    
    # Tes 3: Memeriksa apakah 'secrets' bisa dibaca
    print("Mencoba membaca environment variable (secret)...")
    google_secret = os.getenv("GOOGLE_CLIENT_ID")
    if google_secret:
        print("SUKSES: Berhasil membaca GOOGLE_CLIENT_ID dari secrets.")
    else:
        print("PERINGATAN: Tidak bisa membaca GOOGLE_CLIENT_ID dari secrets.")
    
    print("--- SCRIPT TES DIAGNOSTIK SELESAI ---")
    
