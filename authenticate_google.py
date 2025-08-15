import os
import google.oauth2.credentials
import google_auth_oauthlib.flow

# Scope ini meminta izin untuk mengelola (termasuk mengunggah) video YouTube.
# Ini adalah bagian yang paling penting untuk memperbaiki error 'invalid_scope'.
SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'
CLIENT_SECRETS_FILE = 'client_secret.json'

def get_credentials():
    """
    Menjalankan alur otentikasi interaktif untuk mendapatkan kredensial pengguna,
    lalu mencetak REFRESH_TOKEN untuk digunakan sebagai environment variable.
    """
    if not os.path.exists(CLIENT_SECRETS_FILE):
        print(f"ERROR: File '{CLIENT_SECRETS_FILE}' tidak ditemukan.")
        print("Pastikan Anda sudah mengunduh file JSON kredensial dari Google Cloud Console")
        print("dan menyimpannya di folder yang sama dengan nama tersebut.")
        return None

    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        CLIENT_SECRETS_FILE, SCOPES)
    
    # Jalankan alur otentikasi di browser
    credentials = flow.run_console()
    
    # Cetak refresh_token yang didapat.
    # Token ini yang harus Anda simpan di GitHub Secrets.
    print("\n" + "="*60)
    print("OTENTIKASI BERHASIL!")
    print("Simpan nilai REFRESH_TOKEN di bawah ini ke GitHub Secrets Anda.")
    print("="*60 + "\n")
    print(f"REFRESH_TOKEN: {credentials.refresh_token}")
    print("\n" + "="*60)
    
    # Simpan token lengkap ke file token.json untuk penggunaan di masa depan (opsional)
    with open('token.json', 'w') as token_file:
        token_file.write(credentials.to_json())
    print("\nKredensial lengkap juga telah disimpan ke 'token.json'")

if __name__ == '__main__':
    get_credentials()
