# test_api2.py
import os
import json
import sqlite3
from dotenv import load_dotenv
import requests
from datetime import datetime
import unidecode

load_dotenv()

DB_PATH = "data/processed_music_weather.db"
API_KEY = os.getenv("SOUNDCHART_API_KEY")
APP_ID = os.getenv("SOUNDCHART_APP_ID")  # si nécessaire

HEADERS = {
    "x-app-id": APP_ID,
    "x-api-key": API_KEY,
    "Content-Type": "application/json"
}




def get_tracks_from_db(limit=5):
    """Récupère quelques tracks existants pour tester l'API"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT track_name, artist_name
        FROM processed_tracks
        ORDER BY processed_at DESC
        LIMIT {limit}
    """)
    tracks = cursor.fetchall()
    conn.close()
    return tracks


def enrich_with_soundcharts(track_name, artist_name):
    """Appel API Soundcharts pour récupérer uuid et détails"""
    try:
        # 1️⃣ Rechercher l'UUID
        track_name_clean = unidecode.unidecode(track_name)
        artist_name_clean = unidecode.unidecode(artist_name)
        params = f"{track_name_clean}"
        url = f"https://customer.api.soundcharts.com/api/v2/song/search/{params}?offset=0&limit=1"
        response = requests.get(url, headers=HEADERS)
        data = response.json()
        print(data)
        r1 = requests.get(url, headers=HEADERS, params=params, timeout=10)
        r1.raise_for_status()
        data1 = r1.json()
        if data.get('items'):
            uuid = data['items'][0]['uuid']
        else:
            # Track non trouvée
            uuid = None
        print(data1)
        print(uuid)
        if not uuid:
            print(f"⚠️ UUID non trouvé pour {track_name} - {track_name_clean} - {artist_name} - {artist_name_clean}")
            return None

        # 2️⃣ Obtenir les détails via UUID
        detail_url = f"https://customer.api.soundcharts.com/api/v2.25/song/{uuid}"
        r2 = requests.get(detail_url, headers=HEADERS, timeout=10)
        r2.raise_for_status()
        details = r2.json()

        # 3️⃣ Sauvegarder en base
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS soundcharts_tracks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                track_name TEXT NOT NULL,
                artist_name TEXT NOT NULL,
                uuid TEXT UNIQUE,
                soundcharts_json TEXT,
                enriched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(track_name, artist_name)
            )
        """)
        cursor.execute("""
            INSERT OR REPLACE INTO soundcharts_tracks
            (track_name, artist_name, uuid, soundcharts_json)
            VALUES (?, ?, ?, ?)
        """, (track_name, artist_name, uuid, json.dumps(details)))
        conn.commit()
        conn.close()

        print(f"✅ Track enrichie: {track_name} - {artist_name} (UUID: {uuid})")
        return details

    except Exception as e:
        print(f"❌ Erreur Soundcharts {track_name} - {artist_name}: {e}")
        return None


if __name__ == "__main__":
    tracks = get_tracks_from_db(limit=3)
    print(f"🔍 {len(tracks)} tracks récupérées depuis processed_tracks pour test")

    for track_name, artist_name in tracks:
        details = enrich_with_soundcharts(track_name, artist_name)
        if details:
            print(f"🎵 Détails Soundcharts pour {track_name}: {json.dumps(details, indent=2)[:300]}...")
