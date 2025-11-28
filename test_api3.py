# test_api2.py
import os
import sqlite3
import requests
from dotenv import load_dotenv

load_dotenv()

# -------------------------------
# CONFIGURATION
# -------------------------------
DB_PATH = "data/processed_music_weather.db"
SOUNDCHART_API_KEY = os.getenv("SOUNDCHART_API_KEY")
SOUNDCHART_APP_ID = os.getenv("SOUNDCHART_APP_ID")
HEADERS = {
    "x-app-id": SOUNDCHART_APP_ID,
    "x-api-key": SOUNDCHART_API_KEY
}
SEARCH_LIMIT = 1  # On récupère le premier résultat correspondant

# -------------------------------
# CONNEXION À LA BASE
# -------------------------------
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Récupérer un petit échantillon pour test
cursor.execute("SELECT track_name, artist_name FROM processed_tracks LIMIT 3")
tracks = cursor.fetchall()
print(f"🔍 {len(tracks)} tracks récupérées depuis processed_tracks pour test")

# -------------------------------
# BOUCLE D'ENRICHISSEMENT
# -------------------------------
for track_name, artist_name in tracks:
    try:
        # URL de recherche
        search_url = f"https://customer.api.soundcharts.com/api/v2/song/search/{track_name}"
        params = {"offset": 0, "limit": SEARCH_LIMIT, "artist": artist_name}
        
        response = requests.get(search_url, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
        
        items = data.get("items", [])
        if not items:
            print(f"⚠️ Track non trouvée: {track_name} - {artist_name}")
            continue
        
        # On prend le premier item
        uuid = items[0]["uuid"]
        print(f"✅ UUID trouvé pour {track_name} - {artist_name}: {uuid}")
        
        # Récupérer les infos détaillées
        song_url = f"https://customer.api.soundcharts.com/api/v2.25/song/{uuid}"
        song_response = requests.get(song_url, headers=HEADERS)
        song_response.raise_for_status()
        song_data = song_response.json()
        
        print(f"🎵 Données Soundcharts pour {track_name} - {artist_name}:")
        print(song_data)
        
    except requests.exceptions.HTTPError as e:
        print(f"❌ Erreur Soundcharts {track_name} - {artist_name}: {e}")
    except Exception as e:
        print(f"❌ Autre erreur {track_name} - {artist_name}: {e}")

conn.close()
