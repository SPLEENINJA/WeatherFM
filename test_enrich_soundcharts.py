import os
import sqlite3  
import requests
import json
from dotenv import load_dotenv

def enrich_with_soundcharts(self):
            """
            Enrichit les tracks de processed_tracks avec Soundcharts.
            Stocke les données dans soundcharts_tracks.
            """
            load_dotenv()

            api_key = os.getenv("SOUNDCHART_API_KEY")
            app_id = os.getenv("SOUNDCHART_APP_ID")

            if not api_key:
                raise ValueError("SOUNDCHART_API_KEY manquant dans .env")

            HEADERS = {
                "x-app-id": app_id,
                "x-api-key": api_key
            }

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Récupérer TOUS les tracks distincts
            cursor.execute("""
                SELECT DISTINCT track_name, artist_name
                FROM processed_tracks
            """)
            tracks = cursor.fetchall()

            print(f"🔍 {len(tracks)} tracks à enrichir")

            enriched_tracks = []

            for track_name, artist_name in tracks:
                try:
                    # --------------------------------------------------------------
                    # 1) Recherche UUID Song (Soundcharts Search API)
                    # --------------------------------------------------------------
                    search_url = f"https://customer.api.soundcharts.com/api/v2/song/search/{track_name}"

                    params = {
                        "offset": 0,
                        "limit": 1,
                        "artist": artist_name
                    }

                    r = requests.get(search_url, headers=HEADERS, params=params)
                    r.raise_for_status()
                    search_json = r.json()

                    items = search_json.get("items", [])
                    if not items:
                        print(f"⚠️ Introuvable sur Soundcharts : {track_name} - {artist_name}")
                        continue

                    uuid = items[0]["uuid"]
                    print(f"🎵 {track_name} - {artist_name} → UUID = {uuid}")

                    # --------------------------------------------------------------
                    # 2) Récupération détails complets (v2.25)
                    # --------------------------------------------------------------
                    detail_url = f"https://customer.api.soundcharts.com/api/v2.25/song/{uuid}"

                    r2 = requests.get(detail_url, headers=HEADERS)
                    r2.raise_for_status()
                    obj = r2.json()

                    song_obj = obj.get("object", {})

                    audio = song_obj.get("audio", {})

                    cursor.execute("""
                        INSERT OR REPLACE INTO soundcharts_tracks (
                            track_name, artist_name, uuid,
                            release_date, image_url, credit_name,
                            isrc, isrc_country_code, isrc_country_name,
                            genres, labels,
                            acousticness, danceability, energy, instrumentalness, key, liveness,
                            loudness, mode, speechiness, tempo, time_signature, valence
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        track_name,
                        artist_name,
                        song_obj.get("uuid"),
                        song_obj.get("releaseDate"),
                        song_obj.get("imageUrl"),
                        song_obj.get("creditName"),
                        song_obj.get("isrc", {}).get("value"),
                        song_obj.get("isrc", {}).get("countryCode"),
                        song_obj.get("isrc", {}).get("countryName"),
                        json.dumps(song_obj.get("genres", [])),
                        json.dumps(song_obj.get("labels", [])),
                        audio.get("acousticness"),
                        audio.get("danceability"),
                        audio.get("energy"),
                        audio.get("instrumentalness"),
                        audio.get("key"),
                        audio.get("liveness"),
                        audio.get("loudness"),
                        audio.get("mode"),
                        audio.get("speechiness"),
                        audio.get("tempo"),
                        audio.get("timeSignature"),
                        audio.get("valence")
                    ))
                except requests.exceptions.HTTPError as e:
                    print(f"❌ HTTP Error {track_name} - {artist_name}: {e}")
                except Exception as e:
                    print(f"❌ Erreur pour {track_name} - {artist_name}: {e}")

            conn.commit()
            conn.close()

            print(f"🎉 Enrichissement terminé → {len(enriched_tracks)} tracks enrichis")
            return enriched_tracks
