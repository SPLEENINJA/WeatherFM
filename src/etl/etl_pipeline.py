# src/etl/etl_pipeline.py 
import json
import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Optional
import logging
import pandas as pd
import requests
from dotenv import load_dotenv

class ETLPipeline:
    """
    Pipeline ETL qui transforme les données brutes en données structurées
    """
    
    def __init__(self, db_path: str = '/data/processed_music_weather.db'):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self._init_processed_db()

    def _init_processed_db(self):
        os.makedirs('data', exist_ok=True)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # processed_tracks
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_tracks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                city TEXT NOT NULL,
                country TEXT NOT NULL,
                track_name TEXT NOT NULL,
                artist_name TEXT NOT NULL,
                listeners INTEGER,
                playcount INTEGER,
                rank_position INTEGER,
                weather_condition TEXT,
                weather_description TEXT,
                temperature REAL,
                humidity INTEGER,
                wind_speed REAL,
                mood_category TEXT,
                popularity_score REAL,
                raw_data_path TEXT,
                UNIQUE(city, track_name, artist_name, processed_at),
                CHECK (listeners >= 0),
                CHECK (playcount >= 0)
            )
        ''')

        # etl_stats
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS etl_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                raw_file_path TEXT,
                records_processed INTEGER,
                records_loaded INTEGER,
                success_rate REAL,
                processing_time_seconds REAL
            )
        ''')

        # indexes
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_city_weather 
            ON processed_tracks(city, weather_condition)
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_mood_weather 
            ON processed_tracks(mood_category, weather_condition)
        ''')

        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS soundcharts_tracks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                track_name TEXT NOT NULL,
                artist_name TEXT NOT NULL,
                uuid TEXT UNIQUE,

                release_date TEXT,
                image_url TEXT,
                credit_name TEXT,
                isrc TEXT,
                isrc_country_code TEXT,
                isrc_country_name TEXT,

                genres TEXT,
                labels TEXT,

                acousticness REAL,
                danceability REAL,
                energy REAL,
                instrumentalness REAL,
                key INTEGER,
                liveness REAL,
                loudness REAL,
                mode INTEGER,
                speechiness REAL,
                tempo REAL,
                time_signature INTEGER,
                valence REAL,

                enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()
        conn.close()
        self.logger.info("✅ Base de données ETL initialisée")
    
    def extract_from_raw(self, raw_file_path: str) -> Optional[Dict]:
        """Extrait les données depuis le fichier JSON brut"""
        try:
            with open(raw_file_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            self.logger.info(f"📂 Données extraites de: {raw_file_path}")
            return raw_data
            
        except Exception as e:
            self.logger.error(f"❌ Erreur extraction {raw_file_path}: {e}")
            return None
    
    def transform_track_data(self, track: Dict, weather_data: Dict, metadata: Dict) -> Optional[Dict]:
        """Transforme une piste brute en données structurées"""
        try:
            # Extraction des données Last.fm
            track_name = track.get('name', '').strip()
            artist_name = track['artist'].get('name', '').strip()
            listeners = int(track.get('listeners', 0))
            playcount = int(track.get('playcount', 0))
            rank = int(track.get('@attr', {}).get('rank', 0))
            
            # Validation des données essentielles
            if not track_name or not artist_name:
                self.logger.warning(f"Track ignorée - nom ou artiste manquant: {track_name} - {artist_name}")
                return None
            
            # Extraction des données météo
            weather_main = weather_data['weather'][0]['main'] if weather_data.get('weather') else 'Unknown'
            weather_desc = weather_data['weather'][0]['description'] if weather_data.get('weather') else 'Unknown'
            temperature = weather_data['main'].get('temp', 0)
            humidity = weather_data['main'].get('humidity', 0)
            wind_speed = weather_data['wind'].get('speed', 0)
            
            # Analyse d'humeur
            mood = self._analyze_mood(track_name, artist_name)
            
            # Calcul du score de popularité
            popularity_score = self._calculate_popularity_score(listeners, playcount)
            
            transformed_data = {
                'city': metadata['city'],
                'country': metadata['country'],
                'track_name': track_name,
                'artist_name': artist_name,
                'listeners': listeners,
                'playcount': playcount,
                'rank_position': rank,
                'weather_condition': weather_main,
                'weather_description': weather_desc,
                'temperature': temperature,
                'humidity': humidity,
                'wind_speed': wind_speed,
                'mood_category': mood,
                'popularity_score': popularity_score,
                'raw_data_path': metadata.get('raw_file_path', ''),
                'processed_at': datetime.now().isoformat()
            }
            
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"❌ Erreur transformation track: {e}")
            return None
    
    def _analyze_mood(self, track_name: str, artist_name: str) -> str:
        """Analyse l'humeur basée sur le titre et l'artiste"""
        mood_keywords = {
            'happy': ['love', 'happy', 'sun', 'dance', 'party', 'summer', 'good', 'beautiful', 'smile'],
            'sad': ['sad', 'rain', 'lonely', 'cry', 'broken', 'heart', 'tears', 'miss', 'pain'],
            'energetic': ['fire', 'energy', 'power', 'strong', 'fight', 'wild', 'crazy', 'burn'],
            'calm': ['calm', 'peace', 'quiet', 'soft', 'gentle', 'easy', 'slow', 'dream'],
            'romantic': ['love', 'heart', 'kiss', 'baby', 'darling', 'sweet', 'night', 'moon']
        }
        
        text_to_analyze = f"{track_name} {artist_name}".lower()
        
        mood_scores = {}
        for mood, keywords in mood_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text_to_analyze)
            if score > 0:
                mood_scores[mood] = score
        
        return max(mood_scores.items(), key=lambda x: x[1])[0] if mood_scores else 'neutral'
    
    def _calculate_popularity_score(self, listeners: int, playcount: int) -> float:
        """Calcule un score de popularité normalisé"""
        if listeners == 0:
            return 0.0
        
        # Score basé sur les listeners (normalisé à 10k max)
        base_score = min(listeners / 10000, 1.0)
        
        # Bonus d'engagement (playcount par listener)
        engagement_ratio = playcount / max(listeners, 1)
        engagement_bonus = min(engagement_ratio * 0.1, 0.2)  # Max 0.2 bonus
        
        return round(base_score + engagement_bonus, 3)
    

    
        

    def load_transformed_data(self, transformed_data: List[Dict], raw_file_path: str) -> Dict:
        """Charge les données transformées dans la base"""
        start_time = datetime.now()
        records_processed = len(transformed_data)
        records_loaded = 0
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for record in transformed_data:
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO processed_tracks 
                        (city, country, track_name, artist_name, listeners, playcount, 
                         rank_position, weather_condition, weather_description, temperature,
                         humidity, wind_speed, mood_category, popularity_score, raw_data_path)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        record['city'], record['country'], record['track_name'],
                        record['artist_name'], record['listeners'], record['playcount'],
                        record['rank_position'], record['weather_condition'], record['weather_description'],
                        record['temperature'], record['humidity'], record['wind_speed'],
                        record['mood_category'], record['popularity_score'], record['raw_data_path']
                    ))
                    records_loaded += 1
                    
                except Exception as e:
                    self.logger.warning(f"Erreur chargement {record['track_name']}: {e}")
                    continue
            
            conn.commit()
            
            # Log des statistiques ETL
            processing_time = (datetime.now() - start_time).total_seconds()
            success_rate = records_loaded / records_processed if records_processed > 0 else 0
            
            cursor.execute('''
                INSERT INTO etl_stats 
                (raw_file_path, records_processed, records_loaded, success_rate, processing_time_seconds)
                VALUES (?, ?, ?, ?, ?)
            ''', (raw_file_path, records_processed, records_loaded, success_rate, processing_time))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"✅ ETL réussi: {records_loaded}/{records_processed} records chargés")
            
            return {
                'status': 'success',
                'records_processed': records_processed,
                'records_loaded': records_loaded,
                'success_rate': success_rate,
                'processing_time': processing_time
            }
            
        except Exception as e:
            self.logger.error(f"❌ Erreur chargement ETL: {e}")
            return {
                'status': 'failure',
                'error': str(e),
                'records_processed': records_processed,
                'records_loaded': records_loaded
            }
        
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

                song_obj = obj.get("object", {})  # prend l'objet song

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

        
    def run_etl_for_raw_file(self, raw_file_path: str) -> Dict:
        """Exécute le pipeline ETL complet pour un fichier brut"""
        self.logger.info(f"🚀 Début ETL pour: {raw_file_path}")
        
        # E - EXTRACTION
        raw_data = self.extract_from_raw(raw_file_path)
        if not raw_data:
            self.logger.error(f"❌ Échec extraction pour {raw_file_path}")
            return {'status': 'extraction_failed', 'file': raw_file_path}
        
        # Vérifier que les données nécessaires sont présentes
        if not raw_data.get('lastfm_data') or not raw_data.get('weather_data'):
            self.logger.error(f"❌ Données manquantes dans {raw_file_path}")
            return {'status': 'invalid_data', 'file': raw_file_path}
        
        # T - TRANSFORMATION
        transformed_data = []
        
        # Accès sécurisé aux données Last.fm
        lastfm_data = raw_data.get('lastfm_data', {})
        tracks_data = lastfm_data.get('tracks', {})
        tracks = tracks_data.get('track', [])
        
        weather_data = raw_data.get('weather_data', {})
        metadata = raw_data.get('metadata', {})
        metadata['raw_file_path'] = raw_file_path
        
        self.logger.info(f"📊 {len(tracks)} tracks à transformer")
        
        for track in tracks:
            transformed_track = self.transform_track_data(track, weather_data, metadata)
            if transformed_track:
                transformed_data.append(transformed_track)
        
        if not transformed_data:
            self.logger.warning(f"⚠️  Aucune donnée transformée pour {raw_file_path}")
            return {'status': 'transformation_failed', 'file': raw_file_path}
    
                
        conn = sqlite3.connect('data/processed_music_weather.db')
        pd.read_sql("SELECT * FROM soundcharts_tracks", conn)

        load_result = self.load_transformed_data(transformed_data, raw_file_path)
    
        return {
            'file': raw_file_path,
            'records_extracted': len(tracks),
            'records_transformed': len(transformed_data),
            'records_loaded': len(load_result),
            **load_result
        }


    def _get_connection(self):
        """Retourne une connexion à la base de données"""
        return sqlite3.connect(self.db_path)