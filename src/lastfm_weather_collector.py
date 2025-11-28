# src/lastfm_weather_collector.py
import requests
import sqlite3
import time
from datetime import datetime
import os
import json
import logging
from typing import Dict, List, Optional

from utils.logger import setup_logging
from utils.helpers import load_config, backup_database, validate_environment

class LastFmWeatherCollector:
    """
    Collecteur de données Last.fm et météo pour analyser les tendances musicales
    en fonction des conditions météorologiques.
    """
    
    def __init__(self):
        # Configuration
        self.lastfm_api_key = os.getenv('LASTFM_API_KEY')
        self.weather_api_key = os.getenv('OPENWEATHER_API_KEY')
        
        # Validation de l'environnement
        validate_environment()
        
        # Configuration des villes
        self.cities_config = load_config()
        
        # Setup logging
        self.logger = setup_logging()
        
        # Setup database
        self.setup_database()
        
        self.logger.info("LastFmWeatherCollector initialisé avec succès")
    
    def setup_database(self):
        """Initialise la base de données SQLite avec les tables nécessaires"""
        try:
            # Créer le dossier data si nécessaire
            os.makedirs('data', exist_ok=True)
            
            self.conn = sqlite3.connect('data/lastfm_weather.db')
            cursor = self.conn.cursor()
            
            # Table principale des tendances
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS city_music_trends (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    city TEXT NOT NULL,
                    country TEXT NOT NULL,
                    track_name TEXT NOT NULL,
                    artist_name TEXT NOT NULL,
                    listeners INTEGER DEFAULT 0,
                    playcount INTEGER DEFAULT 0,
                    rank INTEGER DEFAULT 0,
                    weather_main TEXT,
                    weather_description TEXT,
                    temperature REAL,
                    humidity INTEGER,
                    pressure INTEGER,
                    mood_category TEXT,
                    UNIQUE(city, track_name, artist_name, timestamp)
                )
            ''')
            
            # Table des statistiques quotidiennes
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date DATE NOT NULL,
                    city TEXT NOT NULL,
                    total_tracks INTEGER DEFAULT 0,
                    avg_temperature REAL,
                    dominant_mood TEXT,
                    most_popular_artist TEXT,
                    UNIQUE(date, city)
                )
            ''')
            
            # Index pour les performances
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_city_timestamp 
                ON city_music_trends(city, timestamp)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_weather_mood 
                ON city_music_trends(weather_main, mood_category)
            ''')
            
            self.conn.commit()
            self.logger.info("Base de données initialisée avec succès")
            
        except Exception as e:
            self.logger.error(f"Erreur lors de l'initialisation de la base: {e}")
            raise
    
    def get_lastfm_top_tracks(self, country: str, limit: int = 0) -> List[Dict]:
        """
        Récupère les morceaux les plus populaires d'un pays via Last.fm API
        
        Args:
            country: Nom du pays
            limit: Nombre maximum de morceaux à récupérer
            
        Returns:
            Liste des morceaux avec leurs métadonnées
        """
        try:
            url = "http://ws.audioscrobbler.com/2.0/"
            params = {
                'method': 'geo.gettoptracks',
                'country': country,
                'api_key': self.lastfm_api_key,
                'format': 'json',
                'limit': limit
            }
            
            self.logger.info(f"Récupération des tops tracks pour {country}")
            
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            # Validation de la structure de réponse
            if 'tracks' not in data or 'track' not in data['tracks']:
                self.logger.warning(f"Structure de réponse inattendue pour {country}")
                return []
            
            tracks = []
            for rank, track_data in enumerate(data['tracks']['track'][:limit], 1):
                try:
                    track_info = {
                        'track_name': track_data.get('name', 'Unknown').strip(),
                        'artist_name': track_data['artist'].get('name', 'Unknown').strip(),
                        'listeners': int(track_data.get('listeners', 0)),
                        'playcount': int(track_data.get('playcount', 0)),
                        'rank': rank
                    }
                    
                    # Validation des données
                    if (track_info['track_name'] != 'Unknown' and 
                        track_info['artist_name'] != 'Unknown'):
                        tracks.append(track_info)
                        
                except (KeyError, ValueError) as e:
                    self.logger.warning(f"Erreur parsing track {rank}: {e}")
                    continue
            
            self.logger.info(f"Récupéré {len(tracks)} tracks pour {country}")
            return tracks
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erreur réseau pour {country}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Erreur inattendue pour {country}: {e}")
            return []
    
    def get_city_weather(self, city: str) -> Optional[Dict]:
        """
        Récupère les données météo actuelles d'une ville
        
        Args:
            city: Nom de la ville
            
        Returns:
            Dictionnaire des données météo ou None en cas d'erreur
        """
        try:
            url = "https://api.openweathermap.org/data/2.5/weather"
            params = {
                'q': city,
                'appid': self.weather_api_key,
                'units': 'metric',
                'lang': 'fr'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            weather_info = {
                'main': data['weather'][0]['main'],
                'description': data['weather'][0]['description'],
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'wind_speed': data['wind'].get('speed', 0),
                'clouds': data['clouds'].get('all', 0)
            }
            
            self.logger.debug(f"Météo récupérée pour {city}: {weather_info['main']}")
            return weather_info
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erreur météo pour {city}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Erreur inattendue météo {city}: {e}")
            return None
    
    def analyze_track_mood(self, track_name: str, artist_name: str) -> str:
        """
        Analyse l'humeur d'un morceau basé sur des mots-clés dans le titre et l'artiste
        
        Args:
            track_name: Nom du morceau
            artist_name: Nom de l'artiste
            
        Returns:
            Catégorie d'humeur (happy, sad, energetic, calm, romantic, neutral)
        """
        # Dictionnaire étendu de mots-clés pour l'humeur
        # Dans analyze_track_mood(), ajoutez plus de mots-clés
        mood_keywords = {
            'happy': ['love', 'happy', 'sun', 'dance', 'party', 'summer', 'good', 
                    'beautiful', 'smile', 'joy', 'fun', 'celebration', 'sunshine', 'vibe'],
            'sad': ['sad', 'rain', 'lonely', 'cry', 'broken', 'heart', 'tears', 
                'miss', 'pain', 'alone', 'goodbye', 'hurt', 'dark', 'lost', 'blue'],
            'energetic': ['fire', 'energy', 'power', 'strong', 'fight', 'wild', 
                        'crazy', 'burn', 'rage', 'storm', 'rock', 'beat', 'bass', 'loud'],
            'calm': ['calm', 'peace', 'quiet', 'soft', 'gentle', 'easy', 'slow', 
                    'dream', 'sleep', 'silent', 'chill', 'relax', 'serene', 'mellow'],
            'romantic': ['love', 'heart', 'kiss', 'baby', 'darling', 'sweet', 
                        'night', 'moon', 'hold', 'touch', 'romance', 'darling', 'together']
        }
        
        text_to_analyze = f"{track_name} {artist_name}".lower()
        
        # Compter les occurrences de chaque humeur
        mood_scores = {}
        for mood, keywords in mood_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text_to_analyze)
            if score > 0:
                mood_scores[mood] = score
        
        # Retourner l'humeur dominante
        if mood_scores:
            dominant_mood = max(mood_scores.items(), key=lambda x: x[1])[0]
            self.logger.debug(f"Mood analysis: '{track_name}' → {dominant_mood}")
            return dominant_mood
        else:
            return 'neutral'
    
    def save_data_point(self, data: Dict) -> bool:
        """
        Sauvegarde un point de données en base avec gestion des doublons
        
        Args:
            data: Dictionnaire contenant les données à sauvegarder
            
        Returns:
            True si sauvegardé avec succès, False sinon
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO city_music_trends 
                (city, country, track_name, artist_name, listeners, playcount, rank,
                 weather_main, weather_description, temperature, humidity, pressure, mood_category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data['city'], data['country'], data['track_name'],
                data['artist_name'], data['listeners'], data['playcount'], data['rank'],
                data['weather_main'], data['weather_description'], data['temperature'],
                data['humidity'], data.get('pressure', 0), data['mood_category']
            ))
            
            self.conn.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"Erreur sauvegarde données: {e}")
            return False
    
    def collect_city_data(self, city: str, country: str) -> Optional[List[Dict]]:
        """
        Collecte les données complètes pour une ville spécifique
        
        Args:
            city: Nom de la ville
            country: Nom du pays
            
        Returns:
            Liste des données collectées ou None en cas d'erreur
        """
        self.logger.info(f"Début collecte pour {city}, {country}")
        
        try:
            # 1. Récupérer les tops tracks du pays
            tracks = self.get_lastfm_top_tracks(country, limit=10)
            if not tracks:
                self.logger.warning(f"Aucune donnée Last.fm pour {country}")
                return None
            
            # 2. Récupérer la météo de la ville
            weather = self.get_city_weather(city)
            if not weather:
                self.logger.warning(f"Aucune donnée météo pour {city}")
                return None
            
            # 3. Traiter et sauvegarder chaque track
            city_data = []
            successful_saves = 0
            
            for track in tracks:
                try:
                    mood = self.analyze_track_mood(track['track_name'], track['artist_name'])
                    
                    data_point = {
                        'city': city,
                        'country': country,
                        'track_name': track['track_name'],
                        'artist_name': track['artist_name'],
                        'listeners': track['listeners'],
                        'playcount': track['playcount'],
                        'rank': track['rank'],
                        'weather_main': weather['main'],
                        'weather_description': weather['description'],
                        'temperature': weather['temperature'],
                        'humidity': weather['humidity'],
                        'pressure': weather.get('pressure', 0),
                        'mood_category': mood
                    }
                    
                    # Sauvegarder en base
                    if self.save_data_point(data_point):
                        city_data.append(data_point)
                        successful_saves += 1
                    
                except Exception as e:
                    self.logger.error(f"Erreur traitement track {track['track_name']}: {e}")
                    continue
            
            self.logger.info(f"Collecte {city} terminée: {successful_saves}/{len(tracks)} tracks sauvegardées")
            return city_data if city_data else None
            
        except Exception as e:
            self.logger.error(f"Erreur collecte {city}: {e}")
            return None
    
    def generate_daily_stats(self):
        """Génère les statistiques quotidiennes agrégées"""
        try:
            cursor = self.conn.cursor()
            
            # Statistiques par ville
            cursor.execute('''
                INSERT OR REPLACE INTO daily_stats 
                (date, city, total_tracks, avg_temperature, dominant_mood, most_popular_artist)
                SELECT 
                    DATE(timestamp) as date,
                    city,
                    COUNT(*) as total_tracks,
                    AVG(temperature) as avg_temperature,
                    (SELECT mood_category 
                     FROM city_music_trends t2 
                     WHERE t2.city = t1.city AND DATE(t2.timestamp) = DATE(t1.timestamp)
                     GROUP BY mood_category 
                     ORDER BY COUNT(*) DESC 
                     LIMIT 1) as dominant_mood,
                    (SELECT artist_name 
                     FROM city_music_trends t3 
                     WHERE t3.city = t1.city AND DATE(t3.timestamp) = DATE(t1.timestamp)
                     GROUP BY artist_name 
                     ORDER BY COUNT(*) DESC 
                     LIMIT 1) as most_popular_artist
                FROM city_music_trends t1
                WHERE DATE(timestamp) = DATE('now')
                GROUP BY city, DATE(timestamp)
            ''')
            
            self.conn.commit()
            self.logger.info("Statistiques quotidiennes générées")
            
        except Exception as e:
            self.logger.error(f"Erreur génération stats quotidiennes: {e}")
    
    def display_current_insights(self):
        """Affiche les insights actuels basés sur les données récentes"""
        try:
            cursor = self.conn.cursor()
            
            print("\n" + "="*70)
            print("📊 LAST.FM + MÉTÉO - INSIGHTS TEMPS RÉEL")
            print("="*70)
            
            # Humeur dominante par type de météo (dernière heure)
            cursor.execute('''
                SELECT weather_main, mood_category, COUNT(*) as count
                FROM city_music_trends 
                WHERE timestamp >= datetime('now', '-1 hour')
                GROUP BY weather_main, mood_category
                ORDER BY weather_main, count DESC
            ''')
            
            weather_mood_data = cursor.fetchall()
            
            if weather_mood_data:
                print("\n🌤️  HUMEUR DOMINANTE PAR MÉTÉO (dernière heure):")
                current_weather = None
                for weather, mood, count in weather_mood_data:
                    if weather != current_weather:
                        print(f"\n   {weather.upper():<15} → {mood.upper()} ({count} tracks)")
                        current_weather = weather
                    else:
                        print(f"                   → {mood.upper()} ({count} tracks)")
            
            # Top artistes global (24h)
            cursor.execute('''
                SELECT artist_name, COUNT(*) as count
                FROM city_music_trends 
                WHERE timestamp >= datetime('now', '-24 hours')
                GROUP BY artist_name
                ORDER BY count DESC
                LIMIT 5
            ''')
            
            top_artists = cursor.fetchall()
            if top_artists:
                print(f"\n👑 TOP 5 ARTISTES (24h):")
                for artist, count in top_artists:
                    print(f"   🎵 {artist} ({count} apparitions)")
            
            # Ville la plus active
            cursor.execute('''
                SELECT city, COUNT(*) as track_count
                FROM city_music_trends 
                WHERE timestamp >= datetime('now', '-1 hour')
                GROUP BY city
                ORDER BY track_count DESC
                LIMIT 1
            ''')
            
            top_city = cursor.fetchone()
            if top_city:
                print(f"\n🏙️  VILLE LA PLUS ACTIVE: {top_city[0]} ({top_city[1]} tracks)")
                
        except Exception as e:
            self.logger.error(f"Erreur affichage insights: {e}")
    
    def run_collection_cycle(self):
        """
        Exécute un cycle complet de collecte pour toutes les villes configurées
        
        Returns:
            Nombre total de données collectées
        """
        self.logger.info(f"Début cycle de collecte - {len(self.cities_config['cities'])} villes")
        
        total_collected = 0
        all_data = []
        
        for city, country in self.cities_config['cities'].items():
            self.logger.info(f"Traitement de {city}, {country}")
            
            city_data = self.collect_city_data(city, country)
            if city_data:
                all_data.extend(city_data)
                total_collected += len(city_data)
            
            # Respecter le rate limiting
            time.sleep(float(os.getenv('RATE_LIMIT_DELAY', 1.0)))
        
        # Générer les insights et statistiques
        if all_data:
            self.generate_daily_stats()
            self.display_current_insights()
            
            # Sauvegarde de precaution
            backup_file = backup_database()
            self.logger.info(f"Sauvegarde créée: {backup_file}")
        
        self.logger.info(f"Cycle terminé: {total_collected} données collectées")
        return total_collected
    
    def run_continuous_monitoring(self, interval_minutes: int = 60):
        """
        Lance la surveillance continue avec collecte périodique
        
        Args:
            interval_minutes: Intervalle entre les collectes en minutes
        """
        self.logger.info(f"Démarrage surveillance continue - Intervalle: {interval_minutes}min")
        
        print("🚀 LAST.FM + WEATHER MONITORING STARTED")
        print("="*70)
        print(f"🏙️  Villes monitorées: {', '.join(self.cities_config['cities'].keys())}")
        print(f"⏰ Intervalle: {interval_minutes} minutes")
        print(f"🎯 Données: Top tracks + Météo + Analyse d'humeur")
        print("⏹️  Ctrl+C pour arrêter")
        print("="*70)
        
        cycle_count = 0
        
        try:
            while True:
                cycle_count += 1
                self.logger.info(f"Début cycle #{cycle_count}")
                
                print(f"\n📈 CYCLE #{cycle_count} - {datetime.now().strftime('%H:%M:%S')}")
                
                collected = self.run_collection_cycle()
                
                if collected > 0:
                    print(f"✅ Cycle #{cycle_count} terminé: {collected} données collectées")
                else:
                    print(f"⚠️  Cycle #{cycle_count} terminé: Aucune nouvelle donnée")
                
                # Attendre le prochain cycle
                wait_seconds = interval_minutes * 60
                print(f"⏳ Prochain cycle dans {interval_minutes} minutes...")
                
                for remaining in range(wait_seconds, 0, -60):
                    if remaining % 300 == 0:  # Log toutes les 5 minutes
                        self.logger.info(f"Prochain cycle dans {remaining//60} minutes")
                    time.sleep(60)
                    
        except KeyboardInterrupt:
            self.logger.info("Surveillance arrêtée par l'utilisateur")
            print("\n🛑 Surveillance arrêtée")
        except Exception as e:
            self.logger.error(f"Erreur critique: {e}")
            raise
        finally:
            if hasattr(self, 'conn'):
                self.conn.close()
                self.logger.info("Connexion base de données fermée")


# Point d'entrée pour tests
if __name__ == "__main__":
    collector = LastFmWeatherCollector()
    collector.run_continuous_monitoring(interval_minutes=10)