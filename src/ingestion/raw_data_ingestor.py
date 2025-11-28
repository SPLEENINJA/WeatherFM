# src/ingestion/raw_data_ingestor_corrected.py
import requests
import json
import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging
import time
from dataclasses import dataclass
from dotenv import load_dotenv

@dataclass
class IngestionResult:
    """Résultat d'une opération d'ingestion"""
    success: bool
    records_ingested: int
    raw_data_path: Optional[str] = None
    errors: List[str] = None
    source_anomalies: List[str] = None
    db_anomalies: List[str] = None

class RawDataIngestor:
    """
    Système d'ingestion corrigé avec meilleure gestion des APIs
    """
    
    def __init__(self, LASTFM_API_KEY: str, OPENWEATHER_API_KEY: str):
        self.lastfm_api_key = LASTFM_API_KEY
        self.weather_api_key = OPENWEATHER_API_KEY
        self.logger = logging.getLogger(__name__)
        
        # Vérification des clés
        if not self.lastfm_api_key :
            self.logger.error("❌ Clé Last.fm non configurée")
        if not self.weather_api_key:
            self.logger.error("❌ Clé OpenWeather non configurée")
        
        # Configuration des dossiers
        self.raw_data_dir = 'data/raw'
        self.failed_ingestions_dir = 'data/failed_ingestions'
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.failed_ingestions_dir, exist_ok=True)
        
        # Initialisation de la base pour les métadonnées d'ingestion
        self._init_ingestion_db()
    
    def _init_ingestion_db(self):
        """Initialise la base de données pour le suivi de l'ingestion"""
        conn = sqlite3.connect('data/ingestion_metadata.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ingestion_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                city TEXT NOT NULL,
                country TEXT NOT NULL,
                raw_data_path TEXT,
                records_ingested INTEGER DEFAULT 0,
                source_anomalies_count INTEGER DEFAULT 0,
                db_anomalies_count INTEGER DEFAULT 0,
                status TEXT CHECK(status IN ('success', 'partial_failure', 'failure')),
                error_message TEXT,
                processing_time_seconds REAL
            )
        ''')
        
        conn.commit()
        conn.close()
        self.logger.info("✅ Base de métadonnées d'ingestion initialisée")
    
    def _fetch_lastfm_data(self, country: str) -> Optional[Dict]:
        """Récupère les données Last.fm avec gestion d'erreurs améliorée"""
        if not self.lastfm_api_key or self.lastfm_api_key == "votre_cle_lastfm_ici":
            self.logger.error("❌ Clé Last.fm non configurée")
            return None
            
        max_retries = 2
        for attempt in range(max_retries):
            try:
                url = "http://ws.audioscrobbler.com/2.0/"
                params = {
                    'method': 'geo.gettoptracks',
                    'country': country,
                    'api_key': self.lastfm_api_key,  # CORRIGÉ
                    'format': 'json',
                    'limit': 10  # Réduit pour tests
                }
                
                self.logger.debug(f"🔗 Tentative {attempt + 1} Last.fm pour {country}")
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'tracks' in data and 'track' in data['tracks']:
                        self.logger.info(f"✅ Last.fm réussi pour {country}: {len(data['tracks']['track'])} tracks")
                        return data
                    else:
                        self.logger.warning(f"⚠️  Structure Last.fm invalide pour {country}")
                        return None
                else:
                    self.logger.warning(f"⚠️  Last.fm status {response.status_code} pour {country}")
                    if attempt == max_retries - 1:
                        return None
                    time.sleep(1)
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"❌ Erreur réseau Last.fm: {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(1)
        
        return None
    
    def _fetch_weather_data(self, city: str) -> Optional[Dict]:
        """Récupère les données météo avec gestion d'erreurs améliorée"""
        if not self.weather_api_key or self.weather_api_key == "votre_cle_openweather_ici":
            self.logger.error("❌ Clé OpenWeather non configurée")
            return None
            
        max_retries = 2
        for attempt in range(max_retries):
            try:
                url = "https://api.openweathermap.org/data/2.5/weather"
                params = {
                    'q': city,
                    'appid': self.weather_api_key,  # CORRIGÉ - 'appid' au lieu de 'api_key'
                    'units': 'metric',
                    'lang': 'fr'
                }
                
                self.logger.debug(f"🌤️  Tentative {attempt + 1} météo pour {city}")
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    self.logger.info(f"✅ Météo récupérée pour {city}: {data['weather'][0]['main']}")
                    return data
                else:
                    self.logger.warning(f"⚠️  Météo status {response.status_code} pour {city}")
                    if attempt == max_retries - 1:
                        return None
                    time.sleep(1)
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"❌ Erreur réseau météo: {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(1)
        
        return None
    
    def ingest_city_data(self, city: str, country: str) -> IngestionResult:
        """
        Ingère les données brutes pour une ville
        """
        start_time = datetime.now()
        self.logger.info(f"🍽️  Début ingestion pour {city}, {country}")
        
        anomalies = []
        lastfm_data = None
        weather_data = None
        
        try:
            # 1. EXTRACTION LAST.FM
            lastfm_data = self._fetch_lastfm_data(country)
            if not lastfm_data:
                anomalies.append("LASTFM_API_FAILURE: Échec récupération données Last.fm")
            
            # 2. EXTRACTION MÉTÉO
            weather_data = self._fetch_weather_data(city)
            if not weather_data:
                anomalies.append("WEATHER_API_FAILURE: Échec récupération données météo")
            
            # 3. SAUVEGARDE DONNÉES BRUTES (même si échec partiel)
            raw_data_path = self._save_raw_data(lastfm_data, weather_data, city, country)
            
            # 4. CALCUL DES MÉTRIQUES
            records_ingested = 0
            if lastfm_data and 'tracks' in lastfm_data and 'track' in lastfm_data['tracks']:
                records_ingested = len(lastfm_data['tracks']['track'])
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # 5. DÉTERMINATION DU STATUT
            if not lastfm_data and not weather_data:
                status = 'failure'
                error_msg = "Échec complet des APIs"
            elif anomalies:
                status = 'partial_failure'
                error_msg = f"{len(anomalies)} anomalies"
            else:
                status = 'success'
                error_msg = None
            
            # 6. LOG DE L'INGESTION
            self._log_ingestion_attempt(
                city, country, raw_data_path, records_ingested,
                len(anomalies), status, error_msg, processing_time
            )
            
            self.logger.info(f"✅ Ingestion {status} pour {city}: {records_ingested} records")
            
            return IngestionResult(
                success=status != 'failure',
                records_ingested=records_ingested,
                raw_data_path=raw_data_path,
                errors=[error_msg] if error_msg else [],
                source_anomalies=anomalies,
                db_anomalies=[]
            )
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"Erreur inattendue: {str(e)}"
            
            self._log_ingestion_attempt(
                city, country, None, 0, len(anomalies),
                'failure', error_msg, processing_time
            )
            
            self.logger.error(f"❌ Erreur ingestion {city}: {e}")
            
            return IngestionResult(
                success=False,
                records_ingested=0,
                errors=[error_msg],
                source_anomalies=anomalies,
                db_anomalies=[]
            )
    
    def _save_raw_data(self, lastfm_data: Dict, weather_data: Dict, city: str, country: str) -> Optional[str]:
        """Sauvegarde les données brutes en JSON"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{city}_{country}_{timestamp}.json"
            filepath = os.path.join(self.raw_data_dir, filename)
            
            raw_data = {
                'metadata': {
                    'city': city,
                    'country': country,
                    'ingestion_timestamp': datetime.now().isoformat(),
                    'data_source': 'lastfm_weather_ingestor'
                },
                'lastfm_data': lastfm_data,
                'weather_data': weather_data
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(raw_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"💾 Données brutes sauvegardées: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"❌ Erreur sauvegarde données brutes: {e}")
            return None
    
    def _log_ingestion_attempt(self, city: str, country: str, raw_data_path: Optional[str], 
                             records_ingested: int, anomalies_count: int, 
                             status: str, error_message: str, processing_time: float):
        """Log les tentatives d'ingestion dans la base"""
        try:
            conn = sqlite3.connect('data/ingestion_metadata.db')
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO ingestion_log 
                (city, country, raw_data_path, records_ingested, 
                 source_anomalies_count, status, error_message, processing_time_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (city, country, raw_data_path, records_ingested,
                  anomalies_count, status, error_message, processing_time))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"❌ Erreur log ingestion: {e}")
