# src/ingestion/batch_ingestor.py
import logging
import time
from datetime import datetime
from typing import List, Dict
import os
import json
from dotenv import load_dotenv

from .raw_data_ingestor import RawDataIngestor, IngestionResult


class BatchIngestor:
    """
    Orchestrateur d'ingestion par lots avec gestion des erreurs
    et monitoring des performances
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        load_dotenv()

        lastfm_key = os.getenv('LASTFM_API_KEY',"")
        weather_key = os.getenv('OPENWEATHER_API_KEY',"")

        self.ingestor = RawDataIngestor(lastfm_key, weather_key)
        self.cities_config = self._load_cities_config()

        self.logger.info("🏭 BatchIngestor initialisé")

    # ---------------------------------------------------------
    # CHARGEMENT DES VILLES

    def _load_cities_config(self) -> Dict[str, str]:
        """
        Charge les villes + pays via .env
        CITIES="Paris,Nice,Lyon"
        COUNTRIES="France,France,France"
        """

        cities_str = os.getenv('CITIES', "Paris,Nice")
        countries_str = os.getenv('COUNTRIES', "France,France")

        # Split propre
        cities = [c.strip() for c in cities_str.split(',')]
        countries = [c.strip() for c in countries_str.split(',')]

        # Si un seul pays fourni → répéter pour chaque ville
        if len(countries) == 1 and len(cities) > 1:
            countries = countries * len(cities)

        # Sécurité : alignement
        if len(cities) != len(countries):
            raise ValueError(
                f"Nombre de villes ({len(cities)}) ≠ nombre de pays ({len(countries)}). "
                "Vérifie CITIES= et COUNTRIES= dans ton .env."
            )

        return dict(zip(cities, countries))


    # ---------------------------------------------------------
    # INGESTION BATCH
    # ---------------------------------------------------------
    def run_batch_ingestion(self, batch_size: int = None) -> Dict:
        start_time = datetime.now()
        self.logger.info(f"🏭 Début ingestion batch pour {len(self.cities_config)} villes")

        results = []
        cities_to_process = list(self.cities_config.items())

        if batch_size:
            cities_to_process = cities_to_process[:batch_size]

        for city, country in cities_to_process:
            self.logger.info(f"🍽️  Ingestion de {city}, {country}")
            result = self.ingestor.ingest_city_data(city, country)

            results.append({
                'city': city,
                'country': country,
                'result': result
            })

            time.sleep(float(os.getenv('INGESTION_DELAY', 2.0)))

        batch_stats = self._calculate_batch_stats(results)
        batch_stats['total_processing_time'] = (datetime.now() - start_time).total_seconds()
        batch_stats['batch_completed_at'] = datetime.now().isoformat()

        self.logger.info(f"📊 Batch terminé: {batch_stats}")

        self._save_batch_report(batch_stats, results)

        return {
            'batch_stats': batch_stats,
            'detailed_results': results
        }

    # ---------------------------------------------------------
    # STATS
    # ---------------------------------------------------------
    def _calculate_batch_stats(self, results: List[Dict]) -> Dict:
        total_cities = len(results)
        successful_ingestions = sum(1 for r in results if r['result'].success)
        total_records = sum(r['result'].records_ingested for r in results)
        total_anomalies = sum(len(r['result'].source_anomalies) for r in results)

        return {
            'total_cities_processed': total_cities,
            'successful_ingestions': successful_ingestions,
            'failed_ingestions': total_cities - successful_ingestions,
            'success_rate': (successful_ingestions / total_cities * 100) if total_cities > 0 else 0,
            'total_records_ingested': total_records,
            'total_anomalies_detected': total_anomalies,
            'avg_records_per_city': total_records / total_cities if total_cities > 0 else 0
        }

    # ---------------------------------------------------------
    # RAPPORT
    # ---------------------------------------------------------
    def _save_batch_report(self, batch_stats: Dict, results: List[Dict]):
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            path = f"data/ingestion_reports/batch_report_{timestamp}.json"
            os.makedirs('data/ingestion_reports', exist_ok=True)

            report = {
                'metadata': {
                    'report_timestamp': datetime.now().isoformat(),
                    'batch_id': timestamp,
                    'ingestion_system': 'RawDataIngestor'
                },
                'batch_statistics': batch_stats,
                'city_results': [
                    {
                        'city': r['city'],
                        'country': r['country'],
                        'success': r['result'].success,
                        'records_ingested': r['result'].records_ingested,
                        'anomalies_count': len(r['result'].source_anomalies),
                        'raw_data_path': r['result'].raw_data_path
                    }
                    for r in results
                ]
            }

            with open(path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)

            self.logger.info(f"📄 Rapport batch sauvegardé: {path}")

        except Exception as e:
            self.logger.error(f"❌ Erreur sauvegarde rapport: {e}")

    # ---------------------------------------------------------
    # HEALTHCHECK
    # ---------------------------------------------------------
    def get_ingestion_health(self) -> Dict:
        try:
            import sqlite3
            conn = sqlite3.connect('data/ingestion_metadata.db')
            cursor = conn.cursor()

            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as ok,
                    AVG(processing_time_seconds),
                    SUM(source_anomalies_count),
                    MIN(timestamp),
                    MAX(timestamp)
                FROM ingestion_log
            ''')

            stats = cursor.fetchone()
            conn.close()

            return {
                'total_ingestions': stats[0],
                'success_rate': (stats[1] / stats[0] * 100) if stats[0] else 0,
                'avg_processing_time_seconds': round(stats[2], 2) if stats[2] else 0,
                'total_anomalies_detected': stats[3],
                'system_uptime': f"{stats[4]} to {stats[5]}" if stats[4] else "N/A"
            }

        except Exception as e:
            self.logger.error(f"❌ Erreur santé ingestion: {e}")
            return {'error': str(e)}
