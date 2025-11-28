# src/etl/etl_orchestrator.py
import os
import logging
from datetime import datetime
from typing import List, Dict
import glob
import json
from .etl_pipeline import ETLPipeline

class ETLOrchestrator:
    """
    Orchestrateur pour exécuter l'ETL + enrichissement Soundcharts
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.etl_pipeline = ETLPipeline()
        self.raw_data_dir = 'data/raw'
    
    def run_etl_batch(self, process_all: bool = False, do_soundcharts: bool = True) -> Dict:
        """
        Exécute l'ETL sur les fichiers bruts et lance l'enrichissement Soundcharts
        si activé (par défaut activé).
        """
        
        self.logger.info("🏭 Début batch ETL")
        
        raw_files = self._get_raw_files()
        if not raw_files:
            self.logger.warning("⚠️  Aucun fichier brut trouvé")
            return {'status': 'no_files_found'}
        
        results = []
        for raw_file in raw_files:
            self.logger.info(f"🔄 Traitement ETL: {os.path.basename(raw_file)}")
            
            result = self.etl_pipeline.run_etl_for_raw_file(raw_file)
            results.append(result)
            
            if not process_all and result.get('status') == 'success':
                self.logger.info("✅ Premier fichier traité avec succès - arrêt du batch")
                break
        
        # Calcul stats batch
        batch_stats = self._calculate_batch_stats(results)

        # 🎵 Lancement enrichissement Soundcharts une fois que le batch principal est terminé
        soundcharts_results = None
        if do_soundcharts:
            self.logger.info("🎵 Enrichissement Soundcharts...")
            try:
                soundcharts_results = self.etl_pipeline.enrich_with_soundcharts()
                self.logger.info(f"🎉 Enrichissement Soundcharts terminé ({len(soundcharts_results)} tracks)")
            except Exception as e:
                self.logger.error(f"❌ Échec enrichissement Soundcharts : {e}")

        return {
            'batch_stats': batch_stats,
            'detailed_results': results,
            'soundcharts_enrichment': soundcharts_results
        }
        
    def _get_raw_files(self) -> List[str]:
        """Retourne la liste des fichiers bruts valides"""
        pattern = os.path.join(self.raw_data_dir, '*.json')
        all_files = glob.glob(pattern)

        valid_files = []
        for file_path in all_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                if (data.get('lastfm_data') and data.get('weather_data') and 
                    data.get('lastfm_data', {}).get('tracks', {}).get('track')):
                    valid_files.append(file_path)
                else:
                    self.logger.warning(f"⚠️  Fichier ignoré (structure invalide): {os.path.basename(file_path)}")
            
            except (json.JSONDecodeError, Exception) as e:
                self.logger.warning(f"⚠️  Fichier ignoré (corrompu): {os.path.basename(file_path)} - {e}")

        self.logger.info(f"📁 {len(valid_files)}/{len(all_files)} fichiers valides trouvés")
        return valid_files
    
    def _calculate_batch_stats(self, results: List[Dict]) -> Dict:
        """Calcule les statistiques du batch ETL"""
        total_files = len(results)
        successful_etls = sum(1 for r in results if r.get('status') == 'success')
        total_records = sum(r.get('records_loaded', 0) for r in results)

        return {
            'total_files_processed': total_files,
            'successful_etls': successful_etls,
            'failed_etls': total_files - successful_etls,
            'total_records_loaded': total_records,
            'success_rate': (successful_etls / total_files * 100) if total_files > 0 else 0
        }
    
    def get_etl_health(self) -> Dict:
        """Retourne l'état de santé du système ETL"""
        try:
            conn = self.etl_pipeline._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_etl_runs,
                    AVG(success_rate) as avg_success_rate,
                    SUM(records_loaded) as total_records_loaded,
                    MIN(processed_at) as first_run,
                    MAX(processed_at) as last_run
                FROM etl_stats
            """)
            
            stats = cursor.fetchone()
            conn.close()
            
            return {
                'total_etl_runs': stats[0],
                'average_success_rate': round(stats[1] * 100, 2) if stats[1] else 0,
                'total_records_loaded': stats[2],
                'first_etl_run': stats[3],
                'last_etl_run': stats[4]
            }
        
        except Exception as e:
            self.logger.error(f"❌ Erreur santé ETL: {e}")
            return {'error': str(e)}
