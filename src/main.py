# src/main.py
import argparse
import sys
import os
from dotenv import load_dotenv
import logging

# Charger les variables d'environnement
load_dotenv()

# Imports internes
from utils.logger import setup_logging
from lastfm_weather_collector import LastFmWeatherCollector
from data_analyzer import DataAnalyzer
from ingestion.batch_ingestor import BatchIngestor
from etl.etl_orchestrator import ETLOrchestrator

# Logging global
setup_logging()
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Music Weather Trends Analyzer')

    # Modes principaux
    parser.add_argument('--monitor', action='store_true', help='Lancer le monitoring continu')
    parser.add_argument('--test', action='store_true', help='Lancer un test rapide')
    parser.add_argument('--analyze', action='store_true', help='Analyser les données existantes')

    # Ingestion / ETL
    parser.add_argument('--ingest-batch', action='store_true', help='Lancer l’ingestion batch (tout le batch)')
    parser.add_argument('--batch-size', type=int, default=None, help='Nombre de villes à ingérer (pour test)')
    parser.add_argument('--run-etl', action='store_true', help='Lancer la pipeline ETL complète')
    parser.add_argument('--etl-process-all', action='store_true', help='Pour ETL: traiter tous les fichiers bruts')
    parser.add_argument('--interval', type=int, default=3600, help='Intervalle de collecte en secondes (pour --monitor)')
    parser.add_argument('--cities', type=str, help='Liste de villes séparées par des virgules pour override temporaire')

    args = parser.parse_args()

    # Override villes via CLI
    if args.cities:
        os.environ['CITIES'] = args.cities
        logger.info(f"Override CITIES via CLI: {args.cities}")

    # Instanciation du collector
    try:
        collector = LastFmWeatherCollector()
    except Exception as e:
        logger.error(f"Erreur initialisation LastFmWeatherCollector: {e}")
        collector = None

    # Décider si on lance l'ETL automatiquement
    auto_etl = os.getenv("AUTO_RUN_ETL", "false").lower() == "true"
    should_run_etl = args.run_etl or auto_etl

    # Routing principal
    if args.test:
        if not collector:
            logger.error("Collector non initialisé — test impossible")
            sys.exit(1)
        run_test(collector)

    # elif args.analyze:
    #     run_analysis()

    elif args.ingest_batch:
        run_batch_ingestion(batch_size=args.batch_size)

    elif args.monitor:
        if not collector:
            logger.error("Collector non initialisé — monitoring impossible")
            sys.exit(1)
        collector.run_continuous_monitoring(interval_minutes=max(1, args.interval // 60))

    elif should_run_etl:
        run_etl(process_all=True)

    else:
        parser.print_help()


# -----------------------
# Fonctions utilitaires
# -----------------------
def run_test(collector: LastFmWeatherCollector):
    print("🧪 Test rapide du système...")
    try:
        test_city = 'Paris'
        test_country = 'France'
        data = collector.collect_city_data(test_city, test_country)
        if data:
            print("✅ Test réussi!")
            analyzer = DataAnalyzer()
            try:
                insights = analyzer.get_quick_insights()
                print(insights)
                print("📊 Analyse des données...")
                sys.exit(1)
            except Exception as e:
                logger.warning(f"Analyse rapide impossible: {e}")
        else:
            print("❌ Test échoué — aucune donnée collectée")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Erreur test: {e}")
        sys.exit(1)


# def run_analysis():
#     print("📊 Analyse des données...")
#     try:
#         analyzer = DataAnalyzer()
#         report = analyzer.get_quick_insights()
#         print(report)
#         analyzer.create_visualizations()
#     except Exception as e:
#         logger.error(f"Erreur analyse: {e}")
#         sys.exit(1)


def run_batch_ingestion(batch_size: int = None):
    print("📥 Lancement ingestion batch...")
    try:
        batch = BatchIngestor()
        result = batch.run_batch_ingestion(batch_size=batch_size)
        stats = result.get('batch_stats', {})
        print(f"📊 Batch terminé: {stats.get('total_cities_processed', 0)} villes, "
              f"{stats.get('total_records_ingested', 0)} records, "
              f"success_rate={stats.get('success_rate', 0):.2f}%")
        logger.info(f"Batch ingestion result: {stats}")
    except Exception as e:
        logger.error(f"Erreur lors du batch ingestion: {e}")
        sys.exit(1)


def run_etl(process_all: bool = False):
    print("🛠️  Lancement pipeline ETL...")
    try:
        orchestrator = ETLOrchestrator()
        result = orchestrator.run_etl_batch(process_all=process_all)
        stats = result.get('batch_stats', {})
        print(f"📊 ETL terminé: {stats.get('total_files_processed', 0)} fichiers, "
              f"{stats.get('total_records_loaded', 0)} records, "
              f"success_rate={stats.get('success_rate', 0):.2f}%")
        try:
            health = orchestrator.get_etl_health()
            print("🔎 ETL Health:", health)
        except Exception:
            logger.debug("Impossible de récupérer health ETL")
    except Exception as e:
        logger.error(f"Erreur lors de l'ETL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
