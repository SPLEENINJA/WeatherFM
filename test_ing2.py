# test_ingestion_fixed.py
import os
import sys
from dotenv import load_dotenv

# Ajouter le chemin src
sys.path.append('src')

load_dotenv()

def test_ingestion_fixed():
    print("🧪 TEST INGESTION CORRIGÉ")
    print("=" * 50)
    
    # Vérifier que les variables sont chargées
    lastfm_key = os.getenv('LASTFM_API_KEY')
    weather_key = os.getenv('OPENWEATHER_API_KEY')
    
    print(f"Last.fm Key from env: {lastfm_key[:8]}...{lastfm_key[-4:] if lastfm_key else 'NONE'}")
    print(f"Weather Key from env: {weather_key[:8]}...{weather_key[-4:] if weather_key else 'NONE'}")
    
    from src.ingestion.batch_ingestor import BatchIngestor
    
    ingestor = BatchIngestor()
    
    # Test single city
    print("\n🍽️  Test ingestion single city...")
    result = ingestor.ingestor.ingest_city_data('Paris', 'France')
    print(f"✅ Résultat: {result.success}")
    print(f"📊 Records: {result.records_ingested}")
    print(f"📁 Fichier: {result.raw_data_path}")
    
    if result.errors:
        print(f"❌ Erreurs: {result.errors}")

if __name__ == "__main__":
    test_ingestion_fixed()