# test_etl_final.py
import os
import sys
from dotenv import load_dotenv
from src.etl.etl_pipeline import ETLPipeline
from src.etl.etl_orchestrator import ETLOrchestrator

# Configuration du path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
load_dotenv()

def test_etl_final():
    print("🧪 TEST ETL FINAL - PIPELINE COMPLET")
    print("=" * 60)
    
    # Vérifier le fichier brut
    raw_files = [f for f in os.listdir('data/raw') if f.endswith('.json')]
    print(f"📁 Fichier brut trouvé: {raw_files[0] if raw_files else 'Aucun'}")
    
    if not raw_files:
        print("❌ Aucun fichier brut - exécutez d'abord l'ingestion")
        return
    
    
    
    # Initialiser l'orchestrateur ETL
    print("🚀 Initialisation de l'ETL...")
    orchestrator = ETLOrchestrator()
    
    # Exécuter l'ETL sur le fichier brut
    print("🔄 Exécution du pipeline ETL...")
    result = orchestrator.run_etl_batch(process_all=True)
    
    # Afficher les résultats
    print("\n📊 RÉSULTATS ETL")
    print("=" * 40)
    batch_stats = result['batch_stats']
    print(f"• Fichiers traités: {batch_stats['total_files_processed']}")
    print(f"• ETL réussis: {batch_stats['successful_etls']}")
    print(f"• ETL échoués: {batch_stats['failed_etls']}")
    print(f"• Taux de succès: {batch_stats['success_rate']:.1f}%")
    print(f"• Records chargés: {batch_stats['total_records_loaded']}")
    
    # Détails par fichier
    print("\n📋 DÉTAILS PAR FICHIER")
    print("=" * 40)
    for detail in result['detailed_results']:
        status_icon = "✅" if detail.get('status') == 'success' else "❌"
        print(f"{status_icon} {os.path.basename(detail['file'])}")
        if detail.get('status') == 'success':
            print(f"   📊 {detail['records_loaded']}/{detail['records_extracted']} records")
            print(f"   ⏱️  {detail.get('processing_time', 0):.2f}s")
    
    # Vérifier les données dans la base
    print("\n🎵 DONNÉES TRANSFORMÉES")
    print("=" * 40)
    
    etl = ETLPipeline()
    
    conn = etl._get_connection()
    cursor = conn.cursor()
    
    # Statistiques générales
    cursor.execute("SELECT COUNT(*) FROM processed_tracks")
    total_tracks = cursor.fetchone()[0]
    print(f"• Total tracks transformées: {total_tracks}")
    
    cursor.execute("SELECT COUNT(DISTINCT city) FROM processed_tracks")
    cities_count = cursor.fetchone()[0]
    print(f"• Villes différentes: {cities_count}")
    
    cursor.execute("SELECT COUNT(DISTINCT mood_category) FROM processed_tracks")
    moods_count = cursor.fetchone()[0]
    print(f"• Humeurs détectées: {moods_count}")
    
    # Aperçu des données
    if total_tracks > 0:
        print("\n👁️  APERÇU DES DONNÉES:")
        cursor.execute("""
            SELECT city, artist_name, track_name, mood_category, weather_condition, temperature
            FROM processed_tracks 
            ORDER BY processed_at DESC 
            LIMIT 5
        """)
        
        for i, row in enumerate(cursor.fetchall(), 1):
            city, artist, track, mood, weather, temp = row
            print(f"  {i}. 🎵 {city}: {artist} - {track}")
            print(f"     😊 {mood} | 🌤️  {weather} | 🌡️ {temp}°C")
    
    conn.close()
    
    # Santé du système ETL
    print("\n🏥 SANTÉ DU SYSTÈME ETL")
    print("=" * 40)
    health = orchestrator.get_etl_health()
    for key, value in health.items():
        print(f"• {key.replace('_', ' ').title()}: {value}")

if __name__ == "__main__":
    test_etl_final()