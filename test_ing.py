# test_ingestion.py
from src.ingestion.batch_ingestor import BatchIngestor

def test_ingestion():
    ingestor = BatchIngestor()
    
    # Test single city
    result = ingestor.ingestor.ingest_city_data('Paris', 'France')
    print(f"Résultat ingestion: {result}")
    
    # Ou batch complet
    batch_result = ingestor.run_batch_ingestion()
    print(f"Résultat batch: {batch_result['batch_stats']}")
    
    # Santé du système
    health = ingestor.get_ingestion_health()
    print(f"Santé ingestion: {health}")

if __name__ == "__main__":
    test_ingestion()