# src/utils/logger.py
import logging
import sys
import os
from datetime import datetime

def setup_logging():
    """Configure le système de logging"""
    
    # Créer le dossier logs si nécessaire
    os.makedirs('logs', exist_ok=True)
    
    # Format des logs
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configuration de base
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(f'logs/collector_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Logger spécifique pour l'application
    logger = logging.getLogger('MusicWeatherAnalyzer')
    
    return logger