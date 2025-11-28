# src/utils/helpers.py
import json
from datetime import datetime, timedelta
import sqlite3
import os
import dotenv
import shutil

def backup_database_to_host():
    """Copie la base SQLite du container vers l'hôte pour sauvegarde"""
    try:
        backup_dir = 'backups'
        os.makedirs(backup_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f"{backup_dir}/backup_{timestamp}.db"
        
        # Copier depuis le chemin container vers l'hôte
        if os.path.exists('/app/data/lastfm_weather.db'):
            shutil.copy2('/app/data/lastfm_weather.db', backup_file)
            return backup_file
        return None
    except Exception as e:
        print(f"❌ Erreur sauvegarde: {e}")
        return None

def load_config():
    """Charge la configuration depuis les variables d'environnement"""
    cities = os.getenv('CITIES').split(',')
    countries = os.getenv('COUNTRIES').split(',')
    
    return {
        'cities': dict(zip(cities, countries)),
        'interval': int(os.getenv('COLLECTION_INTERVAL', 3600)),
        'max_retries': int(os.getenv('MAX_RETRIES', 3))
    }

def backup_database():
    """Crée une sauvegarde de la base de données"""
    backup_dir = os.getenv('DB_BACKUP_DIR', 'data/backup')
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = f"{backup_dir}/backup_{timestamp}.db"
    
    # Copie simple pour SQLite
    import shutil
    shutil.copy2('data/lastfm_weather.db', backup_file)
    
    return backup_file

def validate_environment():
    """Valide que toutes les variables d'environnement nécessaires sont présentes"""
    from dotenv import load_dotenv
    load_dotenv()
    required_vars = ['LASTFM_API_KEY', 'OPENWEATHER_API_KEY']
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise EnvironmentError(f"Variables manquantes: {', '.join(missing_vars)}")
    
    return True