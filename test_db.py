# test_simple.py
import sqlite3
import os

def test_database():
    db_path = './data/lastfm_weather.db'  # Mode natif
    # Ou pour Docker: db_path = 'data/lastfm_weather.db' dans le container
    
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Tester la lecture
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            print(f"📋 Tables dans la base: {[t[0] for t in tables]}")
            
            # Compter les enregistrements
            cursor.execute("SELECT COUNT(*) FROM city_music_trends")
            count = cursor.fetchone()[0]
            print(f"📊 Nombre d'enregistrements: {count}")
            
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Erreur base de données: {e}")
            return False
    else:
        print(f"❌ Fichier {db_path} non trouvé")
        return False

if __name__ == "__main__":
    test_database()