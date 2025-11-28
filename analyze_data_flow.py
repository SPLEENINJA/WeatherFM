# analyze_data_flow.py
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json

def analyze_data_flow():
    """Analyse complète du flux de données"""
    db_path = 'data/lastfm_weather.db'
    db_path2= 'data/processed_music_weather.db'

    try:
        conn = sqlite3.connect(db_path)
        
        print("🎵 🌦️  ANALYSE DU FLUX DE DONNÉES")
        print("=" * 60)
        
        # 1. STATISTIQUES GÉNÉRALES
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM city_music_trends")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM city_music_trends")
        time_range = cursor.fetchone()
        
        print(f"📊 TOTAL DES ENREGISTREMENTS: {total_records}")
        print(f"⏰ PÉRIODE: {time_range[0]} à {time_range[1]}")
        print()
        
        # 2. RÉPARTITION PAR VILLE
        print("🏙️  RÉPARTITION PAR VILLE:")
        df_cities = pd.read_sql("SELECT city, COUNT(*) as count FROM city_music_trends GROUP BY city ORDER BY count DESC", conn)
        print(df_cities.to_string(index=False))
        print()
        
        # 3. ANALYSE MÉTÉO vs HUMEUR
        print("🌤️  CORRÉLATION MÉTÉO-HUMEUR:")
        df_weather_mood = pd.read_sql("""
            SELECT weather_main, mood_category, COUNT(*) as count 
            FROM city_music_trends 
            GROUP BY weather_main, mood_category 
            ORDER BY weather_main, count DESC
        """, conn)
        print(df_weather_mood.to_string(index=False))
        print()
        
        # 4. TOP ARTISTES
        print("👑 TOP 10 ARTISTES:")
        df_artists = pd.read_sql("""
            SELECT artist_name, COUNT(*) as count, AVG(listeners) as avg_listeners
            FROM city_music_trends 
            GROUP BY artist_name 
            ORDER BY count DESC 
            LIMIT 10
        """, conn)
        print(df_artists.to_string(index=False))
        print()
        
        # 5. DONNÉES MÉTÉO COLLECTÉES
        print("🌡️  DONNÉES MÉTÉO COLLECTÉES:")
        df_weather = pd.read_sql("""
            SELECT 
                weather_main,
                weather_description,
                COUNT(*) as occurrences,
                AVG(temperature) as avg_temp,
                AVG(humidity) as avg_humidity
            FROM city_music_trends 
            GROUP BY weather_main, weather_description
            ORDER BY occurrences DESC
        """, conn)
        print(df_weather.to_string(index=False))
        print()
        
        # 6. EXEMPLE DE DONNÉES RÉCENTES
        print("🎵 DERNIÈRES DONNÉES COLLECTÉES:")
        df_recent = pd.read_sql("""
            SELECT 
                timestamp,
                city,
                artist_name,
                track_name,
                mood_category,
                weather_main,
                temperature
            FROM city_music_trends 
            ORDER BY timestamp DESC 
            LIMIT 5
        """, conn)
        print(df_recent.to_string(index=False))
        conn = sqlite3.connect(db_path2)
        df_sound= pd.read_sql("""select * from soundcharts_tracks""", conn)
        print("\n🎵 DONNÉES SOUNDCHARTS TRACKS :") 
        print(df_sound.head().to_string(index=False))

        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de l'analyse: {e}")
        return False

def check_data_quality():
    """Vérifie la qualité des données collectées"""
    db_path = 'data/lastfm_weather.db'
    db_path2= 'data/processed_music_weather.db'
    try:
        conn = sqlite3.connect(db_path)
        print("\n🔍 QUALITÉ DES DONNÉES")
        print("=" * 40)
        
        # Vérification des valeurs nulles
        checks1 = [
            ("Artistes manquants", "SELECT COUNT(*) FROM city_music_trends WHERE artist_name = 'Unknown'"),
            ("Titres manquants", "SELECT COUNT(*) FROM city_music_trends WHERE track_name = 'Unknown'"),
            ("Météo manquante", "SELECT COUNT(*) FROM city_music_trends WHERE weather_main IS NULL"),
            ("Humeur manquante", "SELECT COUNT(*) FROM city_music_trends WHERE mood_category IS NULL OR mood_category = 'neutral'"),
        ]

        checks2=[("Infos manquantes ","select count(*) from soundcharts_tracks where release_date is null or release_date='None'")]
        
        for check_name, query in checks1:
            cursor = conn.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            status = "✅ OK" if count == 0 else f"⚠️  {count} problèmes"
            print(f"{check_name}: {status}")
        
        conn.close()
        conn = sqlite3.connect(db_path2)
        for check_name, query in checks2:
            cursor = conn.cursor()
            cursor.execute(query)
            count = cursor.fetchone()[0]
            status = "✅ OK" if count == 0 else f"⚠️  {count} problèmes"
            print(f"{check_name}: {status}")
        
    except Exception as e:
        print(f"❌ Erreur vérification qualité: {e}")

if __name__ == "__main__":
    analyze_data_flow()
    check_data_quality()