# src/visualizer.py
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import sqlite3

class DataVisualizer:
    def __init__(self, db_path='/data/lastfm_weather.db'):
        self.db_path = db_path
    
    def create_weather_mood_heatmap(self):
        """Crée une heatmap météo vs humeur"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM city_music_trends", conn)
        
        if df.empty:
            print("❌ Pas de données pour la visualisation")
            return
        
        plt.figure(figsize=(12, 8))
        
        # Heatmap
        pivot_data = pd.crosstab(df['weather_main'], df['mood_category'])
        sns.heatmap(pivot_data, annot=True, fmt='d', cmap='YlOrRd')
        plt.title('Corrélation Météo vs Humeur Musicale')
        plt.tight_layout()
        plt.savefig('data/weather_mood_heatmap.png')
        plt.show()
        
        print("✅ Heatmap sauvegardée: data/weather_mood_heatmap.png")

if __name__ == "__main__":
    visualizer = DataVisualizer()
    visualizer.create_weather_mood_heatmap()