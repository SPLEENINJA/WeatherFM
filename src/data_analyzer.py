# src/data_analyzer.py
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

class DataAnalyzer:
    def __init__(self, db_path='/data/lastfm_weather.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
    
    def get_quick_insights(self):
        """Retourne des insights rapides"""
        df = pd.read_sql_query("""
            SELECT * FROM city_music_trends 
            WHERE timestamp >= datetime('now', '-7 days')
        """, self.conn)
        
        if df.empty:
            return "❌ Pas assez de données pour l'analyse"
        
        insights = []
        
        # Corrélation météo-humeur
        weather_mood = df.groupby(['weather_main', 'mood_category']).size().unstack(fill_value=0)
        
        insights.append("🌤️  CORRÉLATION MÉTÉO-HUMEUR:")
        for weather in weather_mood.index:
            dominant_mood = weather_mood.loc[weather].idxmax()
            insights.append(f"   {weather}: {dominant_mood.upper()}")
        
        # Top artistes
        top_artists = df['artist_name'].value_counts().head(5)
        insights.append("/n👑 TOP 5 ARTISTES:")
        for artist, count in top_artists.items():
            insights.append(f"   🎵 {artist} ({count} appearances)")
        
        return "/n".join(insights)
    
    def create_visualizations(self):
        """Crée les visualisations principales"""
        df = pd.read_sql_query("SELECT * FROM city_music_trends", self.conn)
        
        if df.empty:
            return
        
        # Configuration des plots
        plt.style.use('seaborn-v0_8')
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # 1. Distribution des humeurs
        df['mood_category'].value_counts().plot.pie(ax=axes[0,0], autopct='%1.1f%%')
        axes[0,0].set_title('Distribution des Humeurs Musicales')
        
        # 2. Humeur par météo
        weather_mood = df.groupby(['weather_main', 'mood_category']).size().unstack()
        weather_mood.plot(kind='bar', ax=axes[0,1], stacked=True)
        axes[0,1].set_title('Humeur Musicale par Type de Météo')
        axes[0,1].tick_params(axis='x', rotation=45)
        
        # 3. Température vs Humeur
        sns.boxplot(data=df, x='mood_category', y='temperature', ax=axes[1,0])
        axes[1,0].set_title('Distribution des Températures par Humeur')
        
        # 4. Top villes par activité
        df['city'].value_counts().head(10).plot(kind='bar', ax=axes[1,1])
        axes[1,1].set_title('Top 10 Villes par Nombre de Tracks')
        axes[1,1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig('../data/weather_music_insights.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        print("✅ Visualisations sauvegardées dans data/weather_music_insights.png")

if __name__ == "__main__":
    analyzer = DataAnalyzer()
    insights = analyzer.get_quick_insights()
    print(insights)
    analyzer.create_visualizations()
    print("HAHAHAHHAHAHAHHAHA")