import os
import sqlite3

class DatabaseManager:
    def __init__(self, db_filename="lastfm_weather.db"):
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_dir = os.path.join(BASE_DIR, "data")
        os.makedirs(self.data_dir, exist_ok=True)

        self.db_path = os.path.join(self.data_dir, db_filename)
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

        self._create_tables()

    def _create_tables(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS city_music_trends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT,
                country TEXT,
                artist TEXT,
                track TEXT,
                listeners INTEGER,
                timestamp TEXT
            )
        """)

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                artist TEXT,
                track TEXT,
                total_listeners INTEGER
            )
        """)

        self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_city_timestamp 
                ON city_music_trends(city, timestamp)
            ''')
        
        self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_weather_mood 
                ON city_music_trends(weather_main, mood_category)
            ''')
        
        self.conn.commit()

    def insert_city_music(self, data):
        query = """INSERT INTO city_music_trends 
                   (city, country, artist, track, listeners, timestamp) 
                   VALUES (?, ?, ?, ?, ?, ?)"""
        self.cursor.execute(query, data)
        self.conn.commit()

    def close(self):
        self.conn.close()

if __name__ == "__main__":
    db_manager = DatabaseManager()
    print(f"Database initialized at {db_manager.db_path}")
    db_manager.close()