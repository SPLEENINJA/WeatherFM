    # test_api_keys.py
import requests
import os
from dotenv import load_dotenv

load_dotenv()

def test_api_keys():
    print("🔑 TEST DES CLÉS API")
    print("=" * 50)
    
    # Test Last.fm
    lastfm_key = os.getenv('LASTFM_API_KEY')
    print(f"Last.fm Key: {lastfm_key[:10]}...{lastfm_key[-5:] if lastfm_key else 'MANQUANT'}")
    
    if lastfm_key:
        url = "http://ws.audioscrobbler.com/2.0/"
        params = {
            'method': 'geo.gettoptracks',
            'country': 'France',
            'api_key': lastfm_key,
            'format': 'json',
            'limit': 1
        }
        try:
            response = requests.get(url, params=params, timeout=10)
            print(f"Last.fm Status: {response.status_code}")
            if response.status_code == 200:
                print("✅ Last.fm API fonctionne")
            else:
                print(f"❌ Last.fm Erreur: {response.text[:100]}")
        except Exception as e:
            print(f"❌ Last.fm Exception: {e}")
    
    # Test OpenWeather
    weather_key = os.getenv('OPENWEATHER_API_KEY')
    print(f"\nOpenWeather Key: {weather_key[:10]}...{weather_key[-5:] if weather_key else 'MANQUANT'}")
    
    if weather_key:
        url = f"https://api.openweathermap.org/data/2.5/weather?q=Paris&appid={weather_key}&units=metric"
        try:
            response = requests.get(url, timeout=10)
            print(f"OpenWeather Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"✅ OpenWeather OK - {data['weather'][0]['description']}, {data['main']['temp']}°C")
            else:
                print(f"❌ OpenWeather Erreur: {response.text[:100]}")
        except Exception as e:
            print(f"❌ OpenWeather Exception: {e}")

if __name__ == "__main__":
    test_api_keys()