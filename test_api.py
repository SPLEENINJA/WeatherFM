# test_api.py
import requests
import os
from dotenv import load_dotenv

load_dotenv()

api_key1 = "c88418057941ab301fb784f4a70ba79d"
api_key2= os.getenv('LASTFM_API_KEY')
# api_key3= JCAPEL-API_6AFD60B6 & app_token = 883c8f4076b04c42


url = f"https://api.openweathermap.org/data/2.5/weather?q=Paris&appid={api_key1}&units=metric"
url2 = f"http://ws.audioscrobbler.com/2.0/?method=geo.gettoptracks&country=France&api_key={api_key2}&format=json&limit=1"

response1 = requests.get(url)
print(f"Status: {response1.status_code}")
print(f"Response: {response1.text}")


response2 = requests.get(url2)
print(f"Status: {response2.status_code}")
print(f"Response: {response2.text}")