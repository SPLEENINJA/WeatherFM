# debug_etl.py
import os
import json
import sys
sys.path.insert(0, 'src')

def debug_raw_file():
    """Debug le fichier brut pour voir sa structure"""
    raw_files = [f for f in os.listdir('data/raw') if f.endswith('.json')]
    
    if not raw_files:
        print("❌ Aucun fichier brut trouvé")
        return
    
    sample_file = os.path.join('data/raw', raw_files[0])
    print(f"🔍 Analyse de: {sample_file}")
    
    with open(sample_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print("📊 Structure du fichier:")
    print(f"  - Metadata: {list(data.get('metadata', {}).keys())}")
    print(f"  - Last.fm data: {bool(data.get('lastfm_data'))}")
    print(f"  - Weather data: {bool(data.get('weather_data'))}")
    
    if data.get('lastfm_data'):
        tracks = data['lastfm_data'].get('tracks', {})
        print(f"  - Tracks found: {bool(tracks)}")
        if tracks:
            track_list = tracks.get('track', [])
            print(f"  - Number of tracks: {len(track_list)}")
            if track_list:
                print(f"  - First track: {track_list[0].get('name', 'Unknown')}")

if __name__ == "__main__":
    debug_raw_file()