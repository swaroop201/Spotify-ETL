import requests
import base64
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# Spotify API credentials
client_id = '0e87a094eb7e4f0c8ffb113159fce7a5'
client_secret = 'fe938dabab4343c68d54619f460152dd'

def get_token():
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")
    
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    result = requests.post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token

def get_auth_header(token):
    return {"Authorization": "Bearer " + token}

def extract_artist_data(token, artist_name):
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={artist_name}&type=artist&limit=1"
    query_url = url + query
    result = requests.get(query_url, headers=headers)
    json_result = json.loads(result.content)
    
    if 'artists' in json_result and json_result['artists']['items']:
        artist = json_result['artists']['items'][0]
        return {
            "id": artist["id"],
            "name": artist["name"],
            "popularity": artist["popularity"],
            "genres": artist["genres"],
            "followers": artist["followers"]["total"]
        }
    return None

def extract_artist_tracks(token, artist_id):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
    headers = get_auth_header(token)
    params = {
        "include_groups": "album,single",
    }
    result = requests.get(url, headers=headers, params=params)
    json_result = json.loads(result.content)
    
    tracks = []
    for album in json_result.get("items", []):
        album_tracks = extract_album_tracks(token, album['id'])
        for track in album_tracks:
            track["album_name"] = album["name"]
            track["release_date"] = album["release_date"]
        tracks.extend(album_tracks)
    
    return tracks

def extract_album_tracks(token, album_id):
    url = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
    headers = get_auth_header(token)
    result = requests.get(url, headers=headers)
    json_result = json.loads(result.content)
    
    return [
        {
            "track_id": track["id"],
            "track_name": track["name"],
            "popularity": "N/A",  # Placeholder since we don't have this info here
            "external_url": track["external_urls"]["spotify"]
        }
        for track in json_result.get("items", [])
    ]

def transform_data(artist_data, tracks_data):
    # Create DataFrames
    artist_df = pd.DataFrame([artist_data])
    tracks_df = pd.DataFrame(tracks_data)
    
    # Add extraction timestamp
    current_date = datetime.now().strftime("%Y-%m-%d")
    artist_df["extraction_date"] = current_date
    tracks_df["extraction_date"] = current_date
    
    # Add artist_id to tracks DataFrame
    tracks_df["artist_id"] = artist_data["id"]
    
    return artist_df, tracks_df

def data_quality_check(df):
    if df.empty:
        print('DataFrame is empty')
        return False
    
    if df.isnull().values.any():
        print('Null values found')
        return False
    
    return True

def load_to_postgres(artist_df, tracks_df):
    # PostgreSQL connection details
    db_params = {
        'host': 'localhost',
        'database': 'spotify_db',  # Make sure this matches your actual database name
        'user': 'postgres',
        'password': 'password',  # Replace with your actual password
        'port': '5432'  # This is the key change based on your installation
    }
    
    # Create SQLAlchemy engine
    engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")
    
    # Load DataFrames to PostgreSQL
    try:
        # Insert artist data first
        artist_df.to_sql('artists', engine, if_exists='append', index=False)
        print("Artist data loaded successfully.")
        
        # Then insert track data
        tracks_df.to_sql('tracks', engine, if_exists='append', index=False)
        print("Tracks data loaded successfully.")
    except Exception as e:
        print(f"Error loading data: {e}")

def spotify_etl(artist_name):
    token = get_token()
    
    # Extract
    artist_data = extract_artist_data(token, artist_name)
    
    if not artist_data:
        print(f"Could not find artist: {artist_name}")
        return None
    
    tracks_data = extract_artist_tracks(token, artist_data["id"])
    
    # Transform
    artist_df, tracks_df = transform_data(artist_data, tracks_data)
    
    # Load
    if data_quality_check(artist_df) and data_quality_check(tracks_df):
        load_to_postgres(artist_df, tracks_df)
        return True
    else:
        print("Data quality check failed")
        return None

# Main execution (to be used in Airflow DAG)
if __name__ == "__main__":
    artist_name_input = input("Enter the name of the artist: ")
    spotify_etl(artist_name_input)