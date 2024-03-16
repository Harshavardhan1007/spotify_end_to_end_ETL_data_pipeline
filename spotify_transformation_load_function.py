"""
Transforms Spotify data extracted from Amazon S3 and loads it into different S3 buckets.

This AWS Lambda function is triggered when new data is dumped into the 'extract' S3 bucket.
It extracts track information from a specified Spotify playlist, cleans the data, formats it properly,
and loads it into different S3 buckets for further processing.

Environment Variables:
- client_id: Spotify API client ID stored as an environment variable.
- client_secret: Spotify API client secret stored as an environment variable.

Dependencies:
- spotipy: A lightweight Python library for the Spotify Web API.
- boto3: The AWS SDK for Python, used for interacting with Amazon S3.

Execution Steps:
1. Retrieves Spotify API client ID and client secret from environment variables.
2. Creates a Spotify client object using the client credentials manager.
3. Fetches playlist information from the Spotify API using a predefined playlist URI.
4. Converts the retrieved data to JSON format.
5. Initializes an S3 client object.
6. Generates a unique file name using the current timestamp.
7. Uploads the JSON data to the specified 'raw_data/to_be_processed/' directory in the 'spotify-etl-project-2024' S3 bucket.

Note:
- This function assumes that the specified playlist URI corresponds to a publicly accessible Spotify playlist.
- The AWS Lambda function must have appropriate permissions to interact with the specified S3 bucket.
- This function is designed to be triggered when new data is dumped into the 'extract' S3 bucket.
"""

# Importing necessary modules
import json
import os
import spotipy
import boto3
from datetime import datetime
from spotipy.oauth2 import SpotifyClientCredentials

def lambda_handler(event, context):
    # Retrieving client ID and client secret from environment variables
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    # Creating a Spotify client object
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    # Fetching playlist information from Spotify API
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    spotify_data = sp.playlist_tracks(playlist_URI)
    
    # Initializing S3 client
    s3_client = boto3.client('s3')
    
    # Generating file name using current timestamp
    file_name = "spotify_raw_" + str(datetime.now()) + ".json"
    
    # Uploading JSON data to S3 bucket
    s3_client.put_object(
        Bucket="spotify-etl-project-2024",
        Key="raw_data/to_be_processed/" + file_name,
        Body=json.dumps(spotify_data)
    )
import json
import boto3
from datetime import datetime
import pandas as pd
from io import StringIO


def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id':album_id,'name':album_name,'realese_date':album_release_date,
                          'total_tracks':album_total_tracks,'url':album_url}
        album_list.append(album_element)
    return album_list
        
def artist(data):
    artist_list=[]
    for row in data['items']:
        for key,value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_dict = {'artist_id':artist['id'], 'artist_name':artist['name'],'external_url':artist['href']}
                    artist_list.append(artist_dict)
    return artist_list
                    
def songs(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,
                        'url':song_url,'popularity':song_popularity,'song_added':song_added,
                        'album_id':album_id,'artist_id':artist_id}
        song_list.append(song_element)
    return song_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "spotify-etl-project-2024"
    Key = "raw_data/to_be_processed/"
    
    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split(".")[-1] == "json":
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response["Body"]
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = songs(data)
        
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])
        album_df['realese_date'] = pd.to_datetime(album_df['realese_date'],format='mixed',yearfirst=True)

        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])

        song_df = pd.DataFrame.from_dict(song_list)
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
        album_key ="transformed_data/album_data/albums_transformed" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)
        
        artist_key ="transformed_data/artist_data/artist_transformed" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        
        songs_key ="transformed_data/songs_data/songs_transformed" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body=song_content)
        
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket' : Bucket,
            'Key' : key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, "raw_data/processed/" + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()
