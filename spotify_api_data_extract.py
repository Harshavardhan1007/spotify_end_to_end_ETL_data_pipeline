"""
Extracts data from Spotify API and uploads it to an S3 bucket.

This function retrieves track information from a specified Spotify playlist,
converts it to JSON format, and uploads it to an S3 bucket for further processing.

Parameters:
- event: A dictionary containing information about the event that triggered the Lambda function.
         In this case, it is not utilized, but it's a standard parameter for Lambda functions.
- context: A LambdaContext object providing runtime information about the Lambda function execution.
           Not used in this function.

Returns:
- None

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
7. Uploads the JSON data to the specified S3 bucket and directory.

Note:
- This function assumes that the specified playlist URI corresponds to a publicly accessible Spotify playlist.
- The AWS Lambda function must have appropriate permissions to interact with the specified S3 bucket.
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
