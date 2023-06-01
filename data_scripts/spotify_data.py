import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import configparser

import pandas as pd

import boto3
import pickle
from datetime import datetime

# assuming the 'pipeline.conf' file is in the same location as the 'pipeline_template.conf' file
current_directory = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.dirname(current_directory)
file_conf = os.path.join(parent_directory, 'pipeline.conf')
parser = configparser.ConfigParser()
parser.read(file_conf)

spotipy_client_id = parser.get("spotipy_credentials", "CLIENT_ID")
spotipy_client_secret = parser.get("spotipy_credentials", "CLIENT_SECRET")

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=spotipy_client_id, client_secret=spotipy_client_secret))

uri = "spotify:playlist:37i9dQZEVXbLRQDuF5jeBp"
tracks = sp.playlist_tracks(uri.split(":")[2])

top50_data = []
albums_data = []
artists_data = []
tracks_info = []

for rank, track in enumerate(tracks['items']):
    rank_no = rank + 1
    track_id = track['track']['id']
    tracks_artists_id = [artist['id'] for artist in track['track']['artists']]
    album_id = track['track']['album']['id']
    
    album_pg = sp.album(album_id)
    album_name = album_pg['name']
    album_type = album_pg['album_type']
    album_artists_id = [artist['id'] for artist in album_pg['artists']]
    album_label = album_pg['label']
    album_popularity = album_pg['popularity']
    album_release = album_pg['release_date']
    album_total_tracks = album_pg['total_tracks']
    

    for artist_id in tracks_artists_id:
        artist_pg = sp.artist(artist_id)
        artist_name = artist_pg['name']
        artist_popularity = artist_pg['popularity']
        artist_genres = artist_pg['genres']
        rows_artists = {'artist_id': artist_id,
                        'artist_name': artist_name,
                        'artist_popularity': artist_popularity,
                        'artist_genres': artist_genres}
        artists_data.append(rows_artists)

    audio_feats = sp.audio_features(track_id)[0]
    acousticness= audio_feats['acousticness']
    danceability = audio_feats['danceability']
    energy = audio_feats['energy']
    key = audio_feats['key']
    loudness = audio_feats['loudness']
    mode = audio_feats['mode']
    speechiness = audio_feats['speechiness']
    instrumentalness = audio_feats['instrumentalness']
    liveness = audio_feats['liveness']
    valence = audio_feats['valence']
    tempo = audio_feats['tempo']
    duration_ms = audio_feats['duration_ms']
    time_signature = audio_feats['time_signature']

    track_name = track['track']['name']
    track_popularity = track['track']['popularity']

    date_ = datetime.now().strftime("%Y-%m-%d")
    rows_top50 = {'date_on_top': date_,
                  'rank_no': rank_no,
                  'track_id': track_id,
                  'track_name': track_name,
                  'artists_ids': tracks_artists_id,
                  'album_id': album_id}
    top50_data.append(rows_top50)

    rows_albums = {'album_id': album_id,
                   'album_name': album_name,
                   'album_label': album_label,
                   'album_popularity': album_popularity,
                   'album_release_date': album_release,
                   'album_total_tracks': album_total_tracks,
                   'album_artists_ids': album_artists_id,
                   'album_type': album_type}
    albums_data.append(rows_albums)

    rows_feats = {'track_id': track_id,
                  'acousticness': acousticness,
                  'danceability': danceability,
                  'energy': energy,
                  'key': key,
                  'loudness': loudness,
                  'mode': mode,
                  'speechiness': speechiness,
                  'instrumentalness': instrumentalness,
                  'liveness': liveness,
                  'valence': valence,
                  'tempo': tempo,
                  'duration_ms': duration_ms,
                  'time_signature': time_signature}
    tracks_info.append(rows_feats)


top50_df = pd.DataFrame(top50_data)
artists_df = pd.DataFrame(artists_data)
albums_df = pd.DataFrame(albums_data)
audio_feats_df = pd.DataFrame(tracks_info)

# want to drop duplicates
# will leave this but not needed due to use of merge() from sqlalchemy
artists_df = artists_df.iloc[artists_df.astype(str).drop_duplicates().index]
albums_df = albums_df.iloc[albums_df.astype(str).drop_duplicates().index]
audio_feats_df = audio_feats_df.iloc[audio_feats_df.astype(str).drop_duplicates().index]

# convert dataframes to pickle files
pickled_top50_df = pickle.dumps(top50_df)
pickled_artists_df = pickle.dumps(artists_df)
pickled_albums_df = pickle.dumps(albums_df)
pickled_audio_feats_df = pickle.dumps(audio_feats_df)

bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)

key_for_top50 = 'data/top50_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))
key_for_artists_df = 'data/artists_df_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))
key_for_albums_df = 'data/albums_df_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))
key_for_audio_feats_df = 'data/audio_feats_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))

bucket.put_object(Key = key_for_top50, Body = pickled_top50_df)
bucket.put_object(Key = key_for_artists_df, Body = pickled_artists_df)
bucket.put_object(Key = key_for_albums_df, Body = pickled_albums_df)
bucket.put_object(Key = key_for_audio_feats_df, Body = pickled_audio_feats_df)
