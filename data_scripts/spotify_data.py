import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import configparser

import pandas as pd

import boto3
import pickle
from datetime import datetime
import pprint

#this_folder = os.path.dirname(os.path.abspath(__file__))
#file_conf = os.path.join(this_folder, 'pipeline.conf')

parser = configparser.ConfigParser()
# instead of file_conf, write the absolute path of your pipeline.conf file, if you choose to get credentials this way
#parser.read(file_conf)
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
                   'album_artists_ids': album_artists_id}
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

#before pickle and S3, need to break up columns containing arrays/lists

# Ultimately want to make it easy to query artists

# Will make each entry in the artists column have the same length was the max amount
# and fill the empty positions with 'None' so that pgsql can make sense of it

# first get largest amount of artists
max_artists_track = top50_df['artists_ids'].apply(len).max()
max_artists_album = albums_df['album_artists_ids'].apply(len).max()


# temp f(list in column entry) = ((max_artists_track - len(list in 'artists_ids' column entry)) * [None]) + x
# temp g(list in column entry) = ((max_artists_album - len(list in 'album_artists_ids' column entry)) * [None]) + x
# Note: Use [None] to get in form of list
# we + x because we want to add None values on to each list to make them equal in len

# apply functions to the specified columns

top50_df['artists_ids'] = top50_df['artists_ids'].apply(lambda x: x + ((max_artists_track - len(x)) * [None]))

albums_df['album_artists_ids'] = albums_df['album_artists_ids'].apply(lambda x: x + ((max_artists_album - len(x)) * [None]))

# can use .apply(pd.Series) on each column; it would create a separate dataframe, so would need to concat and drop old
# same process with .to_list() but .to_list() is more efficient
# .to_list() may need the lists to be the same lenght, but not neccessarily the case with pd.Series (double check if this is the cas)

# To set the new column names
top50_artists_cols = [f'artist{i + 1}' for i in range(max_artists_track)]
albums_artists_cols = [f'artist{i + 1}' for i in range(max_artists_album)]

top50_new = pd.DataFrame(top50_df['artists_ids'].to_list(), columns=top50_artists_cols)
top50_df = pd.concat([top50_df, top50_new], axis=1)
top50_df = top50_df.drop(columns='artists_ids')

albums_new = pd.DataFrame(albums_df['album_artists_ids'].to_list(), columns=albums_artists_cols)
albums_df = pd.concat([albums_df,albums_new], axis=1)
albums_df = albums_df.drop(columns='album_artists_ids')

# want to drop duplicates to be able to set PK when creating table in database
artists_df = artists_df.iloc[artists_df.astype(str).drop_duplicates().index]
albums_df = albums_df.drop_duplicates()
audio_feats_df = audio_feats_df.drop_duplicates()

# # if we took the pd.Series approach, process below is to rename columns
# # get_loc accepts value as input and gets index of where the value is given
# # want location/index of artist column amongst the columns
# top50_index = top50_df.columns.get_loc('artists_ids')
# albums_index = albums_df.columns.get_loc('albums_artists_ids')

# # To create map from old to new column names
# # Will use .iloc to get subset of columns we want to focus on to rename
# # Using zip will help us create dictionary or generally the map we want
# # can use range(max_artists_track) since we will slice dataframe to isolate the desired columns
# top50_cols_map = dict(zip(range(max_artists_track),top50_artists_cols))
# albums_cols_map = dict(zip(range(max_artists_album),albums_artists_cols))

# #python slicing is exclusive with end value
# top50_subset = top50_df.iloc[:, top50_index: top50_index + max_artists_track]
# albums_subset = albums_df.iloc[:, albums_index: albums_index + max_artists_album]

# top50_subset = top50_subset.rename(columns=top50_cols_map)
# albums_subset = albums_subset.rename(columns=albums_cols_map)


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
