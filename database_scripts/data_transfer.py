from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
import os, configparser
from database import Artist, Album, Genre, AudioFeatures, TopTrack, Concert, Festival, DataDictionary, Track
from datetime import datetime
import boto3
import pickle

# assuming the 'pipeline.conf' file is in the same location as the 'pipeline_template.conf' file
current_directory = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.dirname(current_directory)
file_conf = os.path.join(parent_directory, 'pipeline.conf')
parser = configparser.ConfigParser()
parser.read(file_conf)

database_url = parser.get("database", "DB_URL")

# making connnection to database, creating session
engine = create_engine(database_url, pool_pre_ping=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()
metadata = MetaData()
metadata.bind = engine

# get aws bucket name
bucket = parser.get("aws_boto_credentials", "bucket_name")
# create a client
s3 = boto3.client('s3')

# to avoid code repetition 
def get_dataframe_from_s3(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        file = response['Body'].read()
        df = pickle.loads(file)
        return df
    except Exception as e:
        print(f"File {key} may not exist in bucket {bucket} or there is an issue \
            with reading or parsing the contents of the file: {str(e)}")   

##

# get all dataframes first, to declutter code below
artists_key = 'data/artists_df_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))
albums_key = 'data/albums_df_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))
top50_key = 'data/top50_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))
audio_key = 'data/audio_feats_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))
concerts_key = 'cleaned_concerts_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))
festivals_key = 'cleaned_festivals_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))

artists_df = get_dataframe_from_s3(bucket, artists_key)
albums_df = get_dataframe_from_s3(bucket, albums_key)
top50_df = get_dataframe_from_s3(bucket, top50_key)
audio_df = get_dataframe_from_s3(bucket, audio_key)
concerts_df = get_dataframe_from_s3(bucket, concerts_key)
festivals_df = get_dataframe_from_s3(bucket, festivals_key)

##

# add data into 'artists' and 'genres', and in turn the association table

# convert DataFrame to a list of dictionaries-- to be in a form we can be insert into the tables
artists_data = artists_df.to_dict('records')

for row in artists_data:
    artist = Artist(artist_id = row['artist_id'],
                    name = row['artist_name'],
                    popularity = row['artist_popularity'])
    genres = row['artist_genres']
    for genre in genres:
        genre_name = Genre(name = genre)
        # to link artist with genre
        artist.genre.append(genre_name)
    # use merge() to handle insert or update process based on PKs
    # otherwise would have to query for existing records and use many conditionals/cases
    session.merge(artist)
session.commit()

##

# insert new albums into 'albums' table
# similar procedure as above
albums_data = albums_df.to_dict('records')

for row in albums_data:
    album = Album(album_id = row['album_id'],
                  title = row['album_name'],
                  label = row['album_label'],
                  album_type = row['album_type'],
                  popularity = row['album_popularity'],
                  release_date = row['album_release_date'],
                  total_tracks = row['album_total_tracks'])
    artists = row['album_artists_ids']
    for artist_id in artists:
        album_artist = Artist(artist_id = artist_id)
        album.artist.append(album_artist)
    session.merge(album)
session.commit()

##

# insert data on songs from top tracks playlist
track_data = top50_df.to_dict('records')

for row in track_data:
    # insert data into 'tracks' with associated artists
    track = Track(track_id = row['track_id'],
                  title = row['track_name'],
                  album_id = row['album_id'])
    artists = row['artists_ids']
    for artist_id in artists:
        track_artist = Artist(artist_id = artist_id)
        track.artist.append(track_artist)
    session.merge(track)
    # insert data into 'top_tracks'
    top_song = TopTrack(date_on_top = row['date_on_top'],
                    rank_number = row['rank_no'],
                    track_id = row['track_id'])
    session.merge(top_song)
session.commit()

##

# insert data into 'audio_features'
audio_data = audio_df.to_dict('records')

# similar process as before except column names all match and no junction table
for row in audio_data:
    audio = AudioFeatures(**row)
    # merge() and PKs prevent duplicate records
    session.merge(audio)
session.commit()

##

# insert data into 'concerts'
concerts_data = concerts_df.to_dict('records')

for row in concerts_data:
    # the value of the 'genre' key is a string consisting of genres separated by commas
    # use of .pop() to remove genre aids in insertion process below
    genres_list = row.pop('genre').split(",")
    genres_list = [genre.strip() for genre in genres_list]
   
    # use .pop() to "rename", i.e. remove, reassign to new column
    row['concertful_ranking'] = row.pop('ranking')

    concert = Concert(**row)
    for genre in genres_list:
        genre_name = Genre(name = genre)
        concert.genre.append(genre_name)
    session.merge(concert)
session.commit()

##

# insert data in 'festivals'

# drop 'genre' column since it is no longer in the database table 
# The column mostly consisted of 'Festival', though some were events over multiple days-- may account for this in future revisions.
festivals_df = festivals_df.drop(columns=['genre'])

# similar process as above except no association with genres
festivals_data = festivals_df.to_dict('records')

for row in festivals_data:
    row['concertful_ranking'] = row.pop('ranking')
    row['festival_id'] = row.pop('concert_id')
    festival = Festival(**row)
    session.merge(festival)
session.commit()

##

# insert data into data_dictionary
dict_key = 'data_dict.pkl'
dict_df = get_dataframe_from_s3(bucket, dict_key)

# should mostly do nothing following first upload, unless new descriptions are added for existing columns
dict_data = dict_df.to_dict('records')

for row in dict_data:
    record = DataDictionary(name = row['type'],
                            description = row['description'])
    session.merge(record)
session.commit()

##

# close the session
session.close() 