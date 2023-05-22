from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
import os, configparser
from database import Artist, Album, Genre
from database_functions import check_value_exists, get_dataframe_from_s3
from datetime import datetime

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

# inserting new artists into table 'artists'
artists_key = 'data/artists_df_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))
artists_df = get_dataframe_from_s3(bucket, artists_key)

filtered_artists = []
for row in artists_df.iterrows():
    if not check_value_exists(Artist, artist_id, row.artist_id):
        filtered_artists.append(row)
session.add_all(filtered_artists)    

# to dynamically add new genres to 'genres' table
genres_list = []
for row in artists_df.itertuples():
    genres_list.extend(row.artist_genres)
distinct_genres = set(genres_list)

existing_genres = session.query(Genre.genre_name).all()
for genre in distinct_genres:
    if genre not in existing_genres:
        new_genre = Genre(genre_name=genre)
        session.add(new_genre)

# inserting new albums into 'albums' table
albums_key = 'data/albums_df_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))
albums_df = get_dataframe_from_s3(bucket, albums_key)

filtered_albums = []
for row in albums_df.iterrows():
    if not check_value_exists(Album, album_id, row.album_id):
        filtered_albums.append(row)
session.add_all(filtered_albums)

# inserting data of top tracks playlist
top50_key = 'data/top50_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))
top50_df = get_dataframe_from_s3(bucket, top50_key)

# create dictionary including map to corresponding mapped class 'TopTrack'


