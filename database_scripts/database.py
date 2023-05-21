from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import Column, ForeignKey, Integer, String, SmallInteger, Date, Float, Boolean, Table, Time
import os, configparser

# would need to edit with absolute path of you pipeline.conf file
#this_folder = os.path.dirname(os.path.abspath(__file__))
#file_conf = os.path.join(this_folder, 'pipeline.conf')
#parser = configparser.ConfigParser()
#parser.read(file_conf)
database_url = parser.get("database", "DB_URL")

engine = create_engine(database_url, pool_pre_ping=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()
metadata = MetaData()
metadata.bind = engine

# Note: for simple many-to-many relationships (i.e. no additional features describing the relatiionship)
# we can directly create table instead of creating a class to link the two
# since there are no other attributes beyond the one that links them

# Define the table to join the 'artists' and 'genres' tables
# ~ simple many-to-many relationship between Artist and Genre 
# 1 artist can have multiple genres and multiple artists can play in the same genre
artist_genre_table = Table(
    'artist_genre', 
    Base.metadata,
    Column('artist_id', String, ForeignKey('artists.artist_.id')),
    Column('genre_id', SmallInteger, ForeignKey('genres.genre_id'))
)

# Define the table to join the 'artists' and 'albums' tables
# ~ simple many-to-many relationship between Artist and Album
# 1 artist can have multiple albums, 1 album can contain multiple artists
artist_album_table = Table(
    'artist_album',
    Base.metadata,
    Column('artist_id', String, ForeignKey('artists.artist_id')),
    Column('album_id', String, ForeignKey('albums.album_id'))
)

# Define the table to join the 'artists' and 'top_tracks'' tables
# ~ simple many-to-many relationship between Artist and TopTrack
# 1 artist can have multiple songs in top songs playlist
# and 1 song in the playlist can have multiple artists
artist_toptrack_table = Table(
    'artist_toptrack',
    Base.metadata,
    Column('artist_id', String, ForeignKey('artists.artist_id')),
    Column('track_id', String, ForeignKey('top_tracks.track_id'))
)

# ~ simple many-to-many relatinoship between Concert and Genre
concert_genre_table = Table(
    'concert_genre',
    Base.metadata,
    Column('concert_id', Integer, ForeignKey('concerts.concert_id')),
    Column('genre_id', SmallInteger, ForeignKey('genres.genre_id'))
)

# ~ simple many-to-many relationship between Festival and Genre
festival_genre_table = Table(
    'festival_genre',
    Base.metadata,
    Column('festival_id', Integer, ForeignKey('festivals.festival_id')),
    Column('genre_id', SmallInteger, ForeignKey('genres.genre_id'))
)

class Artist(Base):
    __tablename__ = 'artists'
    artist_id = Column(String, primary_key = True)
    name = Column(String, nullable = False)
    popularity = Column(SmallInteger)

    ##relationships##

    # 'artist' attribute in Genre connects Genre to Artist
    # artist_genre_table is the association/junction table connecting Artist and Genre
    genre = relationship('Genre', secondary = artist_genre_table, back_populates = 'artist')

    # 'artist' attribute in Album connects Album to Artist
    # artist_album_table is junction table for Artist and Album
    albums = relationship('Album', seconday = artist_album_table, back_populates = 'artist')
    
    # 'artist' attribute in TopTrack connects TopTrack to Artist
    # artist_toptrack_table is junction table for Artist and TopTrack
    toptrack = relationship('TopTrack',secondary = artist_toptrack_table, back_populates = 'artist')

class Genre(Base):
    __tablename__ = 'genres'
    genre_id = Column(String, primary_key = True, autoincrement = True)
    name = Column(String, nullable = False)

    # 'genre' attribute in Artist connects Artist to Genre
    # artist_genre_table is junction table for Artist and Genre
    artist = relationship('Artist', secondary=artist_genre_table, back_populates='genre')

    concert = relationship('Concert', secondary=concert_genre_table, back_populates='genre')
    festival = relationship('Festival', secondary=festival_genre_table, back_populates = 'genre')

class Album(Base):
    __tablename__ = 'albums'
    album_id = Column(String, primary_key = True)
    title = Column(String, nullable = False)
    album_type = Column(String)
    label = Column(String)
    popularity = Column(SmallInteger)
    release_date = Column(Date)
    total_tracks = Column(SmallInteger)

    ##relationships##

    # 'album' attribute in Artist links Artist to Album
    # artist_album_table is junction table for Artist and Album
    artist = relationship('Album', secondary=artist_album_table, back_populates = 'album')

    toptrack = relationship('TopTrack', back_populates = 'album')

class TopTrack(Base):
    __tablename__ = 'top_tracks'
    date_on_top = Column(Date, primary_key = True)
    rank_number = Column(SmallInteger, primary_key = True)

    # FK for 1-to-1 relationship between TopTrack and AudioFeatures
    track_id = Column(String, ForeignKey('audio_features.track_id'), nullable = False)

    title = Column(String, nullable = False)

    # FK for 1-to-1 relationship between TopTrack and Album
    album_id = Column(String, ForeignKey('albums.album_id'), nullable=False)

    ##relationships##

    # 'track' attribute in AudioFeatures connects AudioFeatures to TopTrack
    audio_features = relationship('AudioFeatures', back_populates = 'track')

    # 'toptrack' attribute in Album links Album to TopTrack
    album = relationship('Album', back_populates = 'toptrack')
    
    # 'toptrack' attribute in Artist links Artist to TopTrack
    # artist_toptrack_table is junction table for Artist and TopTrack
    artist = relationship('Artist', secondary = artist_toptrack_table, back_populates = 'toptrack')

    
class AudioFeatures(Base):
    __tablename__ = 'audio_features'
    track_id = Column(String, primary_key=True)
    acousticness = Column(Float)
    danceability = Column(Float)
    energy = Column(Float)
    key = Column(SmallInteger)
    loudness = Column(Float)
    mode = Column(Boolean)
    speechiness = Column(Float)
    instrumentalness = Float()
    liveness = Column(Float)
    valence = Column(Float)
    tempo = Column(Float)
    duration_ms = Column(Integer)
    time_signature = Column(SmallInteger)

    # the 'audio_features' attribute in TopTrack associates TopTrack to AudioFeatures
    track = relationship('TopTrack', back_populates = 'audio_features')

class Concert(Base):
    __tablename__ = 'concerts'
    concert_id = Column(Integer, primary_key=True)
    performer = Column(String) 
    # future edit: make connection between performer and artist possibly by using spotipy search <performer>
    venue = Column(String)
    location = Column(String)
    date = Column(Date)
    time = Column(Time)
    concertful_ranking = Column(SmallInteger)
    genre = relationship('Genre', secondary = concert_genre_table, back_populates = 'concert')

class Festival(Base):
    __tablename__ = 'festivals'
    festival_id = Column(String, primary_key = True)
    performer = Column(String)
    venue = Column(String)
    location = Column(String)
    start_date = Column(Date)
    end_date = Column(Date)
    time = Column(Time)
    concertful_ranking = Column(SmallInteger)
    genre = relationship('Genre', secondary = festival_genre_table, back_populates = 'festival')


# create the tables in the database
Base.metadata.create_all(engine)
# make changes permanent
session.commit()
# close the session
session.close() 
