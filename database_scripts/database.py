from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import Column, ForeignKey, Integer, String, SmallInteger, Date, Float, Boolean, Table, Time
import os, configparser

# assuming the 'pipeline.conf' file is in the same location as the 'pipeline_template.conf' file
current_directory = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.dirname(current_directory)
file_conf = os.path.join(parent_directory, 'pipeline.conf')
parser = configparser.ConfigParser()
parser.read(file_conf)

database_url = parser.get("database", "DB_URL")

engine = create_engine(database_url, pool_pre_ping=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()
metadata = MetaData()
metadata.bind = engine

# Note: for simple many-to-many relationships (i.e. no additional features describing the relationship)
# we can directly create table instead of creating a class to link the two
# since there are no other attributes beyond the one that links them

# ~ simple many-to-many relationship between Artist and Genre 
artist_genre_table = Table(
    'artist_genre', 
    Base.metadata,
    Column('artist_id', String, ForeignKey('artists.artist_id', ondelete='CASCADE')),
    Column('genre_id', Integer, ForeignKey('genres.genre_id', ondelete='CASCADE'))
)

# ~ simple many-to-many relationship between Artist and Album
artist_album_table = Table(
    'artist_album',
    Base.metadata,
    Column('artist_id', String, ForeignKey('artists.artist_id', ondelete='CASCADE')),
    Column('album_id', String, ForeignKey('albums.album_id'))
)

# ~ simple many-to-many relationship between Artist and Track
artist_track_table = Table(
    'artist_track',
    Base.metadata,
    Column('artist_id', String, ForeignKey('artists.artist_id', ondelete='CASCADE')),
    Column('track_id', String, ForeignKey('tracks.track_id'))
    )

# ~ simple many-to-many relationship between Concert and Genre
concert_genre_table = Table(
    'concert_genre',
    Base.metadata,
    Column('concert_id', Integer, ForeignKey('concerts.concert_id', ondelete='CASCADE')),
    Column('genre_id', Integer, ForeignKey('genres.genre_id', ondelete='CASCADE'))
)

class Artist(Base):
    __tablename__ = 'artists'
    artist_id = Column(String, primary_key = True)
    name = Column(String)
    popularity = Column(SmallInteger)

    ##relationships##

    # 'artist' attribute in Genre connects Genre to Artist
    # artist_genre_table is the association table of Artist and Genre
    genre = relationship('Genre', secondary = artist_genre_table, back_populates = 'artist')

    # 'artist' attribute in Album connects Album to Artist
    # artist_album_table is junction table for Artist and Album
    album = relationship('Album', secondary = artist_album_table, back_populates = 'artist')
    
    # 'artist' attribute in Track connects Track to Artist
    # artist_track_table is junction table for Artist and Track
    track = relationship('Track', secondary = artist_track_table, back_populates = 'artist')

class Genre(Base):
    __tablename__ = 'genres'
    genre_id = Column(Integer, primary_key = True, autoincrement = True)
    name = Column(String, nullable = False)

    # 'genre' attribute in Artist connects Artist to Genre
    # artist_genre_table is junction table for Artist and Genre
    artist = relationship('Artist', secondary=artist_genre_table, back_populates='genre')

    # 'genre' attribute in Concert connects Concert to Genre
    # concert_genre_table is junction table for Concert and Genre
    concert = relationship('Concert', secondary=concert_genre_table, back_populates='genre')

class Album(Base):
    __tablename__ = 'albums'
    album_id = Column(String, primary_key = True)
    title = Column(String, nullable = False)
    album_type = Column(String)
    label = Column(String)
    popularity = Column(SmallInteger)
    # note: changed release_date type from Date to String due to some dates consisting of only the year
    # requires additional validation, such as:
    #   - defining methods within the class to check whether value is a valid date/year, raise error otherwise
    #   - unit tests to check whether the custom methods correctly validate the data
    release_date = Column(String)
    total_tracks = Column(SmallInteger)

    ##relationships##

    # 'album' attribute in Artist links Artist to Album
    # artist_album_table is junction table for Artist and Album
    artist = relationship('Artist', secondary=artist_album_table, back_populates = 'album')

    # 'album' attribute in Track connects Track to Album
    # 1-to-1 relationship, Track references Album on album_id
    track = relationship('Track', back_populates = 'album')


class Track(Base):
    __tablename__ = 'tracks'
    track_id = Column(String, primary_key = True)
    title = Column(String, nullable = False)

    # FK for 1-to-1 relationship between Track and Album
    album_id = Column(String, ForeignKey('albums.album_id', ondelete='CASCADE'), nullable=False)

    album = relationship('Album', back_populates = 'track')

    # 'track' connects Artist to Track
    # artist_track_table corresponding junction table
    artist = relationship('Artist', secondary = artist_track_table, back_populates = 'track')

    # 'track' connects AudioFeatures to Track
    # 1-to-1 relationship, AudioFeatures references Track on track_id
    audio_features = relationship('AudioFeatures', back_populates = 'track')

class TopTrack(Base):
    __tablename__ = 'top_tracks'
    date_on_top = Column(Date, primary_key = True)
    rank_number = Column(SmallInteger, primary_key = True)

    track_id = Column(String, ForeignKey('tracks.track_id', ondelete='CASCADE'), nullable = False)
    # will not define relationship here
    # seems more intuitive to instead query Track for TopTrack info (regarding the reverse relationship)

class AudioFeatures(Base):
    __tablename__ = 'audio_features'
    track_id = Column(String, ForeignKey('tracks.track_id', ondelete='CASCADE'), primary_key=True)
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

    # the 'audio_features' attribute in Track associates Track to AudioFeatures
    track = relationship('Track', back_populates = 'audio_features')

class DataDictionary(Base):
    __tablename__ = 'data_dictionary'
    id = Column(SmallInteger, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable = False)

class Concert(Base):
    __tablename__ = 'concerts'
    concert_id = Column(Integer, primary_key=True)
    performer = Column(String) 
    # future edit: make connection between performer and artist possibly by using spotipy search <performer>
    # another future edit: break up multiple performers given
    venue = Column(String)
    location = Column(String)
    date = Column(Date)
    time = Column(Time)
    concertful_ranking = Column(String)
    genre = relationship('Genre', secondary = concert_genre_table, back_populates = 'concert')

class Festival(Base):
    __tablename__ = 'festivals'
    festival_id = Column(String, primary_key = True)
    performer = Column(String)
    # future edit: get info on individual concerts within each multi-day event
    venue = Column(String)
    location = Column(String)
    start_date = Column(Date)
    end_date = Column(Date)
    time = Column(Time)
    concertful_ranking = Column(String)

# create the tables in the database
Base.metadata.create_all(engine)
# make changes permanent
session.commit()
# close the session
session.close() 
