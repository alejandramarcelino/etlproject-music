import re
import pandas as pd
import boto3
import pickle
import os, configparser

# creating a data dictionary to provide a brief explanation on all the features
# most descriptions, particularly for audio features, are directly from Spotify's Web API docs
top50_songs_df_dict ={
    'songs_id': ['string', 'The Spotify ID for the track.'],\
    'song_name': ['string', 'The name of the track.'],\
    'popularity': ['int64', 'The popularity of the track.\
                    Given a value between 0 and 100, with 100 being the most popular.\
                    The popularity is calculated by algorithm and is based, in the most part, \
                    on the total number of plays the track has had and how recent those plays are.\
                    The value is not updated in real time and may lag actual popularity by a few days.'],\
    'first_artist': ['string', 'The name of the first artist listed for the track.'],\
    'all_artists': ['string', 'The names of all of the artists listed for the track.'],\
    'album_id': ['string', 'The Spotify ID for the album.'],\
    'album_name': ['string','The name of the album the track is included in.'],\
    'album_release_date': ['datetime', 'The date the album was first released.'],\
    'danceability': ['float64', 'Danceability describes how suitable a track is for dancing based on a combination of\
                    musical elements including tempo, rhythm stability, beat strength, and overall regularity. \
                    A value of 0.0 is least danceable and 1.0 is most danceable.'],\
    'energy': ['float64', 'Energy is a measure from 0.0 to 1.0 and represents a perceptual measure\
                    of intensity and activity. Typically, energetic tracks feel fast, loud, and noisy.\
                    For example, death metal has high energy, while a Bach prelude scores low on the scale.\
                    Perceptual features contributing to this attribute include dynamic range, perceived loudness,\
                    timbre, onset rate, and general entropy.'],\
    'key': ['int64', 'The key the track is in. Integers map to pitches using standard Pitch Class notation.\
                    If no key was detected, the value is -1. The value is between -1 and 11.'],\
    'loudness': ['float64', 'The overall loudness of a track in decibels (dB). Loudness values\
                    are averaged across the entire track and are useful for comparing relative\
                    loudness of tracks. Loudness is the quality of a sound that is the primary \
                    psychological correlate of physical strength (amplitude). Values typically range between -60 and 0 db.'],\
    'mode': ['int64', 'Mode indicates the modality (major or minor) of a track, the type of scale \
                    from which its melodic content is derived. Major is represented by 1 and minor is 0.'],\
    'speechiness': ['float64', 'Spechiness detects the presence of spoken word in a track.\
                    The more exclusively speech-like the recording (e.g. talk show, audio book, poetry) \
                    the close to 1.0 the attribute value. Values above 0.66 describe tracks that are probably made\
                    entirely of spoken words. Values between 0.33 and 0.66 describe tracks that are probably made\
                    either in sections or layered, including such cases as rap music. Values below 0.33 most likely\
                    represent music and other non-speech-like tracks.'],\
    'instrumentalness': ['float64', 'Predicts whether a track contains no vocals. "Ooh" and \
                    "aah" sounds are treated as instrumental in this context. Rap or spoken word tracks\
                    are clearly "vocal". The closer the instrumentalness value is to 1.0, the greater\
                    likelihood the track contains no vocal content. Values above 0.5 are intended to \
                    represent instrumental tracks, but confidence is higher as the value approaches 1.0.'],
    'liveness': ['float64', 'Detects the presence of an audience in the recording. Higher liveness values represent\
                    an increased probability that the track was performed live. A value above 0.8 provides strong\
                    likelihood that the track is live.'],
    'valence': ['float64', 'A measure from 0.0 to 1.0 describing the musical positiveness conveyed by a track.\
                Tracks with high valence sound more positive (e.g. happy, cheerful, euphoric), while tracks\
                with low valence sound more negative (e.g. sad, depressed, angry).'],
    'tempo': ['float64', 'The overall estimated tempo of a track in beats per minute (BPM). In musical terminology,\
                    tempo is the speed or pace of a given piece and derives directly from the average beat duration.'],\
    'duration_ms': ['int64', 'The duration of the track in milliseconds.'],\
    'time_signature': ['int64', 'An estimated time signature. The time signature (meter) is a notational convention\
                    to specify how many beats are in each bar (or measure). The time signature ranges from 3 to 7\
                    indicating time signatures of "3/4", to "7/4".']}

# removing extra spaces to improve readability 
top50_songs_df_dict = {key: [value[0], re.sub('\s{2,}', '', value[1])] for key,value in top50_songs_df_dict.items()}

data_dict_df = pd.DataFrame.from_dict(top50_songs_df_dict, orient='index',
                       columns=['type', 'description'])

pickled_data_dict = pickle.dumps(data_dict_df)

#would need to edit with your absolute path of pipeline.conf file
#this_folder = os.path.dirname(os.path.abspath(__file__))
#file_conf = os.path.join(this_folder, 'pipeline.conf')
#parser = configparser.ConfigParser()
#parser.read(file_conf)
bucket = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto3.resource('s3')

key_for_data_dict = parser.get("aws_boto_credentials", "key_data_dict")
# I show name of my data dictionary document in other files, but I wanted to show that you 
# can conceal file keys as well if you'd like
bucket.put_object(Key=key_for_data_dict, Body=pickled_data_dict)
