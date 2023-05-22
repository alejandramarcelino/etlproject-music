import pandas as pd
import boto3
import pickle
import datetime
import os, configparser

# assuming the 'pipeline.conf' file is in the same location as the 'pipeline_template.conf' file
current_directory = os.path.dirname(os.path.abspath(__file__))
parent_directory = os.path.dirname(current_directory)
file_conf = os.path.join(parent_directory, 'pipeline.conf')
parser = configparser.ConfigParser()
parser.read(file_conf)

bucket = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto3.resource('s3')
concerts_key = 'concerts_df_{}.pkl'.format(datetime.datetime.now().strftime("%Y-%m-%d"))

obj = s3.Object(bucket_name=bucket, key=concerts_key)
response = obj.get()
concerts_data = response['Body'].read()
df = pickle.loads(concerts_data)

df.columns=['performer', 'venue', 'date', 'genre', 'ranking', 'concert_id']

# remove 'Perfomer' and new line characters
df['performer'] = df['performer'].replace(r'\nPerformer:\n\n\n|\n\n\n',"",regex=True)\
                                 .replace(r'\n',', ', regex=True)

# address the excess trailing \t with 'and More>>' for artists not mentioned
df['performer']=df['performer'].replace(r', \t*and More >>',', others', regex=True)

# remove 'Venue' and new line characters
df[['venue','location']]=df['venue'].replace(r'\nVenue:\n\n',"",regex=True).str.split(pat='\n',n=1,expand=True)

# shift columns around so that venue and venue location are adjacent columns
df.insert(2, "location", df.pop("location"))

# clean up location column by removing US, new line and tab characters
df['location'] = df["location"].replace(r'\n|\t',"", regex=True).str.strip(', United States')

# only keep city, state to query more easily, and remove leading space
df['location'] = df['location'].str.split(",").str[-2:].str.join(', ')
df['location']=df['location'].str.strip()

# clean up genre column
df['genre']= df['genre'].str.replace(r'\nGenre:\n|\n','',regex=True).replace(' / ',', ',regex=True)

# remove 'Date', new line, tab characters
df['date'] = df['date'].replace('\nDate:|\n|\t', "", regex=True)

# separate date and time into 2 columns
df[['date','time']]=df['date'].str.split(pat='|',n=1,expand=True)

# rearrange columns to have date and time next to each other
df.insert(4,'time',df.pop('time'))

# remove day of week before date
df['date']=df['date'].str.split(', ',n=1,expand=True)[1]

# dictionary to help with converting date data into datetime format
month_dict={'January':'01','February':'02','March':'03', 'April':'04','May':'05','June':'06',\
            'July':'07','August':'08','September':'09','October':'10','November':'11','December':'12'}

# replace month with mm using month_dict, clean up by separating with '/'
df['date']=df['date'].replace(month_dict,regex=True).str.strip().replace(r" |, ","/",regex=True)

# remove day of week still remaining in range of dates
df['date']=df['date'].replace(r'/Sun/|/Mon/|/Tues/|/Wed/|/Thu/|/Fri/|/Sat/','',regex=True).replace(r'/-','-',regex=True)

# convert single dates into pandas datetime objects, only display the date, would add 00:00:00 otherwise
df.loc[df['date'].map(len)==10,'date'] = pd.to_datetime(df.loc[df['date'].map(len)==10,'date']).dt.date

# remove leading space to be able to use to_datetime method
df['time']=df['time'].str.strip()

# only show time
df['time'] = pd.to_datetime(df['time'],format= '%I:%M%p').dt.time

# move id to first column
df.insert(0,'concert_id',df.pop('concert_id'))

df = df.replace({pd.NaT: None})
# pd.reset_option("display.max_rows")
# pd.set_option("display.max_columns",None)

festivals_df = df[[isinstance(value, str) for value in df['date']]]
concerts_df = df[[value.__class__ == datetime.date for value in df['date']]]

# if convert to DataFrame, will no longer get SettingWithCopyWarning
festivals_df=pd.DataFrame(festivals_df)
concerts_df=pd.DataFrame(concerts_df)

festivals_df = festivals_df.reset_index(drop=True)
concerts_df = concerts_df.reset_index(drop=True)

#with pd.option_context('mode.chained_assignment', None):
festivals_df[['start_date','end_date']]=festivals_df['date'].str.split(pat='-',n=1,expand=True)
festivals_df=festivals_df.drop(columns='date')

festivals_df.insert(4,'start_date',festivals_df.pop('start_date'))
festivals_df.insert(5,'end_date',festivals_df.pop('end_date'))

pickled_concerts_df = pickle.dumps(concerts_df)
pickled_fest_df = pickle.dumps(festivals_df)

cleaned_concerts_key = 'cleaned_concerts_{}.pkl'.format(datetime.datetime.now().strftime("%Y-%m-%d"))
cleaned_festivals_key = 'cleaned_festivals_{}.pkl'.format(datetime.datetime.now().strftime("%Y-%m-%d"))

object_concerts = s3.Object(bucket_name=bucket, key=cleaned_concerts_key)
object_festivals = s3.Object(bucket_name=bucket, key=cleaned_festivals_key)

# Note: 'Body' parameter expects binary data to be passed
# If passing string, would write b'<string>' to indicate it should be treated as byte string
# In this case, pickled_df is a pickle object, which is represented as binary data
object_concerts.put(Body=pickled_concerts_df)
object_festivals.put(Body=pickled_fest_df)

