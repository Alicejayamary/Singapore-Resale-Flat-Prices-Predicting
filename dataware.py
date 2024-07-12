import pandas as pd
import streamlit as st
from googleapiclient.discovery import build

from pymongo import MongoClient
from bson import ObjectId
# import mysql.connector
import mysql.connector as sql 
from datetime import datetime
import time
pip install google-api-python-client


#this code is sidebar loding

with st.sidebar:
    # with st.echo():
    st.write("please wait")

    with st.spinner("Loading..."):
        time.sleep(5)
    st.success("Done!")

# This code is connecting postgres SQL
cnx = sql.connect(host= "localhost",
                   user="root",
                   password="Varsha@05",
                   database="yt"
                   )
mycursor = cnx.cursor(buffered=True)

cursor = cnx.cursor()

# Connect to MongoDB Atlas
atlas_username = 'VarshaGS'
atlas_password = 'VarshaGS'
atlas_cluster = 'cluster0'
client = MongoClient(
    f"mongodb+srv://{atlas_username}:{atlas_password}@{atlas_cluster}.vzitov2.mongodb.net/?retryWrites=true&w=majority")
# mongodb+srv://VarshaGS:<password>@cluster0.vzitov2.mongodb.net/?retryWrites=true&w=majority
db = client['youtube_data']
collection = db['channel_data']
#api_key
Api_key = 'AIzaSyBdI8onhUJYn_qq-c0ue8IuHXh6nHiP7K8'

# Set Streamlit app title
st.title("YouTube Data Harvesting and Warehousing")

# Display input field for YouTube channel ID
channel_id = st.text_input("Enter YouTube Channel ID")
a,b = st.tabs([" Retrieve Channel Data ", " Store Data In MongoDB Atlas "])
c,d = st.tabs([" Retrieve Data From MongoDB Atlas ", " Create tables In SQL"])
e,f = st.tabs(["Migrate channel information from MongoDB Atlas to SQL",
     " Migrate video information from MongoDB Atlas to SQL "])
g,h = st.tabs(["Migrate comment information from MongoDB Atlas to SQL",
     "channel analysis"])


def parse_duration(duration):
    duration_str = ""
    hours = 0
    minutes = 0
    seconds = 0

    # Remove 'PT' prefix from duration
    duration = duration[2:]

    # Check if hours, minutes, and/or seconds are present in the duration string
    if "H" in duration:
        hours_index = duration.index("H")
        hours = int(duration[:hours_index])
        duration = duration[hours_index + 1:]
    if "M" in duration:
        minutes_index = duration.index("M")
        minutes = int(duration[:minutes_index])
        duration = duration[minutes_index + 1:]
    if "S" in duration:
        seconds_index = duration.index("S")
        seconds = int(duration[:seconds_index])

    # Format the duration string
    if hours >= 0:
        duration_str += f"{hours}h "
    if minutes >= 0:
        duration_str += f"{minutes}m "
    if seconds >= 0:
        duration_str += f"{seconds}s"

    return duration_str.strip()


# Retrieve all video_ids
def get_video_ids(youtube, playlist_id):
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
        maxResults=50)
    response = request.execute()

    video_ids = []

    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')

    return video_ids


# Retrieve videos for a given YouTube channel ID
def get_video_details(youtube, video_ids):
    videos = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=','.join(video_ids[i:i + 50]))

        video_response = request.execute()

        videos.extend(video_response['items'])
    return videos


def get_video_comments(youtube, videoid):
    youtube = build("youtube", "v3", developerKey=Api_key)
    comments = []

    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=videoid,
            maxResults=100
        )

        while request:
            response = request.execute()

            for comment in response['items']:
                data = {
                    'Video_Id': videoid,
                    'Comment_Id': comment['snippet']['topLevelComment']['id'],
                    'Comment_Text': comment['snippet']['topLevelComment']['snippet']['textOriginal'],
                    'Comment_Author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_PublishedAt': comment['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                comments.append(data)

            if 'nextPageToken' in response:
                request = youtube.commentThreads().list(
                    part="snippet",
                    textFormat="plainText",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=response.get('nextPageToken')
                )
            else:
                break
    except HttpError as e:
        if e.resp.status == 403 and 'disabled comments' in str(e):
            data = {
                'Video_Id': video_id,
                'Comment_Id': f'comments_disabled_{video_id}',
                'Comment_Text': 'comments_disabled',
                'Comment_Author': 'comments_disabled',
                'Comment_PublishedAt': 'comments_disabled'
            }
            comments.append(data)
            print(f"Comments are disabled for video: {video_id}")
        else:
            print(f"An error occurred while retrieving comments for video: {video_id}")
            print(f"Error details: {e}")

    return comments


# def date_formatter(date):
#    x = date.replace('T',' ')
#    x.translate({ord('Z'): None})
#    date_format = '%Y-%m-%d %H:%M:%S'
#    date_obj = datetime.strptime(x, date_format)
#    return date_obj

def durationtoint(time_str):
    hours, minutes, seconds = time_str.split('h ')[0], time_str.split('h ')[1].split('m ')[0], \
        time_str.split('h ')[1].split('m ')[1][:-1]

    total_seconds = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
    return (total_seconds)

    # Initialize YouTube Data API client


youtube = build('youtube', 'v3', developerKey=Api_key)

# Make API request to get channel data
request = youtube.channels().list(
    part='snippet,statistics,contentDetails',
    id=channel_id
)
response = request.execute()

if 'items' in response:
    channel_data = response['items'][0]
    snippet = channel_data['snippet']
    statistics = channel_data['statistics']
    content_details = channel_data.get('contentDetails', {})
    related_playlists = content_details.get('relatedPlaylists', {})

    # Extract relevant data
    data = {
        'Channel_Name': {
            'Channel_Name': snippet.get('title', ''),
            'Channel_Id': channel_id,
            'Subscription_Count': int(statistics.get('subscriberCount', 0)),
            'Channel_Views': int(statistics.get('viewCount', 0)),
            'Channel_Description': snippet.get('description', ''),
            'Playlist_Id': related_playlists.get('uploads', '')
        }
    }

    # Retrieve video data
    video_ids = get_video_ids(youtube, data['Channel_Name']['Playlist_Id'])
    videos = get_video_details(youtube, video_ids)

    for video in videos:
        video_id = video['id']
        video_data = {
            'Video_Id': video_id,
            'Video_Name': video['snippet'].get('title', ''),
            'Video_Description': video['snippet'].get('description', ''),
            'Tags': video['snippet'].get('tags', []),
            'PublishedAt': pd.to_datetime(video['snippet'].get('publishedAt', '')),
            'View_Count': int(video['statistics'].get('viewCount', 0)),
            'Like_Count': int(video['statistics'].get('likeCount', 0)),
            'Dislike_Count': int(video['statistics'].get('dislikeCount', 0)),
            'Favorite_Count': int(video['statistics'].get('favoriteCount', 0)),
            'Comment_Count': int(video['statistics'].get('commentCount', 0)),
            'Duration': parse_duration(video['contentDetails'].get('duration', '')),
            'Thumbnail': video['snippet'].get('thumbnails', {}).get('default', {}).get('url', ''),
            'Caption_Status': video['snippet'].get('localized', {}).get('localized', 'Not Available'),
            'Comments': get_video_comments(youtube, video_id)
        }
        data[video_id] = video_data

# Retrieve channel data using YouTube API
with a:
    if st.button("Retrieve Channel Data"):
        try:

            # Display channel data
            st.write("Channel Name:", data['Channel_Name']['Channel_Name'])
            st.write("Subscribers:", data['Channel_Name']['Subscription_Count'])
            st.write("Total Videos:", len(videos))

            # Display video data
            st.subheader("Video Data:")
            for video_id, video_data in data.items():
                if video_id != 'Channel_Name':
                    st.write("Video Name:", video_data['Video_Name'])
                    st.write("Video Description:", video_data['Video_Description'])
                    st.write("Published At:", video_data['PublishedAt'])
                    st.write("View Count:", video_data['View_Count'])
                    st.write("Like Count:", video_data['Like_Count'])
                    st.write("Favorite_Count:", video_data['Favorite_Count'])
                    st.write("Dislike Count:", video_data['Dislike_Count'])
                    st.write("Comment Count:", video_data['Comment_Count'])
                    st.write("Duration:", video_data['Duration'])
                    st.write("Thumbnail:", video_data['Thumbnail'])
                    st.write("comments:", video_data['Comments'])
            st.write()
        except Exception as e:
            st.error(f"Error retrieving channel data: {str(e)}")

with b:
    # Store data in MongoDB Atlas
    if st.button("Store Data in MongoDB Atlas"):
        collection.insert_one(data)
        st.success("Data stored successfully in MongoDB Atlas!")

with c:
    # Retrieve data from MongoDB Atlas
    if st.button("Retrieve Data from MongoDB Atlas"):
        retrieved_data = collection.find_one({'Channel_Name.Channel_Id': channel_id})
        if retrieved_data:
            st.subheader("Retrieved Data:")
            st.write("Channel Name:", retrieved_data['Channel_Name']['Channel_Name'])
            st.write("Subscribers:", retrieved_data['Channel_Name']['Subscription_Count'])
            st.write("Total Videos:", len(videos))
            for video_id, video_data in retrieved_data.items():
                if video_id != 'Channel_Name' and not isinstance(video_data, ObjectId):
                    st.write("Video Name:", video_data['Video_Name'])
                    st.write("Video Description:", video_data['Video_Description'])
                    st.write("Published At:", video_data['PublishedAt'])
                    st.write("View Count:", video_data['View_Count'])
                    st.write("Like Count:", video_data['Like_Count'])
                    st.write("Dislike Count:", video_data['Dislike_Count'])
                    st.write("Comment Count:", video_data['Comment_Count'])
                    st.write("Duration:", video_data['Duration'])
                    st.write("Thumbnail:", video_data['Thumbnail'])
        else:
            st.warning("Data not found in MongoDB Atlas!")
with d:
    if st.button("Create tables in SQL"):
        cursor.execute("""CREATE TABLE Channel (channel_id VARCHAR(255) PRIMARY KEY,
                      channel_name VARCHAR(255),
                      channel_views INT,
                      channel_description text)""")

        cursor.execute("""CREATE TABLE playlist (playlist_id VARCHAR(255) PRIMARY KEY, 
                      channel_id VARCHAR(255),
                      channel_name VARCHAR(255),
                      FOREIGN KEY(channel_id) REFERENCES Channel(channel_id) ON DELETE SET NULL )""")

        cursor.execute("""CREATE TABLE video (video_id VARCHAR(255) PRIMARY KEY,
                      playlist_id VARCHAR(255),
                      video_name VARCHAR(255),
                      video_description text,
                      published_date TIMESTAMP,
                      view_count INT,
                      like_count INT,
                      dislike_count INT,
                      comment_count INT,
                      duration INT,
                      thumbnail VARCHAR(255),
                      FOREIGN KEY(playlist_id) REFERENCES playlist(playlist_id) ON DELETE SET NULL)""")

        cursor.execute("""CREATE TABLE comment (comment_id VARCHAR(255) PRIMARY KEY,
                      video_id VARCHAR(255),
                      comment_text text,
                      comment_author VARCHAR(255),
                      comment_published_date VARCHAR(255),
                      FOREIGN KEY(video_id) REFERENCES Video(video_id) ON DELETE SET NULL )""")
        cnx.commit()
        st.success("Tables have been created!")
with e:
    if st.button("Migrate channel information from MongoDB Atlas to SQL"):
        migration_data = collection.find_one({'Channel_Name.Channel_Id': channel_id})
        if migration_data:
            query_channel = """
                INSERT INTO Channel (
                    channel_id,
                    channel_name,
                    channel_views,
                    channel_description
                    ) VALUES (%s, %s, %s, %s)
                    """
            values_channel = (
                migration_data['Channel_Name']['Channel_Id'],
                migration_data['Channel_Name']['Channel_Name'],
                migration_data['Channel_Name']['Channel_Views'],
                migration_data['Channel_Name']['Channel_Description']
            )

            query_playlist = """
                INSERT INTO playlist (
                    playlist_id,
                    channel_id,
                    channel_name
                    ) VALUES (%s, %s, %s)
                    """
            values_playlist = (
                migration_data['Channel_Name']['Playlist_Id'],
                migration_data['Channel_Name']['Channel_Id'],
                migration_data['Channel_Name'])
...


