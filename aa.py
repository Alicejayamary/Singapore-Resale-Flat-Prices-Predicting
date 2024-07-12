import pandas as pd
import streamlit as st
from googleapiclient.discovery
import mysql.connector as sql 
from datetime import datetime

# YouTube API key
API_KEY = 'AIzaSyD7MpUY7WamIahBQcFfxUlRrzG0l1c_knI'

# Initialize YouTube Data API client
youtube = googleapiclient.discovery('youtube', 'v3', developerKey=API_KEY)

# SQL connection setup
cnx = sql.connect(host="localhost",
                  user="root",
                  password="Varsha@05",
                  database="yt")
cursor = cnx.cursor(buffered=True)

# Function to parse duration
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

# Function to fetch video details and comments
def fetch_video_details(channel_id):
    # Retrieve channel data
    request = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    )
    response = request.execute()
    
    if 'items' in response:
        uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        # Retrieve videos from uploads playlist
        request = youtube.playlistItems().list(
            part='snippet',
            playlistId=uploads_playlist_id,
            maxResults=50
        )
        videos = request.execute()['items']
        
        for video in videos:
            video_id = video['snippet']['resourceId']['videoId']
            
            # Retrieve video details
            request = youtube.videos().list(
                part='snippet,statistics',
                id=video_id
            )
            video_details = request.execute()['items'][0]
            
            # Retrieve video comments
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            comments = request.execute()['items']
            
            # Store video details and comments in database
            store_video_data(channel_id, video_details, comments)

# Function to store video details and comments in database
def store_video_data(channel_id, video_details, comments):
    # Store video details in database
    video_data = {
        'Video_Id': video_details['id'],
        'Channel_Id': channel_id,
        'Video_Title': video_details['snippet']['title'],
        'Channel_Title': video_details['snippet']['channelTitle'],
        'Published_At': pd.to_datetime(video_details['snippet']['publishedAt']),
        'View_Count': int(video_details['statistics']['viewCount']),
        'Like_Count': int(video_details['statistics']['likeCount']),
        'Dislike_Count': int(video_details['statistics']['dislikeCount']),
        'Comment_Count': int(video_details['statistics']['commentCount']),
        'Duration': parse_duration(video_details['contentDetails']['duration'])
    }
    
    # Insert video details into database
    cursor.execute("""
        INSERT INTO videos (Video_Id, Channel_Id, Video_Title, Channel_Title, Published_At, View_Count, 
                            Like_Count, Dislike_Count, Comment_Count, Duration)
        VALUES (%(Video_Id)s, %(Channel_Id)s, %(Video_Title)s, %(Channel_Title)s, %(Published_At)s, %(View_Count)s, 
                %(Like_Count)s, %(Dislike_Count)s, %(Comment_Count)s, %(Duration)s)
    """, video_data)
    
    # Store comments in database
    for comment in comments:
        comment_data = {
            'Comment_Id': comment['id'],
            'Video_Id': video_details['id'],
            'Comment_Text': comment['snippet']['topLevelComment']['snippet']['textOriginal'],
            'Author_Name': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            'Published_At': pd.to_datetime(comment['snippet']['topLevelComment']['snippet']['publishedAt'])
        }
        
        # Insert comment into database
        cursor.execute("""
            INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, Author_Name, Published_At)
            VALUES (%(Comment_Id)s, %(Video_Id)s, %(Comment_Text)s, %(Author_Name)s, %(Published_At)s)
        """, comment_data)

# Function to execute SQL query and display results in Streamlit table
def execute_sql_query(query):
    cursor.execute(query)
    result = cursor.fetchall()
    st.table(pd.DataFrame(result, columns=cursor.column_names))

# Main Streamlit app
def main():
    st.title("YouTube Data Analysis")
    
    # Fetch data for 10 YouTube channels
    channel_ids = ['CHANNEL_ID_1', 'CHANNEL_ID_2', 'CHANNEL_ID_3', 'CHANNEL_ID_4', 'CHANNEL_ID_5',
                   'CHANNEL_ID_6', 'CHANNEL_ID_7', 'CHANNEL_ID_8', 'CHANNEL_ID_9', 'CHANNEL_ID_10']
    for channel_id in channel_ids:
        fetch_video_details(channel_id)
    
    # Execute SQL queries
    st.header("SQL Query Results")
    
    st.subheader("1. Names of all videos and their corresponding channels")
    execute_sql_query("""
        SELECT Video_Title, Channel_Title 
        FROM videos
    """)
    
    st.subheader("2. Channels with the most number of videos and their counts")
    execute_sql_query("""
        SELECT Channel_Title, COUNT(*) AS Video_Count
        FROM videos
        GROUP BY Channel_Title
        ORDER BY Video_Count DESC
        LIMIT 1
    """)
    
    st.subheader("3. Top 10 most viewed videos and their respective channels")
    execute_sql_query("""
        SELECT Video_Title, Channel_Title, View_Count
        FROM videos
        ORDER BY View_Count DESC
        LIMIT 10
    """)
    
    st.subheader("4. Number of comments on each video and their corresponding video names")
    execute_sql_query("""
        SELECT Video_Title, COUNT(*) AS Comment_Count
        FROM comments
        INNER JOIN videos ON comments.Video_Id = videos.Video_Id
        GROUP BY Video_Title
    """)
    
    st.subheader("5. Videos with the highest number of likes and their corresponding channel names")
    execute_sql_query("""
        SELECT Video_Title, Channel_Title,""")
execute_sql_query("""
        SELECT Video_Title, Channel_Title,
        MAX(Like_Count) AS Max_Likes
        FROM videos
        GROUP BY Video_Title, Channel_Title
        ORDER BY Max_Likes DESC
        LIMIT 10
    """)
