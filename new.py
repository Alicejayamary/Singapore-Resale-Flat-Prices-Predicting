import streamlit as st
from googleapiclient.discovery import build
import pandas as pd

# YouTube API key
API_KEY = 'AIzaSyD7MpUY7WamIahBQcFfxUlRrzG0l1c_knI'

# Initialize YouTube Data API client
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Function to fetch data from YouTube channels
def fetch_channel_data(channel_ids):
    all_channel_data = []
    for channel_id in channel_ids:
        request = youtube.channels().list(
            part="snippet,statistics",
            id=channel_id
        )
        response = request.execute()
        if 'items' in response:
            channel_data = response['items'][0]
            all_channel_data.append(channel_data)
    return all_channel_data

# Main Streamlit app
def main():
    st.title("YouTube Channel Data Viewer")

    # List of YouTube channel IDs
    channel_ids = ['CHANNEL_ID_1', 'CHANNEL_ID_2', 'CHANNEL_ID_3', 'CHANNEL_ID_4', 'CHANNEL_ID_5',
                   'CHANNEL_ID_6', 'CHANNEL_ID_7', 'CHANNEL_ID_8', 'CHANNEL_ID_9', 'CHANNEL_ID_10']

    # Fetch data from YouTube channels
    channel_data = fetch_channel_data(channel_ids)

    # Display fetched data using Streamlit
    st.write("Fetched YouTube Channel Data:")
    for data in channel_data:
        st.write(pd.DataFrame(data))

