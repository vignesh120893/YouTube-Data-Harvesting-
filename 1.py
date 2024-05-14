import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import json

# YouTube API Configuration
API_KEY = 'YOUR_YOUTUBE_API_KEY'
youtube = build('youtube', 'v3', developerKey=API_KEY)

# SQL Configuration
DATABASE_URL = "mysql+pymysql://username:password@localhost/yourdatabase"

# Create SQL Alchemy Engine
engine = create_engine(DATABASE_URL)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

# Define SQLAlchemy ORM Models
class Channel(Base):
    __tablename__ = 'channels'
    id = Column(Integer, primary_key=True)
    channel_id = Column(String, unique=True)
    name = Column(String)
    description = Column(String)
    subscribers = Column(Integer)
    views = Column(Integer)
    playlist_id = Column(String)

class Video(Base):
    __tablename__ = 'videos'
    id = Column(Integer, primary_key=True)
    video_id = Column(String, unique=True)
    channel_id = Column(Integer, ForeignKey('channels.id'))
    name = Column(String)
    description = Column(String)
    tags = Column(String)
    published_at = Column(String)
    view_count = Column(Integer)
    like_count = Column(Integer)
    dislike_count = Column(Integer)
    favorite_count = Column(Integer)
    comment_count = Column(Integer)
    duration = Column(String)
    thumbnail = Column(String)
    caption_status = Column(String)

class Comment(Base):
    __tablename__ = 'comments'
    id = Column(Integer, primary_key=True)
    comment_id = Column(String, unique=True)
    video_id = Column(Integer, ForeignKey('videos.id'))
    text = Column(String)
    author = Column(String)
    published_at = Column(String)

Base.metadata.create_all(engine)

def fetch_channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    return response['items'][0] if response['items'] else None

def fetch_video_data(playlist_id):
    videos = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        videos.extend(response['items'])
        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return videos

def fetch_video_details(video_id):
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
    response = request.execute()
    return response['items'][0] if response['items'] else None

def fetch_comments(video_id):
    comments = []
    next_page_token = None

    while True:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            pageToken=next_page_token
        )
        response = request.execute()
        comments.extend(response['items'])
        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return comments

def save_channel_to_db(channel_data):
    channel = Channel(
        channel_id=channel_data['id'],
        name=channel_data['snippet']['title'],
        description=channel_data['snippet']['description'],
        subscribers=channel_data['statistics']['subscriberCount'],
        views=channel_data['statistics']['viewCount'],
        playlist_id=channel_data['contentDetails']['relatedPlaylists']['uploads']
    )
    session.add(channel)
    session.commit()
    return channel.id

def save_video_to_db(video_data, channel_id):
    video = Video(
        video_id=video_data['id'],
        channel_id=channel_id,
        name=video_data['snippet']['title'],
        description=video_data['snippet']['description'],
        tags=json.dumps(video_data['snippet'].get('tags', [])),
        published_at=video_data['snippet']['publishedAt'],
        view_count=video_data['statistics'].get('viewCount', 0),
        like_count=video_data['statistics'].get('likeCount', 0),
        dislike_count=video_data['statistics'].get('dislikeCount', 0),
        favorite_count=video_data['statistics'].get('favoriteCount', 0),
        comment_count=video_data['statistics'].get('commentCount', 0),
        duration=video_data['contentDetails']['duration'],
        thumbnail=video_data['snippet']['thumbnails']['high']['url'],
        caption_status=video_data['contentDetails']['caption']
    )
    session.add(video)
    session.commit()
    return video.id

def save_comment_to_db(comment_data, video_id):
    comment = Comment(
        comment_id=comment_data['id'],
        video_id=video_id,
        text=comment_data['snippet']['topLevelComment']['snippet']['textOriginal'],
        author=comment_data['snippet']['topLevelComment']['snippet']['authorDisplayName'],
        published_at=comment_data['snippet']['topLevelComment']['snippet']['publishedAt']
    )
    session.add(comment)
    session.commit()

def main():
    st.title('YouTube Data Harvesting and Warehousing')
    st.sidebar.title('Options')
    add_channel = st.sidebar.checkbox('Add Channel')
    search_data = st.sidebar.checkbox('Search Data')

    if add_channel:
        st.header('Add a YouTube Channel')
        channel_id_input = st.text_input('Enter YouTube Channel ID')

        if st.button('Fetch and Store Data'):
            channel_data = fetch_channel_data(channel_id_input)

            if channel_data:
                channel_id = save_channel_to_db(channel_data)
                videos = fetch_video_data(channel_data['contentDetails']['relatedPlaylists']['uploads'])

                for video in videos:
                    video_details = fetch_video_details(video['contentDetails']['videoId'])

                    if video_details:
                        video_id = save_video_to_db(video_details, channel_id)
                        comments = fetch_comments(video['contentDetails']['videoId'])

                        for comment in comments:
                            save_comment_to_db(comment, video_id)

                st.success('Data fetched and stored successfully.')
            else:
                st.error('Channel not found.')

    if search_data:
        st.header('Search YouTube Data')
        query = st.text_area('Enter SQL Query')
        
        if st.button('Execute Query'):
            result = pd.read_sql(query, engine)
            st.dataframe(result)

if __name__ == "__main__":
    main()
