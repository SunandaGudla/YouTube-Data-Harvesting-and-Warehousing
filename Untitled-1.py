# ==================================================       /     IMPORT LIBRARY    /      =================================================== #

# [Youtube API libraries]
import googleapiclient.discovery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# [File handling libraries]
import json
import re

# [MongoDB]
import pymongo

# [SQL libraries]
import mysql.connector

import sqlalchemy
from sqlalchemy import create_engine
import pymysql

# [pandas, numpy]
import pandas as pd
import numpy as np

# [Dash board libraries]
import streamlit as st
import plotly.express as px

# Comfiguring Streamlit GUI 
st.set_page_config(layout='wide')

# Title
st.title(':red[Youtube Data Harvesting]')

# Data collection zone
col1, col2 = st.columns(2)
with col1:
    st.header(':violet[Data collection zone]')
    st.write ('(Note:- This zone **collect data** by using channel id and **stored it in the :green[MongoDB] database**.)')
    channel_id = st.text_input('**Enter 11 digit channel_id**')
    st.write('''Get data and stored it in the MongoDB database to click below **:blue['Get data and stored']**.''')
    Get_data = st.button('**Get data and stored**')

    # Define Session state to Get data button
    if "Get_state" not in st.session_state:
        st.session_state.Get_state = False
    if Get_data or st.session_state.Get_state:
        st.session_state.Get_state = True


        # Access youtube API
        api_service_name = 'youtube'
        api_version = 'v3'
        api_key = 'AIzaSyDQYpHKd36QOoBloxRsemSzSITMepCzoTU'
        youtube = build(api_service_name,api_version,developerKey =api_key)


        
        # Define a function to retrieve channel data
        def get_channel_data(youtube,channel_id):
            try:
                try:
                    channel_request = youtube.channels().list(
                        part = 'snippet,statistics,contentDetails',
                        id = channel_id)
                    channel_response = channel_request.execute()
                    
                    if 'items' not in channel_response:
                        st.write(f"Invalid channel id: {channel_id}")
                        st.error("Enter the correct 11-digit **channel_id**")
                        return None
                    
                    return channel_response
                
                except HttpError as e:
                    st.error('Server error (or) Check your internet connection (or) Please Try again after a few minutes', icon='🚨')
                    st.write('An error occurred: %s' % e)
                    return None
            except:
                st.write('You have exceeded your YouTube API quota. Please try again tomorrow.')


        # Function call to Get Channel data from a single channel ID 
        channel_data = get_channel_data(youtube,channel_id)

        # Process channel data
        # Extract required information from the channel_data
        channel_name = channel_data['items'][0]['snippet']['title']
        channel_video_count = channel_data['items'][0]['statistics']['videoCount']
        channel_subscriber_count = channel_data['items'][0]['statistics']['subscriberCount']
        channel_view_count = channel_data['items'][0]['statistics']['viewCount']
        channel_description = channel_data['items'][0]['snippet']['description']
        channel_playlist_id = channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

     
          # Format channel_data into dictionary
        channel = {
            "Channel_Details": {
                "Channel_Name": channel_name,
                "Channel_Id": channel_id,
                "Video_Count": channel_video_count,
                "Subscriber_Count": channel_subscriber_count,
                "Channel_Views": channel_view_count,
                "Channel_Description": channel_description,
                "Playlist_Id": channel_playlist_id
            }
        }


        # Define a function to retrieve video IDs from channel playlist
        def get_video_ids(youtube, channel_playlist_id):
            
            video_id = []
            next_page_token = None
            while True:
                # Get playlist items
                request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId=channel_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token)
                response = request.execute()

                # Get video IDs
                for item in response['items']:
                    video_id.append(item['contentDetails']['videoId'])

                # Check if there are more pages
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break

            return video_id

        # Function call to Get  video_ids using channel playlist Id
        video_ids = get_video_ids(youtube, channel_playlist_id)


         # Define a function to retrieve video data
        def get_video_data(youtube, video_ids):
            
            video_data = []
            for video_id in video_ids:
                try:
                    # Get video details
                    request = youtube.videos().list(
                        part='snippet, statistics, contentDetails',
                        id=video_id)
                    response = request.execute()

                    video = response['items'][0]


                    # Get comments if available (comment function call)
                    try:
                        video['comment_threads'] = get_video_comments(youtube, video_id, max_comments=2)
                    except:
                        video['comment_threads'] = None

                    # Duration format transformation (Duration format transformation function call)
                    duration = video.get('contentDetails', {}).get('duration', 'Not Available')
                    if duration != 'Not Available':
                        duration = convert_duration(duration)
                    video['contentDetails']['duration'] = duration        
                                
                    video_data.append(video)
                    
                except:
                    st.write('You have exceeded your YouTube API quota. Please try again tomorrow.')

            return video_data
        

        # Define a function to retrieve video comments
        def get_video_comments(youtube, video_id, max_comments):
            
            request = youtube.commentThreads().list(
                part='snippet',
                maxResults=max_comments,
                textFormat="plainText",
                videoId=video_id)
            response = request.execute()
            
            return response
        

         # Define a function to convert duration
        def convert_duration(duration):
            regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
            match = re.match(regex, duration)
            if not match:
                return '00:00:00'
            hours, minutes, seconds = match.groups()
            hours = int(hours[:-1]) if hours else 0
            minutes = int(minutes[:-1]) if minutes else 0
            seconds = int(seconds[:-1]) if seconds else 0
            total_seconds = hours * 3600 + minutes * 60 + seconds
            return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))
        

        # Function call to Get Videos data and comment data from video ids
        video_data = get_video_data(youtube, video_ids)


        # video details processing
        videos = {}
        for i, video in enumerate (video_data):
            video_id = video['id']
            video_name = video['snippet']['title']
            video_description = video['snippet']['description']
            tags = video['snippet'].get('tags', [])
            published_at = video['snippet']['publishedAt']
            view_count = video['statistics']['viewCount']
            like_count = video['statistics'].get('likeCount', 0)
            dislike_count = video['statistics'].get('dislikeCount', 0)
            favorite_count = video['statistics'].get('favoriteCount', 0)
            comment_count = video['statistics'].get('commentCount', 0)
            duration = video.get('contentDetails', {}).get('duration', 'Not Available')
            thumbnail = video['snippet']['thumbnails']['high']['url']
            caption_status = video.get('contentDetails', {}).get('caption', 'Not Available')
            comments = 'Unavailable'

            # Handle case where comments are enabled
            if video['comment_threads'] is not None:
                comments = {}
                for index, comment_thread in enumerate(video['comment_threads']['items']):
                    comment = comment_thread['snippet']['topLevelComment']['snippet']
                    comment_id = comment_thread['id']
                    comment_text = comment['textDisplay']
                    comment_author = comment['authorDisplayName']
                    comment_published_at = comment['publishedAt']
                    comments[f"Comment_Id_{index + 1}"] = {
                        'Comment_Id': comment_id,
                        'Comment_Text': comment_text,
                        'Comment_Author': comment_author,
                        'Comment_PublishedAt': comment_published_at
                    }


            # Format processed video data into dictionary        
            videos[f"Video_Id_{i + 1}"] = {
                'Video_Id': video_id,
                'Video_Name': video_name,
                'Video_Description': video_description,
                'Tags': tags,
                'PublishedAt': published_at,
                'View_Count': view_count,
                'Like_Count': like_count,
                'Dislike_Count': dislike_count,
                'Favorite_Count': favorite_count,
                'Comment_Count': comment_count,
                'Duration': duration,
                'Thumbnail': thumbnail,
                'Caption_Status': caption_status,
                'Comments': comments
            }

        #combine channel data and videos data to a dict 
        final_output = {**channel, **videos}

        # -----------------------------------    /   MongoDB connection and store the collected data   /    ---------------------------------- #

        # create a client instance of MongoDB
        client = pymongo.MongoClient('mongodb://localhost:27017/')

        # create a database or use existing one
        mydb = client['YouTube_DB']

        # create a collection
        collection = mydb['Youtube_data']

        # define the data to insert
        final_output_data = {
            'Channel_Name': channel_name,
            "Channel_data":final_output
            }
        

         # insert or update data in the collection
        upload = collection.replace_one({'_id': channel_id}, final_output_data, upsert=True)

        # print the result of the insertion operation
        st.write(f"Updated document id: {upload.upserted_id if upload.upserted_id else upload.modified_count}")

        # Close the connection
        client.close()


# ========================================   /     Data Migrate zone (Stored data to MySQL)    /   ========================================== #

with col2:
    st.header(':violet[Data Migrate zone]')
    st.write ('''(Note:- This zone specific channel data **Migrate to :blue[MySQL] database from  :green[MongoDB] database** depending on your selection,
                if unavailable your option first collect data.)''')
    
     # Connect to the MongoDB server
    client = pymongo.MongoClient("mongodb://localhost:27017/")

    # create a database or use existing one
    mydb = client['YouTube_DB']

    # create a collection
    collection = mydb['Youtube_data']

    # create a collection# Collect all document names and give them
    document_names = []
    for document in collection.find():
        document_names.append(document["Channel_Name"])
    document_name = st.selectbox('**Select Channel name**', options = document_names, key='document_names')
    st.write('''Migrate to MySQL database from MongoDB database to click below **:blue['Migrate to MySQL']**.''')
    Migrate = st.button('**Migrate to MySQL**')

    # Define Session state to Migrate to MySQL button
    if 'migrate_sql' not in st.session_state:
        st.session_state_migrate_sql = False
    if Migrate or st.session_state_migrate_sql:
        st.session_state_migrate_sql = True

        # Retrieve the document with the specified name
        result = collection.find_one({"Channel_Name": document_name})
        client.close()

        # ----------------------------- Data conversion --------------------- #

        # Channel data json to df
        channel_details_to_sql = {
            "Channel_Name": result['Channel_Name'],
            "Video_Count": result['Channel_data']['Channel_Details']['Video_Count'],
            "Subscriber_Count": result['Channel_data']['Channel_Details']['Subscriber_Count'],
            "Channel_Views": result['Channel_data']['Channel_Details']['Channel_Views'],
            "Channel_Description": result['Channel_data']['Channel_Details']['Channel_Description'],
            "Playlist_Id": result['Channel_data']['Channel_Details']['Playlist_Id']
            }
        channel_df = pd.DataFrame.from_dict(channel_details_to_sql, orient='index').T
              

              # playlist data json to df
        playlist_tosql = {"Channel_Id": result['_id'],
                        "Playlist_Id": result['Channel_data']['Channel_Details']['Playlist_Id']
                        }
        playlist_df = pd.DataFrame.from_dict(playlist_tosql, orient='index').T

        # video data json to df
        video_details_list = []
        for i in range(1,len(result['Channel_data'])-1):
            video_details_tosql = {
                'Playlist_Id':result['Channel_data']['Channel_Details']['Playlist_Id'],
                'Video_Id': result['Channel_data'][f"Video_Id_{i}"]['Video_Id'],
                'Video_Name': result['Channel_data'][f"Video_Id_{i}"]['Video_Name'],
                'Video_Description': result['Channel_data'][f"Video_Id_{i}"]['Video_Description'],
                'Published_date': result['Channel_data'][f"Video_Id_{i}"]['PublishedAt'],
                'View_Count': result['Channel_data'][f"Video_Id_{i}"]['View_Count'],
                'Like_Count': result['Channel_data'][f"Video_Id_{i}"]['Like_Count'],
                'Dislike_Count': result['Channel_data'][f"Video_Id_{i}"]['Dislike_Count'],
                'Favorite_Count': result['Channel_data'][f"Video_Id_{i}"]['Favorite_Count'],
                'Comment_Count': result['Channel_data'][f"Video_Id_{i}"]['Comment_Count'],
                'Duration': result['Channel_data'][f"Video_Id_{i}"]['Duration'],
                'Thumbnail': result['Channel_data'][f"Video_Id_{i}"]['Thumbnail'],
                'Caption_Status': result['Channel_data'][f"Video_Id_{i}"]['Caption_Status']
                }
            video_details_list.append(video_details_tosql)
        video_df = pd.DataFrame(video_details_list)

        # Comment data json to df
        Comment_details_list = []
        for i in range(1, len(result['Channel_data']) - 1):
            comments_access = result['Channel_data'][f"Video_Id_{i}"]['Comments']
            if comments_access == 'Unavailable' or ('Comment_Id_1' not in comments_access or 'Comment_Id_2' not in comments_access) :
                Comment_details_tosql = {
                    'Video_Id': 'Unavailable',
                    'Comment_Id': 'Unavailable',
                    'Comment_Text': 'Unavailable',
                    'Comment_Author':'Unavailable',
                    'Comment_Published_date': 'Unavailable',
                    }
                Comment_details_list.append(Comment_details_tosql)
                
            else:
                for j in range(1,3):
                    Comment_details_tosql = {
                    'Video_Id': result['Channel_data'][f"Video_Id_{i}"]['Video_Id'],
                    'Comment_Id': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_Id'],
                    'Comment_Text': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_Text'],
                    'Comment_Author': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_Author'],
                    'Comment_Published_date': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_PublishedAt'],
                    }
                    Comment_details_list.append(Comment_details_tosql)
        Comments_df = pd.DataFrame(Comment_details_list)

        # -------------------- Data Migrate to MySQL --------------- #

        # Connect to the MySQL server
        connect = mysql.connector.connect(
        host = "127.0.0.1",
        user = "root",
        password = "sunanda1")


        # Create a new database and use
        mycursor = connect.cursor(buffered=True)
        mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")

        # Close the cursor and database connection
        mycursor.close()
        connect.close()
    
    # Connect to the new created database
        engine = create_engine('mysql+mysqlconnector://root:root@localhost/youtube_db', echo=False)

        # Use pandas to insert the DataFrames data to the SQL Database -> table1

        # Channel data to SQL
        channel_df.to_sql('channel', engine, if_exists='append', index=False,
                        dtype = {"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                                "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                "Video_Count": sqlalchemy.types.INT,
                                "Subscriber_Count": sqlalchemy.types.BigInteger,
                                "Channel_Views": sqlalchemy.types.BigInteger,
                                "Channel_Description": sqlalchemy.types.TEXT,
                                "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),})
        
        # Playlist data to SQL
        playlist_df.to_sql('playlist', engine, if_exists='append', index=False,
                        dtype = {"Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                    "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),})

        # Video data to SQL
        video_df.to_sql('video', engine, if_exists='append', index=False,
                    dtype = {'Playlist_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Name': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Description': sqlalchemy.types.TEXT,
                            'Published_date': sqlalchemy.types.String(length=50),
                            'View_Count': sqlalchemy.types.BigInteger,
                            'Like_Count': sqlalchemy.types.BigInteger,
                            'Dislike_Count': sqlalchemy.types.INT,
                            'Favorite_Count': sqlalchemy.types.INT,
                            'Comment_Count': sqlalchemy.types.INT,
                            'Duration': sqlalchemy.types.VARCHAR(length=1024),
                            'Thumbnail': sqlalchemy.types.VARCHAR(length=225),
                            'Caption_Status': sqlalchemy.types.VARCHAR(length=225),})

        # Commend data to SQL
        Comments_df.to_sql('comments', engine, if_exists='append', index=False,
                        dtype = {'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                                'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                                'Comment_Text': sqlalchemy.types.TEXT,
                                'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                                'Comment_Published_date': sqlalchemy.types.String(length=50),})



                    



