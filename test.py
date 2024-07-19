from googleapiclient.discovery import build

import streamlit as st
from streamlit_option_menu import option_menu

import pandas as pd

import re

import pymongo

import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql

# Set the page configuration
st.set_page_config(
    layout="wide",  # Use "wide" layout
)

selected = option_menu(
    menu_title = "YOUTUBE DATA HARVESTING AND WAREHOUSING",
    options = ["DATA COLLECTION", "SELECT AND STORE", "MIGRATION OF DATA", "DATA ANALYSIS"],
    menu_icon = "youtube",
    orientation = "horizontal"
)

Yt_channels = {
    'NAME': ["CHENNAI SUPER KINGS", "MUMBAI INDIANS", "ROYAL CHALLENGERS BANGALORE",
              "KOLKATA KNIGHT RIDERS", "RAJASTAN ROYALS", "PUNJAB KINGS", "DELHI CAPITALS",
               "SUN RISERS HYDERABAD", "LUCKNOW SUPER GIANTS", "GUJARAT TITANS"],
    'Channel_ids' : ["UC2J_VKrAzOEJuQvFFtj3KUw", "UCl23mvQ3321L7zO6JyzhVmg",
                    "UCCq1xDJMBRF61kiOgU90_kw", "UCp10aBPqcOeBbEg7d_K9SBw",
                    "UCkpgyRmcNy-aZFLUkKkWK4w", "UCvRa1LWA_-aARq1AQMC4AyA",
                    "UCEzB47eM-HZu04f4mB2nycg", "UCScgEv0U9Wcnk24KfAzGTXg",
                    "UC-mi8xUqL43BMlhvJbAf-Ew", "UCCBe9iIoN9Ar-Elluxca-Xw"]
}

if selected == "DATA COLLECTION":

    st.header("DATA COLLECTION PAGE")
    st.dataframe(Yt_channels, width = 750)

if selected == "SELECT AND STORE":

    channel_id = st.text_input("**Enter a channel ID**")

    store_data = st.button("STORE DATA IN MONGODB")

    if store_data:

        # Setting API key
        api_key = "AIzaSyA7uNtCLhYFkpUFrzS9GnLCURyb-Ngzjes"
        api_service_name = "youtube"
        api_version = "v3"

        # Initializing Youtube data API client
        youtube = build(api_service_name, api_version, developerKey = api_key)

        # Function to get channel details

        def get_channel_details(channel_id):
    
            request = youtube.channels().list(
                part="snippet, contentDetails, statistics",
                id=channel_id)
            response = request.execute()

            data = {
                "Channel_Id": channel_id,
                "Channel_Name": response['items'][0]['snippet']['title'],
                "Channel_Description": response['items'][0]['snippet']['description'],
                "Subscriber_Count": int(response['items'][0]['statistics']['subscriberCount']),
                "Channel_View_Count": int(response['items'][0]['statistics']['viewCount']),
                "Channel_Video_Count": int(response['items'][0]['statistics']['videoCount']),
                "Playlist_Id": response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            }
    
            return data

        get_channel_details(channel_id)

        channel_data = get_channel_details(channel_id)


        # Function to get video IDs

        def get_video_ids(playlist_id, max_results=10):
            video_ids = []
            next_page_token = None

            while len(video_ids) < max_results:
                request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId=playlist_id,
                    maxResults=min(50, max_results - len(video_ids)),
                    pageToken=next_page_token)
                response = request.execute()

                for item in response['items']:
                    video_ids.append(item['contentDetails']['videoId'])

                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break

            return video_ids

        playlist_id = (channel_data['Playlist_Id'])

        max_results = 10  # Change this to the desired number of video IDs you want
        video_ids = get_video_ids(playlist_id, max_results)

        # Function to get comment details (For a single video - 5 comments)

        def get_comment_data(video_id):
    
            comment_info = {}
        
            request = youtube.commentThreads().list(
                        part='snippet',
                        videoId=video_id, 
                        textFormat='plainText',
                        maxResults=5)
            response = request.execute()

            for index, comment in enumerate(response['items'],start=1):
                comment_id = comment['snippet']['topLevelComment']['id']
                comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay']
                comment_data = {
                    "Comment_Id": comment_id,
                    "Comment_Text": comment_text,
                    "Comment_Author": comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_PublishedAt": comment['snippet']['topLevelComment']['snippet']['publishedAt']}
                comment_info[f"Comment_Id_{index}"] = comment_data

            return comment_info

        for i in range(len(video_ids)):
            video_id = video_ids[i]

        get_comment_data(video_id)

        comment_data = get_comment_data(video_id)

        # Function to get video details

        def get_video_data(video_ids):
    
            dict_video_data = {}

            for i in range(len(video_ids)):
                video_id = video_ids[i]
                
                request = youtube.videos().list(
                    part="snippet, statistics,contentDetails",
                    id = video_id)
                response = request.execute()
        
                video_data = {
                    "Video_Id": video_id,
                    "Video_Name": response['items'][0]['snippet']['title'],
                    "Video_Description": response['items'][0]['snippet']['description'],
                    "Video_View_Count": int(response['items'][0]['statistics'].get('viewCount',0)),
                    "Video_Likes": int(response['items'][0]['statistics'].get('likeCount',0)),
                    "Video_Dislikes": int(response['items'][0]['statistics'].get('dislikeCount',0)),
                    "Video_PublishedAt": response['items'][0]['snippet']['publishedAt'],
                    "Video_Comments_Count": int(response['items'][0]['statistics'].get('commentCount',0)),
                }
            
                if video_data['Video_Comments_Count']>0:
                    video_data['Video_Comments'] = comment_data
            
                else:
                    video_data['Video_Comments'] = "No commments on this video"
            
                dict_video_data[f"Video_Id_{i+1}"] = video_data  
        
            return dict_video_data

        get_video_data(video_ids)

        video_data = get_video_data(video_ids)       


 # Combined data

        combined_data = {**channel_data, **video_data}

        #Connect to MongoDB

        client = pymongo.MongoClient('mongodb://localhost:27017/')
        db = client['YouTube_DB']
        collection = db['Youtube_data']

        final_output = {
            "Channel_Name": channel_data["Channel_Name"],

            "Channel_data": combined_data
            }

        # Insert data into MongoDB
        upload = collection.replace_one({'_id': channel_id}, final_output, upsert=True)

        st.write("Data stored in MongoDB")

        st.json(combined_data)

        client.close()

# Data Migration

if selected == "MIGRATION OF DATA":
    st.header("DATA MIGRATION ZONE")

    # Connect to MongoDB

    client = pymongo.MongoClient('mongodb://localhost:27017/')

    # Create a database or use exsisting one
    db = client['YouTube_DB']

    # Creating a collection
    collection = db['Youtube_data']

    # Collect all document names
    document_names = []
    for document in collection.find():
        document_names.append(document['Channel_data']["Channel_Name"])
    document_name = st.selectbox("Select Channel name", options = document_names, key = "document_names")
    st.write("Click below to migrate data to MySQL from MongoDB")
    migrate = st.button("MIGRATE DATA TO MYSQL")

    if migrate:
        result = collection.find_one({"Channel_Name": document_name})

        client.close()

# Channel data json to df
        channel_details_to_sql = {
            "Channel_Id": result['_id'],
            "Channel_Name": result['Channel_data']['Channel_Name'],
            "Channel_Video_Count": result['Channel_data']['Channel_Video_Count'],
            "Subscriber_Count": result['Channel_data']['Subscriber_Count'],
            "Channel_View_Count": result['Channel_data']['Channel_View_Count'],
            "Channel_Description": result['Channel_data']['Channel_Description'],
            "Playlist_Id": result['Channel_data']['Playlist_Id']
            }
        channel_df = pd.DataFrame.from_dict(channel_details_to_sql, orient='index').T

        # playlist data json to df
        playlist_tosql = {"Channel_Id": result['_id'],
                          "Playlist_Id": result['Channel_data']['Playlist_Id']
                        }
        playlist_df = pd.DataFrame.from_dict(playlist_tosql, orient='index').T

        # video data json to df
        video_details_list = []
        for i in range(1,10+1):
            video_details_tosql = {
                "Playlist_Id":result['Channel_data']['Playlist_Id'],
                "Video_Id": result['Channel_data'][f"Video_Id_{i}"]['Video_Id'],
                "Video_Name": result['Channel_data'][f"Video_Id_{i}"]['Video_Name'],
                "Video_Description": result['Channel_data'][f"Video_Id_{i}"]['Video_Description'],
                "Video_PublishedAt": result['Channel_data'][f"Video_Id_{i}"]['Video_PublishedAt'],
                "Video_View_Count": result['Channel_data'][f"Video_Id_{i}"]['Video_View_Count'],
                "Video_Like_Count": result['Channel_data'][f"Video_Id_{i}"]['Video_Likes'],
                "Video_Dislike_Count": result['Channel_data'][f"Video_Id_{i}"]['Video_Dislikes'],
                'Video_Comment_Count': result['Channel_data'][f"Video_Id_{i}"]['Video_Comments_Count'], }
            video_details_list.append(video_details_tosql)
        video_df = pd.DataFrame(video_details_list)

        # Comment data json to df
        Comment_details_list = []
        for i in range(1, 10+1):
            comments_access = result['Channel_data'][f"Video_Id_{i}"]['Video_Comments']
            if comments_access == 'Unavailable' or ('Comment_Id_1' not in comments_access or 'Comment_Id_2' not in comments_access) :
                Comment_details_tosql = {
                    "Video_Id": 'Unavailable',
                    "Comment_Id": 'Unavailable',
                    "Comment_Text": 'Unavailable',
                    "Comment_Author":'Unavailable',
                    "Comment_PublishedAt": 'Unavailable',
                    }
                Comment_details_list.append(Comment_details_tosql)

            else:
                for j in range(1,5):
                    Comment_details_tosql = {
                    "Video_Id": result['Channel_data'][f"Video_Id_{i}"]['Video_Id'],
                    "Comment_Id": result['Channel_data'][f"Video_Id_{i}"]['Video_Comments'][f"Comment_Id_{j}"]['Comment_Id'],
                    "Comment_Text": result['Channel_data'][f"Video_Id_{i}"]['Video_Comments'][f"Comment_Id_{j}"]['Comment_Text'],
                    "Comment_Author": result['Channel_data'][f"Video_Id_{i}"]['Video_Comments'][f"Comment_Id_{j}"]['Comment_Author'],
                    "Comment_PublishedAt": result['Channel_data'][f"Video_Id_{i}"]['Video_Comments'][f"Comment_Id_{j}"]['Comment_PublishedAt'],
                    }
                    Comment_details_list.append(Comment_details_tosql)
        Comments_df = pd.DataFrame(Comment_details_list)




# -------------------- Data Migrate to MySQL --------------- #

        # Connect to the MySQL server
        connect = mysql.connector.connect(
        host = "localhost",
        user="root",
        password="Sunanda@1",
        auth_plugin = "mysql_native_password")

 # Create a new database and use
        mycursor = connect.cursor()
        mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")

        # Close the cursor and database connection
        mycursor.close()
        connect.close()

# Connect to the new created database
        engine = create_engine('mysql+mysqlconnector://root:yourpassword@localhost/youtube_db', echo=False)

        # Use pandas to insert the DataFrames data to the SQL Database -> table1

        # Channel data to SQL
        channel_df.to_sql('Channel', engine, if_exists='append', index=False,
                        dtype = {"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                                 "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                 "Channel_Video_Count": sqlalchemy.types.INT,
                                 "Subscriber_Count": sqlalchemy.types.BigInteger,
                                 "Channel_View_Count": sqlalchemy.types.BigInteger,
                                 "Channel_Description": sqlalchemy.types.TEXT,
                                 "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),})

        # Playlist data to SQL
        playlist_df.to_sql('Playlist', engine, if_exists='append', index=False,
                        dtype = {"Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                 "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),})

        # Video data to SQL
        video_df.to_sql('Video', engine, if_exists='append', index=False,
                    dtype = {"Playlist_Id": sqlalchemy.types.VARCHAR(length=225),
                             "Video_Id": sqlalchemy.types.VARCHAR(length=225),
                             "Video_Name": sqlalchemy.types.VARCHAR(length=225),
                             "Video_Description": sqlalchemy.types.TEXT,
                             "Video_PublishedAt": sqlalchemy.types.String(length=50),
                             "Video_View_Count": sqlalchemy.types.BigInteger,
                             "Video_Like_Count": sqlalchemy.types.BigInteger,
                             "Video_Dislike_Count": sqlalchemy.types.INT,
                             "Video_Comment_Count": sqlalchemy.types.INT,})

        # Commend data to SQL
        Comments_df.to_sql('Comment', engine, if_exists='append', index=False,
                        dtype = {"Video_Id": sqlalchemy.types.VARCHAR(length=225),
                                 "Comment_Id": sqlalchemy.types.VARCHAR(length=225),
                                 "Comment_Text": sqlalchemy.types.TEXT,
                                 "Comment_Author": sqlalchemy.types.VARCHAR(length=225),
                                 "Comment_PublishedAt": sqlalchemy.types.String(length=50),})
        
# Data Analysis Zone

if selected == "DATA ANALYSIS":

    st.header("DATA ANALYSIS")

    question_tosql = st.selectbox("Select a question:",[
        "1. What are the names of all the videos and their corresponding channels?",
        "2. Which channels have the most number of videos, and how many videos do they have?",
        "3. What are the top 10 most viewed videos and their respective channels?",
        "4. How many comments were made on each video, and what are their corresponding video names?",
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
        "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
        "7. What is the total number of views for each channel, and what are their corresponding channel names?",
        "8. What are the names of all the channels that have published videos in the year 2022?",
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
        ],index=0)
    # Creat a connection to SQL
    connect_for_question = pymysql.connect(host='localhost', user='root', password='yourpassword', db='youtube_db')
    cursor = connect_for_question.cursor()

    # Q1
    if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("SELECT Channel.Channel_Name, Video.Video_Name FROM Channel JOIN Playlist JOIN video ON Channel.Channel_Id = Playlist.Channel_Id AND Playlist.Playlist_Id = Video.Playlist_Id;")
        result_1 = cursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Channel_Name', 'Video_Name']).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    # Q2
    elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':

        cursor.execute("SELECT Channel_Name, Channel_Video_Count FROM Channel ORDER BY Channel_Video_Count DESC;")
        result_2 = cursor.fetchall()
        df2 = pd.DataFrame(result_2,columns=['Channel_Name','Channel_Video_Count']).reset_index(drop=True)
        df2.index += 1
        st.dataframe(df2)

    # Q3
    elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':

        cursor.execute("SELECT Channel.Channel_Name, Video.Video_Name, Video.Video_View_Count FROM Channel JOIN Playlist ON Channel.Channel_Id = Playlist.Channel_Id JOIN Video ON Playlist.Playlist_Id = Video.Playlist_Id ORDER BY Video.Video_View_Count DESC LIMIT 10;")
        result_3 = cursor.fetchall()
        df3 = pd.DataFrame(result_3,columns=['Channel_Name', 'Video_Name', 'Video_View_Count']).reset_index(drop=True)
        df3.index += 1
        st.dataframe(df3)

    # Q4 
    elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("SELECT Channel.Channel_Name, Video.Video_Name, Video.Video_Comment_Count FROM Channel JOIN Playlist ON Channel.Channel_Id = Playlist.Channel_Id JOIN Video ON Playlist.Playlist_Id = Video.Playlist_Id;")
        result_4 = cursor.fetchall()
        df4 = pd.DataFrame(result_4,columns=['Channel_Name', 'Video_Name', 'Video_Comment_count']).reset_index(drop=True)
        df4.index += 1
        st.dataframe(df4)

    # Q5
    elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("SELECT Channel.Channel_Name, Video.Video_Name, Video.Video_Like_Count FROM Channel JOIN Playlist ON Channel.Channel_Id = Playlist.Channel_Id JOIN Video ON Playlist.Playlist_Id = Video.Playlist_Id ORDER BY Video.Video_Like_Count DESC;")
        result_5= cursor.fetchall()
        df5 = pd.DataFrame(result_5,columns=['Channel_Name', 'Video_Name', 'Video_Like_Count']).reset_index(drop=True)
        df5.index += 1
        st.dataframe(df5)

    # Q6
    elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
        cursor.execute("SELECT Channel.Channel_Name, Video.Video_Name, Video.Video_Like_Count, Video.Video_Dislike_Count FROM Channel JOIN Playlist ON Channel.Channel_Id = Playlist.Channel_Id JOIN Video ON Playlist.Playlist_Id = Video.Playlist_Id ORDER BY Video.Video_Like_Count DESC;")
        result_6= cursor.fetchall()
        df6 = pd.DataFrame(result_6,columns=['Channel_Name', 'Video_Name', 'Video_Like_Count','Video_Dislike_Count']).reset_index(drop=True)
        df6.index += 1
        st.dataframe(df6)

    # Q7
    elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':

        cursor.execute("SELECT Channel_Name, Channel_View_Count FROM Channel ORDER BY Channel_View_Count DESC;")
        result_7= cursor.fetchall()
        df7 = pd.DataFrame(result_7,columns=['Channel_Name', 'Channel_View_Count']).reset_index(drop=True)
        df7.index += 1
        st.dataframe(df7)

    # Q8
    elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("SELECT Channel.Channel_Name, Video.Video_Name, Video.Video_PublishedAt FROM Channel JOIN Playlist ON Channel.Channel_Id = Playlist.Channel_Id JOIN Video ON Playlist.Playlist_Id = Video.Playlist_Id  WHERE EXTRACT(YEAR FROM Video_PublishedAt) = 2022;")
        result_8= cursor.fetchall()
        df8 = pd.DataFrame(result_8,columns=['Channel_Name','Video_Name', 'Year 2022 only']).reset_index(drop=True)
        df8.index += 1
        st.dataframe(df8)

    # Q9
    elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("SELECT channel.Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.Duration)))), '%H:%i:%s') AS duration  FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id GROUP by Channel_Name ORDER BY duration DESC ;")
        result_9= cursor.fetchall()
        df9 = pd.DataFrame(result_9,columns=['Channel Name','Average duration of videos (HH:MM:SS)']).reset_index(drop=True)
        df9.index += 1
        st.dataframe(df9)

    # Q10
    elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("SELECT Channel.Channel_Name, Video.Video_Name, Video.Video_Comment_Count FROM Channel JOIN Playlist ON Channel.Channel_Id = Playlist.Channel_Id JOIN Video ON Playlist.Playlist_Id = Video.Playlist_Id ORDER BY Video.Video_Comment_Count DESC;")
        result_10= cursor.fetchall()
        df10 = pd.DataFrame(result_10,columns=['Channel_Name','Video_Name', 'Video_Comment_Count']).reset_index(drop=True)
        df10.index += 1
        st.dataframe(df10)

    # SQL DB connection close
    connect_for_question.close()       
