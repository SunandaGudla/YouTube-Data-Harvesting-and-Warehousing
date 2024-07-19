import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build
from PIL import Image

# SETTING PAGE CONFIGURATIONS
icon = Image.open("Youtube_logo.png")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing | By Sunanda",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by *Sunanda!*"""})
# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Extract & Transform","View"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}})
    
    # Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.create_db_connection

# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(host="127.0.0.1",
                   user="root",
                   password="Sunanda@1",
                   database= "create_db_connection",
                   port = "3306"
                  )
mycursor = mydb.cursor(buffered=True)

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyA7uNtCLhYFkpUFrzS9GnLCURyb-Ngzjes" #"this is api key" 
youtube = build('youtube','v3',developerKey=api_key)


# Function to get channel names from the database
def channel_names():
    conn = create_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT channel_name FROM channels")
    channels = [row[0] for row in cursor.fetchall()]
    conn.close()
    return channels

# Streamlit UI
st.title("YouTube Insights")

selected = st.selectbox("Select an option", ["Transform to SQL", "View"])

if selected == "Transform to SQL":
    st.markdown("#   ")
    st.markdown("### Select a channel to begin Transformation to SQL")
    ch_names = channel_names()
    user_inp = st.selectbox("Select channel", options=ch_names)

    mydb = create_db_connection()
    mycursor = mydb.cursor()

    def insert_into_channels():
        collections = db.channel_details
        query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i in collections.find({"channel_name": user_inp}, {'_id': 0}):
            mycursor.execute(query, tuple(i.values()))
        mydb.commit()

    def insert_into_videos():
        collections1 = db.video_details
        query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        for i in collections1.find({"channel_name": user_inp}, {'_id': 0}):
            values = [str(val).replace("'", "''").replace('"', '""') if isinstance(val, str) else val for val in i.values()]
            mycursor.execute(query1, tuple(values))
            mydb.commit()

    def insert_into_comments():
        collections1 = db.video_details
        collections2 = db.comments_details
        query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""

        for vid in collections1.find({"channel_name": user_inp}, {'_id': 0}):
            for i in collections2.find({'Video_id': vid['Video_id']}, {'_id': 0}):
                mycursor.execute(query2, tuple(i.values()))
                mydb.commit()

    if st.button("Submit"):
        try:
            insert_into_videos()
            insert_into_channels()
            insert_into_comments()
            st.success("Transformation to MySQL Successful !!")
        except:
            st.error("Channel details already transformed !!")

    mydb.close()

# VIEW PAGE
if selected == "View":

    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions', [
        '1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
    ])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mydb = create_db_connection()
        mycursor = mydb.cursor()
        mycursor.execute("""SELECT Video_name AS Video_name, channel_name AS Channel_Name
                            FROM videos
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(), columns=["Video_name", "Channel_Name"])
        st.write(df)
        mydb.close()

    # Implement similar logic for other questions

st.stop()



