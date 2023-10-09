import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import pymongo
import mysql.connector 
from googleapiclient.discovery import build
from PIL import Image
import datetime
from datetime import datetime



st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing |Lalithkishore",
                 
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by *Lalithkishore!*"""})

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Extract & Transform","View"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "15px", "text-align": "centre", "margin": "0px", 
                                    "--green-color": "#000000"},
                                   "icon": {"font-size": "15px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#1e1e45"}})
    


# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)

from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB URI
db = client["youtube"]
coll1=db["channel_details"]
mycol =db["youtubedata"]



# CONNECTING WITH MYSQL DATABASE

import mysql.connector

mydb =mysql.connector.connect(
  host="localhost",
  user="root",
  password= "1234",
  port = "3306",
  database="databasename"
 )
print(mydb) 
mycursor = mydb.cursor(buffered=True)



# CONNECTING WITH MYSQL DATABASE


# BUILDING CONNECTION WITH YOUTUBE API
api_key="AIzaSyCOW-FNWWCFftR5v1upobTwU_miTpfQlg4"
#api_service_name "youtube" #api_version = 'v3'
#channel_id= 'UCXhbCCZAG4GlaBLm80ZL-iA'
youtube = build("youtube","v3",developerKey = api_key)

def get_channel_stats(youtube):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id="UCXhbCCZAG4GlaBLm80ZL-iA")
    response = request.execute()
    
    return response

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    all_data=[]
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
    )
    response = request.execute()
    if 'items' in response:
        for i in range(len(response['items'])):

           data=dict(
                  channel_name=response['items'][i]['snippet']['title'],
                  subscriberCount=int(response['items'][i]['statistics']['subscriberCount']),
                  viewCount=int(response['items'][i]['statistics']['viewCount']),
                  total_count=int(response['items'][i]['statistics']['videoCount']),
                  publishedAt = response['items'][i]['snippet']['publishedAt'].replace('Z', ''),
                  chl_id=channel_id,
                  description=response['items'][i]['snippet']['description'],
                  playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads']
                  
                  
                 )
        all_data.append(data)
        
    return all_data

        

#duration conversion 
def extract_time_components(duration_str):
 
    duration_str = duration_str[2:]
 
    hours, minutes, seconds = 0, 0, 0

    parts = duration_str.split('H')
    if len(parts) > 1:
        hours = int(parts[0])
        duration_str = parts[1]

    parts = duration_str.split('M')
    if len(parts) > 1:
        minutes = int(parts[0])
        duration_str = parts[1]

    if 'S' in duration_str:
        seconds = int(duration_str.split('S')[0])

  
    hours_str = str(hours).zfill(2)
    minutes_str = str(minutes).zfill(2)
    seconds_str = str(seconds).zfill(2)

    return ":".join([hours_str, minutes_str, seconds_str])  



# FUNCTION TO GET VIDEO ID
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id

    res = youtube.channels().list(id=channel_id, 
                                  part='snippet,contentDetails,statistics').execute()
    if 'items' in res:
        playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        next_page_token = None
    
        while True:
            res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
            for i in range(len(res['items'])):
                video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token = res.get('nextPageToken')
            if next_page_token is None:
                break
        return video_ids




# FUNCTION TO GET VIDEO Details
def get_video_details(video_id):
    video_stats = []
    
    for i in range(0,len(video_id),50):
        
        response = youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    id=','.join(video_id[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                
                                
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = extract_time_components(video['contentDetails']['duration']),
                                Views = int(video['statistics']['viewCount']),
                                Likes = int(video['statistics'].get('likeCount')),
                                Comments = int(video['statistics'].get('commentCount')),
                                
                                
                               )
            video_stats.append(video_details)
    return video_stats


#comment details
def get_comments(video_id):
    list_of_comments=[]
    for i in video_id:
         response=(youtube.commentThreads().list(
            part="snippet,replies",
            videoId=i, maxResults=50)).execute()
         for item in response["items"]:
            comment_info={
                " Video_id" : i,
                
                "comment_id":item['snippet']['topLevelComment']['id'],
                "comment_text":item['snippet']['topLevelComment']['snippet']['textDisplay'],
                "comment_author":item['snippet'][ 'topLevelComment']['snippet']['authorDisplayName'],
                "comment_publishedAt":item['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            list_of_comments.append(comment_info)
            
    return list_of_comments




#channel name
def channel_names(): 

    ch_name = []
    for i in coll1.find():
        ch_name.append(i['channel_name'])
    return ch_name

# HOME PAGE
if selected == "Home":
    # Title Image
    
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("## :blue[Domain] : Social Media")
    col1.markdown("## :green[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown("## :red[Overview] : Takeing the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")
  



# EXTRACT AND TRANSFORM PAGE
if selected == "Extract & Transform":
    tab1,tab2 = st.tabs(["$\huge 📝 EXTRACT $", "$\huge🚀 TRANSFORM $"])

    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input("Hint : Enter channel_id")
        
        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["channel_name"]}" channel]')

            st.write(ch_details)
            
            
        
        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_details(ch_id)
                coll1 = db.channel_details
                coll1.insert_many(ch_details)
                
                def main(channel_id):
                    channel_st=get_channel_details(channel_id)
                    video_id = get_channel_videos(channel_id)
                    videodetails = get_video_details(video_id)
                    comments = get_comments(video_id)
                    data={
                        'channel_details':channel_st,
                        'video_details':videodetails,
                        'comment_details':comments,
                        }
                    return data 
                data = main(ch_id)
                mycol.insert_one(data)
                st.success("Upload to MongoDB successful !!")



    #TRANSFORM TAB
    with tab2: 
            st.markdown("#   ")
            st.write("### Select a channel to begin Transformation to SQL")
            ch_names = channel_names()
            user_inp = st.selectbox("Select channel",options= ch_names)
            d=mycol.find_one({'channel_details.channel_name':user_inp},{'_id':0})
            
    def insert_details(d):
        for i in d['channel_details']:
           query = """INSERT INTO channeldetails VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
           values = tuple(i.values())
           mycursor.execute(query, values)
           mydb.commit()
        for i in d['video_details']:
            query = """INSERT INTO videodetails VALUES ( %s, %s, %s, %s, %s, %s,%s,%s,%s,%s)"""
            values = tuple(i.values())
            mycursor.execute(query, values)
            mydb.commit()
        for i in d['comment_details']:
            query = """INSERT INTO commentsdetails VALUES ( %s, %s, %s, %s, %s)"""
            values = tuple(i.values())
            mycursor.execute(query, values)
            mydb.commit()
    


    if st.button("Submit"):
           
           insert_details(d)
           st.success("Transformation to MySQL Successful !!")
        

# VIEW PAGE
if selected == "View":
    
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT Title AS Video_name, channel_name AS Channel_Name
                            FROM videodetails
                            ORDER BY Title""")
        df = pd.DataFrame(mycursor.fetchall(),columns= mycursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, max(total_count) AS Total_Videos
                            FROM channeldetails
                            group by channel_name 
                            ORDER BY channel_name  DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name,Title,views from videodetails order by views desc limit 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""select Title as  Video_name,commentCount from videodetails order by Title""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""select channel_name,Title,likeCount from videodetails order by likeCount desc""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""select channel_name,Title,likeCount from videodetails order by likeCount desc""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""select channel_name,viewCount from channeldetails""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""select distinct channel_name from videodetails where year(publishedAt) = '2022'""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,AVG(duration) / 60 AS "Average_Video_Duration (mins)"
                            FROM videodetails
                            GROUP BY channel_name
                           ORDER BY "Average_Video_Duration (mins)" DESC;""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Avg video duration for channels :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""select channel_name,Title as Video_name,commentCount from videodetails order by commentCount desc limit 1""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
