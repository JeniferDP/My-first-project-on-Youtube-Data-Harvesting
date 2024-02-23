import googleapiclient.discovery
from pymongo import MongoClient
import psycopg2
import pandas as pd
import streamlit as st


api_key="AIzaSyADYWzuZZUq-zxFJWBP8T-ZH8UnjJJgOgE"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

#get channel information
def get_Channelinfo(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response = request.execute()
    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                  Channel_id=i['id'],
                  Subscription_count=i['statistics']['subscriberCount'],
                  Channel_views=i['statistics']['viewCount'],
                  Total_video=i['statistics']['videoCount'],
                  Channel_description=i['snippet']['description'],
                  Playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
                 )
        return data


#get video ids
# we have to get video id from playlist id
def get_VideoId(channel_id):
    videoid=[]
    request = youtube.channels().list(id=channel_id,
                part="contentDetails"   
                )
    response=request.execute()    
    Playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None #to get all the video details,we are using this
    while True:
        request1=youtube.playlistItems().list(
            part="snippet",
            playlistId=Playlist_id,
            maxResults=50,
            pageToken=next_page_token)
        response=request1.execute()
        for i in range(len(response['items'])):
            videoid.append(response['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response.get("nextPageToken")
        if next_page_token is None:
            break
       
    return videoid
videoid=get_VideoId("UC5R1S71OG_i40cuZ8fvxwfg")

#get video information
def get_video_info(video_ids):
    video_data=[]
    for i in videoid:
        request=youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=i
        )
        response=request.execute()
        for j in response['items']:#nested for loop to get specific details
            data=dict(Channel_name=j['snippet']['channelTitle'],
                     Channel_id=j['snippet']['channelId'],
                     Video_id=j['id'],
                     Video_name=j['snippet']['title'],
                     Video_description=j['snippet']['description'],
                      Published_at=j['snippet']['publishedAt'],
                      View_count=j['statistics'].get('viewCount'),
                      Like_count=j['statistics'].get('likeCount'),
                      Favorite_count=j['statistics']['favoriteCount'],
                      Duration=j['contentDetails']['duration'],
                      Thumbnail=j['snippet']['thumbnails']['default']['url'],
                      Definition=j['contentDetails']['definition'],
                      Caption_status=j['contentDetails']['caption'],
                      Tags=j['snippet'].get('tags'),
                      Comments=j['statistics'].get('commentCount')
                     )
            video_data.append(data)
    return video_data  


#get comments info
def get_comments_info(video_ids):
    try: #suppose if comment data is disabled
        Comment_data=[] #to get all the comments from all the videos
        for i in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=i,
                maxResults=50)
            response = request.execute()
            for j in response['items']:
                    data=dict(Comment_id=j['snippet']['topLevelComment']['id'],
                              Video_id=j['snippet']['topLevelComment']['snippet']['videoId'],
                              Comment_text=j['snippet']['topLevelComment']['snippet']['textDisplay'],
                              Comment_author=j['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                              Comment_publishedat=j['snippet']['topLevelComment']['snippet']['publishedAt']
                             )
                    Comment_data.append(data)
                
    except:
        pass
    
    return Comment_data

from pymongo import MongoClient
connection=MongoClient("mongodb+srv://jenifer:TrLSfNWpSYi04oLY@cluster0.hwls57f.mongodb.net/?retryWrites=true&w=majority")
connection
#creating db name
db=connection['project1']
col=db['youtubedata']

#Mongodb connection
def channeldetails(channel_id):
    channel_details=get_Channelinfo(channel_id)
    videoid=get_VideoId(channel_id)
    videoinfo=get_video_info(videoid)
    comment_info=get_comments_info(videoid)
    col=db['youtubedata']
    col.insert_one({"channel_information":channel_details,"video_information":videoinfo,"comment_information":comment_info})
    return "uploaded successfully"

#Table creation for channels
def channels_table():
    connection=psycopg2.connect(host="localhost",user="postgres",password="12345",database="youtube_data",port="5432")
    mycursor=connection.cursor()

    drop_query="""drop table if exists channels"""
    mycursor.execute(drop_query)
    connection.commit()
    try:
        query="""create table channels(Channel_Name varchar(400),Channel_id varchar(200) primary key,
                                        Subscription_count bigint,Channel_views bigint,Total_video int,
                                        Channel_description text,Playlist_id varchar(300))"""
        mycursor.execute(query)    
        connection.commit()
    except:
        print("channel table created")
        
    ch_list=[]
    for ch_data in col.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df1=pd.DataFrame(ch_list)
    df1
    for index,row in df1.iterrows():
            insert_query="""insert into channels(Channel_Name ,Channel_id,
                                                Subscription_count ,  Channel_views,
                                                Total_video ,Channel_description,
                                                Playlist_id) 
                                                values(%s,%s,%s,%s,%s,%s,%s)"""
            values=(row["Channel_Name"],row["Channel_id"],
                    row["Subscription_count"],row["Channel_views"],
                    row["Total_video"],row["Channel_description"],row["Playlist_id"])
        
            mycursor.execute(insert_query,values)
            connection.commit()
            



#table creation for videos
def videos_table():
        connection=psycopg2.connect(host="localhost",user="postgres",password="12345",database="youtube_data",port="5432")
        mycursor=connection.cursor()

        drop_query="""drop table if exists videos"""
        mycursor.execute(drop_query)
        connection.commit()
        try:
                query="""create table videos(Channel_name varchar(400),Channel_id varchar(200),
                                                Video_id varchar(200),Video_name varchar(300),Video_description text,
                                                Published_at timestamp,View_count bigint,Like_count bigint,Favorite_count bigint,
                                                Duration interval,
                                                Thumbnail varchar(200),
                                                Definition varchar(10),
                                                Caption_status varchar(100),Tags text,Comments bigint)"""
                mycursor.execute(query)    
                connection.commit()
        except:
                print("video tables created")

        vi_list=[]
        for vi_data in col.find({},{'_id':0,'video_information':1}):
                for i in range(len(vi_data['video_information'])):
                        vi_list.append(vi_data['video_information'][i])
        df2=pd.DataFrame(vi_list)
        df2
        for index,row in df2.iterrows():
                insert_query="""insert into videos(Channel_name,Channel_id,
                                        Video_id,Video_name,Video_description,
                                        Published_at,View_count,Like_count,Favorite_count,
                                        Duration,Thumbnail,
                                        Definition,Caption_status,Tags,Comments) 
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                values=(row["Channel_name"],row["Channel_id"],
                        row["Video_id"],row["Video_name"],row["Video_description"],
                        row["Published_at"],row["View_count"],row["Like_count"],row["Favorite_count"] ,
                        row["Duration"],row["Thumbnail"],row["Definition"],
                        row["Caption_status"],row["Tags"],row["Comments"])
                
                mycursor.execute(insert_query,values)
                connection.commit()

                

#table creation for comments
def comments_table():
     connection=psycopg2.connect(host="localhost",user="postgres",password="12345",database="youtube_data",port="5432")
     mycursor=connection.cursor()
     drop_query="""drop table if exists comments"""
     mycursor.execute(drop_query)
     connection.commit()
     query="""create table comments(Comment_id varchar(600) primary key,
                                   Video_id varchar(400),
                                   Comment_text text,
                                   Comment_author varchar(200),
                                   Comment_publishedat timestamp)"""
     mycursor.execute(query)    
     connection.commit()


     com_list=[]
     for com_data in col.find({},{'_id':0,'comment_information':1}):
          for i in range(len(com_data['comment_information'])): # to get all the comments in each video
               com_list.append(com_data['comment_information'][i])
     df3=pd.DataFrame(com_list)
     for index,row in df3.iterrows():
                    insert_query="""insert into comments(Comment_id, 
                                                       Video_id, 
                                                       Comment_text, 
                                                       Comment_author,
                                                       Comment_publishedat) 
                                                  values(%s,%s,%s,%s,%s)"""
                    values=(row["Comment_id"], 
                         row["Video_id"], 
                         row["Comment_text"], 
                         row["Comment_author"],
                         row["Comment_publishedat"])
                    
                    mycursor.execute(insert_query,values)
                    connection.commit()


#function calling tables
def tables():
    channels_table()
    videos_table()
    comments_table()

    return "Tables created successfully"

def show_channels_table():
    ch_list=[]
    for ch_data in col.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df1=pd.DataFrame(ch_list)
    

    return df1

def show_videos_table():
    vi_list=[]
    for vi_data in col.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
             vi_list.append(vi_data['video_information'][i])
    df2=pd.DataFrame(vi_list)
    return df2


def show_comments_table():
    com_list=[]
    for com_data in col.find({},{'_id':0,'comment_information':1}):
         for i in range(len(com_data['comment_information'])): # to get all the comments in each video
               com_list.append(com_data['comment_information'][i])
    df3=pd.DataFrame(com_list)
    
    
    return df3

   


#streamlit
 
with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header(":red[Skills Interested]")
    st.caption("Python coding")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API key integration")
    st.caption("Data Management in MongoDB and SQL")


channel_id=st.text_input("Enter the Channel ID")

if st.button("Collect the data"):
    ch_ids=[]
    db=connection["project1"]
    col=db["youtubedata"]
    for ch_data in col.find({},{'_id':0,'channel_information':1}):
        ch_ids.append(ch_data["channel_information"]["Channel_id"])

    if channel_id in ch_ids:
        st.success("Channel id already exists")

    else:
        insert=channeldetails(channel_id)
        st.success(insert)
 

if st.button("Migrate to SQL"):
    table=tables()
    st.success(table)


show_table=st.radio("SELECT THE TABLE",("CHANNELS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
   st.dataframe(show_channels_table())

if show_table=="VIDEOS":
    st.dataframe(show_videos_table())

if show_table=="COMMENTS":
    st.dataframe(show_comments_table())
    

#sql connection
import streamlit as st
connection=psycopg2.connect(host="localhost",user="postgres",password="12345",database="youtube_data",port="5432")
mycursor=connection.cursor()

question=st.selectbox("SELECT YOUR QUESTION",("1. What are the names of all the videos and their corresponding channels?",
                                              "2. Which channels have the most number of videos, and how many videos do they have?",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video?",
                                              "5.Which videos have the highest number of likes?",
                                              "6.What is the total number of likes for each video?",
                                              "7.What is the total number of views for each channel?",
                                              "8.What are the names of all the channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel?",
                                              "10.Which videos have the highest number of comments?"))

if question=="1. What are the names of all the videos and their corresponding channels?":
    query1='''select video_name as videos,channel_name as channelname from videos'''
    mycursor.execute(query1)
    connection.commit()
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["video_title","channel name"])
    st.write(df)


    
elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelname,total_video as no_of_videos from channels
               order by total_video desc'''
    mycursor.execute(query2)
    connection.commit()
    t2=mycursor.fetchall()
    df4=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df4)

elif question=="3.What are the top 10 most viewed videos and their respective channels?":
    query3='''select view_count as viewcount,channel_name as channelname,video_name as videotitle from videos 
                where view_count is not null order by view_count desc limit 10'''
    mycursor.execute(query3)
    connection.commit()
    t3=mycursor.fetchall()
    df5=pd.DataFrame(t3,columns=["view count","channel name","videotitle"])
    st.write(df5)

elif question=="4.How many comments were made on each video?":
    query4='''select comments as no_comments,video_name as videotitle from videos 
                where comments is not null'''
    mycursor.execute(query4)
    connection.commit()
    t4=mycursor.fetchall()
    df6=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df6)

elif question=="5.Which videos have the highest number of likes?":
    query5='''select video_name as videotitle,channel_name as channelname,like_count as likecount from videos 
                where like_count is not null order by like_count desc'''
    mycursor.execute(query5)
    connection.commit()
    t5=mycursor.fetchall()
    df7=pd.DataFrame(t5,columns=["video name","channel name","like count"])
    st.write(df7)

elif question=="6.What is the total number of likes for each video?":
    query6='''select like_count as likecount,video_name as videotitle from videos'''
    mycursor.execute(query6)
    connection.commit()
    t6=mycursor.fetchall()
    df8=pd.DataFrame(t6,columns=["like count","video title"])
    st.write(df8)


elif question=="7.What is the total number of views for each channel?":
    query7='''select channel_name as channelname,channel_views as viewcount from channels'''
    mycursor.execute(query7)
    connection.commit()
    t7=mycursor.fetchall()
    df9=pd.DataFrame(t7,columns=["channel name","view count"])
    st.write(df9)

elif question=="8.What are the names of all the channels that have published videos in the year 2022?":
    query8='''select video_name as videoname,published_at as videorelease,channel_name as channelname from videos
                where extract(year from published_at)=2022'''
    mycursor.execute(query8)
    connection.commit()
    t8=mycursor.fetchall()
    df10=pd.DataFrame(t8,columns=["video name","published date","channel name"])
    st.write(df10)


elif question=="9.What is the average duration of all videos in each channel?":
    query9='''select channel_name as channelname,AVG(duration) as avgduration from videos
                group by channel_name'''
    mycursor.execute(query9)
    connection.commit()
    t9=mycursor.fetchall()
    df11=pd.DataFrame(t9,columns=["channel name","average_duration"])
    
    T9=[]
    for index,row in df11.iterrows():
        channel_title=row["channel name"]
        avg_duration=row["average_duration"]
        avg_duration_str=str(avg_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=avg_duration_str))
    df=pd.DataFrame(T9)
    st.write(df)

elif question=="10.Which videos have the highest number of comments?":
    query10='''select video_name as videoname,channel_name as channelname,comments as comments from videos
                where comments is not null order by comments desc'''
    mycursor.execute(query10)
    connection.commit()
    t10=mycursor.fetchall()
    df12=pd.DataFrame(t10,columns=["video name","channel name","comments count"])
    st.write(df12)

                     