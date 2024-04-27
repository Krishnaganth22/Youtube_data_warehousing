import googleapiclient.discovery
import pymongo
import pandas as pd
import mysql.connector
import re
import streamlit as st
import time
import plotly.express as px




api_service_name = "youtube"
api_version = "v3"
api_key='AIzaSyBOJQslAidvVay6cpZVdRgzWunEaioSdhA'
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)


def channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    channel_response = request.execute()
    data={'channel_id':channel_response['items'][0]['id'],
          'channel_name':channel_response['items'][0]['snippet']['title'],
          'channel_description':channel_response['items'][0]['snippet']['description'],
          'channel_play':channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
          'channel_vid':channel_response['items'][0]['statistics']['videoCount'],
          'channel_sub':channel_response['items'][0]['statistics']['subscriberCount'],
          'channel_view':channel_response['items'][0]['statistics']['viewCount'] }
    
    return data

def get_video_ids(channel_id):
    video_ids=[]
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    channel_response = request.execute()

    channel_play=channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_Page_Token=None
    while True:
        response1=youtube.playlistItems().list( part='snippet',playlistId=channel_play,maxResults=50,pageToken=next_Page_Token).execute()

        for i in range (len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_Page_Token=response1.get('nextPageToken')
        if next_Page_Token is None:
            break  
    return video_ids



def videos(video_ids):
    try:
        vid=[]
        for i in video_ids:
            request = youtube.videos().list(part="snippet,contentDetails,statistics",id=i)
            video = request.execute()
            tags=(video['items'][0]['snippet'].get('tags',['NA']))
            duration=video['items'][0]['contentDetails'].get('duration')
            if duration != None:
                duration= convert_time(duration)
            
            video_data={'channel_id':video['items'][0]['snippet']['channelId'],
                        'video_id':i,
                        'video_title':video['items'][0]['snippet']['title'],
                        'video_description':video['items'][0]['snippet']['description'],
                        'video_tag':','.join(tags),
                        'video_pub':video['items'][0]['snippet']['publishedAt'],
                        'video_thumb':(video['items'][0]['snippet']['thumbnails']['default']['url']),
                        'video_vc':video['items'][0]['statistics']['viewCount'],
                        'video_like':video['items'][0]['statistics']['likeCount'],
                        'video_fav':video['items'][0]['statistics'].get('favoriteCount'),
                        'video_commcount':video['items'][0]['statistics'].get('commentCount'),
                        'video_dura':duration,
                        'video_cap':video['items'][0]['contentDetails']['caption']}
            vid.append(video_data)

        
        
    except:
        pass
    return vid
        
    #return table

def convert_time(duration):
    regex=r'PT(\d+H)?(\d+M)?(\d+S)'
    match=re.match(regex,duration)
    if not match:
        return '00:00:00'
    hours,minutes,seconds= match.groups()
    hours=int(hours[:-1]) if hours else 0
    minutes=int(minutes[:-1]) if minutes else 0
    seconds=int(seconds[:-1]) if seconds else 0
    total_sec=hours * 3600 + minutes* 60 + seconds
    return '{:02d}:{:02d}:{:02d}'.format(int(total_sec/3600),int((total_sec % 3600)/60),int(total_sec % 60))
            

def commentdet(video_ids):
    comments=[]
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(part='snippet',videoId=video_id,maxResults=100)
            comment = request.execute()
            for item in comment['items']:
                comment_data={'comment_id':item['snippet']['topLevelComment']['id'],
                            'videoid':item['snippet']['topLevelComment']['snippet']['videoId'],
                            'comments':item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            'author':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'published':item['snippet']['topLevelComment']['snippet']['publishedAt']
                                }
                comments.append(comment_data)
        except:
            pass
    return comments


client=pymongo.MongoClient('mongodb+srv://krishnaganth09:Krishna@cluster0.yhgbrws.mongodb.net/')
db=client['Youtube_data']

def channel_details(channel_id):
    channel_info=channel_data(channel_id)
    video_id=get_video_ids(channel_id)
    video_info=videos(video_id)
    comment_info=commentdet(video_id)
    
    coll1=db["channel_details"]

    coll1.insert_one({'Channel_info':channel_info,'Video_info':video_info,
                      'Comment_info':comment_info})
    return 'Uploaded'



mydb= mysql.connector.connect(
    host='localhost',
    user='root',
    password='')
    
mycursor=mydb.cursor(buffered=True)
mycursor.execute("create database if not exists youtube_data")

def channel_table(single_name):
    mydb= mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='Youtube_data'
        )
    mycursor=mydb.cursor(buffered=True)
    
    

    
    create_query='''Create table if not exists Channels(channel_id varchar(100) primary key,
                                                        channel_name varchar(200),
                                                        channel_description text,
                                                        channel_play varchar(100),
                                                        channel_vid bigint,
                                                        channel_sub bigint,
                                                        channel_view bigint)'''
    mycursor.execute(create_query)
    mydb.commit()


    single_ch=[]
    db=client['Youtube_data']
    coll1=db["channel_details"]
    for channel_data in coll1.find({'Channel_info.channel_name':single_name},{'_id':0}):
        single_ch.append(channel_data['Channel_info'])
    df_single_ch=pd.DataFrame(single_ch)

    for index,row in df_single_ch.iterrows():
        channel_insert='''INSERT into channels( channel_id,
                                                channel_name,
                                                channel_description,
                                                channel_play,
                                                channel_vid,
                                                channel_sub,
                                                channel_view
                                                )
                                                
                                                VALUES(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_id'],
                row['channel_name'],
                row['channel_description'],
                row['channel_play'],
                row['channel_vid'],
                row['channel_sub'],
                row['channel_view'])
        try:
            mycursor.execute(channel_insert,values)
            mydb.commit()
        except:
            information=f'Provided channel name {single_name} exists already'
            return information 

def video_table(single_name):   
    mydb= mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='Youtube_data')
    mycursor=mydb.cursor(buffered=True)
    
    

   
    create_query='''Create table if not exists Videos(channel_id varchar(100),
                                                    video_id varchar(100) primary key,
                                                    video_title varchar(200),
                                                    video_description TEXT,
                                                    video_tag TEXT,
                                                    video_pub Timestamp,
                                                    video_thumb varchar(200),
                                                    video_vc BIGINT,
                                                    video_like bigint,
                                                    video_fav bigint,
                                                    video_commcount bigint,
                                                    video_dura time,
                                                    video_cap varchar(100))'''
    mycursor.execute(create_query)
    mydb.commit()
 
    single_vid=[]
    db=client['Youtube_data']
    coll1=db["channel_details"]
    for video_data in coll1.find({'Channel_info.channel_name':single_name},{'_id':0}):
        single_vid.append(video_data['Video_info'])
    df_single_vid=pd.DataFrame(single_vid[0])

    for index,row in df_single_vid.iterrows():
        videos_insert='''INSERT into Videos(channel_id,
                                            video_id,
                                            video_title,
                                            video_description,
                                            video_tag ,
                                            video_pub ,
                                            video_thumb ,
                                            video_vc ,
                                            video_like ,
                                            video_fav ,
                                            video_commcount ,
                                            video_dura ,
                                            video_cap )
                                                
                                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_id'],
                row['video_id'],
                row['video_title'],
                row['video_description'],
                row['video_tag'],
                row['video_pub'],
                row['video_thumb'],
                row['video_vc'],
                row['video_like'],
                row['video_fav'],
                row['video_commcount'],
                row['video_dura'],
                row['video_cap'])
        
        mycursor.execute(videos_insert,values)
        mydb.commit()

def comment_table(single_name):
        mydb= mysql.connector.connect(
                host='localhost',
                user='root',
                password='',
                database='Youtube_data')

        mycursor=mydb.cursor(buffered=True)

        
        comment_query='''Create TABLE if not exists Comments (comment_id VARCHAR(100) PRIMARY KEY,
                                                                videoid  VARCHAR (200),
                                                                comments TEXT,
                                                                author VARCHAR (200),
                                                                published TIMESTAMP)'''
        mycursor.execute(comment_query)
        mydb.commit()

        single_comm=[]
        db=client['Youtube_data']
        coll1=db["channel_details"]
        for comment_data in coll1.find({'Channel_info.channel_name':single_name},{'_id':0}):
            single_comm.append(comment_data['Comment_info'])
        df_single_comm=pd.DataFrame(single_comm[0])


        for index,row in df_single_comm.iterrows():

                comments_insert='''INSERT into Comments(comment_id ,
                                                        videoid ,
                                                        comments,
                                                        author,
                                                        published)
                                                                        
                                                VALUES(%s,%s,%s,%s,%s)'''

                values=(row['comment_id'],
                        row['videoid'],
                        row['comments'],
                        row['author'],
                        row['published'])
                mycursor.execute(comments_insert,values)
                mydb.commit()

def all_tables(unique_channel):
    information=channel_table(unique_channel)
    if information:
        return information
    else:
        video_table(unique_channel)
        comment_table(unique_channel)
    
    return "Tables created and inserted"

def display_channel():    
    channel_list=[]
    db=client['Youtube_data']
    coll1=db["channel_details"]
    for channel_data in coll1.find({},{'_id':0,'Channel_info':1}):
        channel_list.append(channel_data['Channel_info'])
    df=st.dataframe(channel_list)
    return df

def display_video():
    videos_list=[]
    db=client['Youtube_data']
    coll1=db["channel_details"]
    for video_data in coll1.find({},{'_id':0,'Video_info':1}):
        for i in range(len(video_data['Video_info'])):
                videos_list.append(video_data['Video_info'][i])
    df2=st.dataframe(videos_list)

    return df2

def display_comments():
    comment_list=[]
    db=client['Youtube_data']
    coll1=db["channel_details"]
    for comm_data in coll1.find({},{'_id':0,'Comment_info':1}):
            for i in range(len(comm_data['Comment_info'])):
                    comment_list.append(comm_data['Comment_info'][i])
    df3=st.dataframe(comment_list)
    return df3


st.set_page_config(page_title='Youtube data', page_icon=':butterfly:',layout='wide',initial_sidebar_state='auto',menu_items=None)
st.image(r'https://getwallpapers.com/wallpaper/full/7/2/c/1415963-large-buddha-quotes-wallpaper-1920x1200-for-desktop.jpg',width=700,caption=None,output_format='auto',channels='RGB')
information='''This is an web application,where it gets youtube channel 
            id as input from user and fetches Channel,video,comment details and store the fetched data in :red[MongoDB] and 
            the data is inserted into Mysql and the data is displayed in the Streamlit application.
            This web app answers certain type of questions listed in it.:blue[Use wisely].:pink[THANK YOU]
            '''
def info():
    for i in information.split(" "):
            yield i + " "
            time.sleep(0.02) 

with st.sidebar:
    st.title(':green[YOUTUBE DATA HARVESTING ADN WAREHOUSING]')
    st.caption(':yellow[You can also listen to the track to work peacefully]')
    st.audio(r"C:\Users\sankara subramanian\Downloads\Pudhu Vellai Mazhai (Instrumental).mp3",format='audio/mpeg',loop=False,start_time=0)
    st.header(':blue[Streamlit usage video]')
    st.video(r'https://s3-us-west-2.amazonaws.com/assets.streamlit.io/videos/hero-video.mp4',start_time=0,loop=False,format="video/mp4")
   
    if st.button('Know more'):
        st.write_stream(info())
        

channel_id=st.text_input('Enter Channel ID here!')
if st.button('Collect and store datas'):
    ch_ids=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'Channel_info':1}):
        ch_ids.append(ch_data['Channel_info']['channel_id'])
    if channel_id in ch_ids:
        st.success('Duplicate channel id')
    else:
        insert=channel_details(channel_id)
        st.success(insert)

channel_list=[]
db=client['Youtube_data']
coll1=db["channel_details"]
for channel_data in coll1.find({},{'_id':0,'Channel_info':1}):
    channel_list.append(channel_data['Channel_info']['channel_name'])
quick_channel=st.selectbox("Select channel",channel_list)

if st.button('Insert to MySQL'):
    Table=all_tables(quick_channel)
    st.success(Table)

show_table=st.radio('SELECT TO VIEW TABLES',('Channel','Videos','Comments'))
if show_table=='Channel':
    st.snow()
    display_channel()
   
elif show_table == 'Videos':
    st.snow()
    display_video()
elif show_table =='Comments':
    display_comments()
    
mydb= mysql.connector.connect(
                host='localhost',
                user='root',
                password='',
                database='Youtube_data')

mycursor=mydb.cursor(buffered=True)

questions =st.selectbox("Select any questions given below:",
['Click the questions of your query',
'1.What are the names of all the videos and their corresponding channels?',
'2.Which channels have the most number of videos, and how many videos do they have?',
'3.What are the top 10 most viewed videos and their respective channels?',
'4.How many comments were made on each video, and what are their corresponding video names?',
'5.Which videos have the highest number of likes, and what are their corresponding channel names?',
'6.What is the total number of likes  for each video, and what are their corresponding video names?',
'7.What is the total number of views for each channel, and what are their corresponding channel names?',
'8.What are the names of all the channels that have published videos in the year 2022?',
'9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
'10.Which videos have the highest number of comments, and what are their corresponding channel names?'
])

if questions =='1.What are the names of all the videos and their corresponding channels?':
    mycursor.execute('''SELECT videos.video_title AS video_name, channels.channel_name AS channel_name
    FROM videos
    left JOIN channels ON videos.channel_id = channels.channel_id''')
    mydb.commit()
    result=mycursor.fetchall()
    q1=pd.DataFrame(result,columns=['Video_names','Channel_name'])
    st.write(q1)
    

elif questions=='2.Which channels have the most number of videos, and how many videos do they have?':
    mycursor.execute('''Select channel_name as Channel_name,max(channel_vid) as Max_videos from channels group by channel_name ''')
    mydb.commit()
    result=mycursor.fetchall()
    q2=pd.DataFrame(result,columns=[ 'Channel_name','Max_videos'])
    st.write(q2)
    plot=st.bar_chart(q2.set_index('Channel_name'),color='#09ab3b',width=2)
    


elif questions=='3.What are the top 10 most viewed videos and their respective channels?':
    mycursor.execute('''Select channels.channel_name as Channel_name, videos.video_title as Video_name, videos.video_vc as Views  
                 from videos join channels on channels.channel_id=videos.channel_id order by Views Desc limit 10 ''')
    result=mycursor.fetchall()
    mydb.commit()
    q3=pd.DataFrame(result,columns=[ 'Channel_name','Video_name','Views'],index=range(1,11))
    st.write(q3)
    st.bar_chart(q3.set_index('Video_name'),color=['#ff2b2b','#09ab3b'],width=2)


elif questions=='4.How many comments were made on each video, and what are their corresponding video names?':
    mycursor.execute('''select video_title as Video_name, video_commcount as Comment_count from videos 
                    group by Video_name''')
    mydb.commit()
    result=mycursor.fetchall()
    q4=pd.DataFrame(result,columns=['Video_name','Comment_count'])
    st.write(q4)

elif questions=='5.Which videos have the highest number of likes, and what are their corresponding channel names?':
    mycursor.execute('''Select channel_name as Channel_name,video_title as Video_name, max(video_like) as Highest_like 
                FROM videos left join channels on channels.channel_id=videos.channel_id Join (select channel_id,max(video_like) as Highest_like 
                FROM videos
                GROUP BY channel_id)as Sub on videos.channel_id=sub.channel_id and videos.video_like=sub.Highest_like group by channel_name''')
    mydb.commit()
    results=mycursor.fetchall() 
    q5=pd.DataFrame(results,columns=['Channel_name','Video_name','Highest_like'])
    st.write(q5)
    fig=px.bar(q5,x='Channel_name',y='Highest_like',color='Channel_name',title='Highest like for a channel')
    st.plotly_chart(fig,use_container_width=True)
    
    

elif questions=='6.What is the total number of likes  for each video, and what are their corresponding video names?':
    mycursor.execute('''Select video_title as Video_name, video_like as No_of_likes from videos group by video_name''')
    mydb.commit()
    results=mycursor.fetchall()
    q6=pd.DataFrame(results,columns=['Video_name','No_of_likes'])
    st.write(q6)


elif questions=='7.What is the total number of views for each channel, and what are their corresponding channel names?':
    mycursor.execute('''SELECT channel_name as Channel_name, channel_view as Channel_views from channels ''')
    mydb.commit()
    results=mycursor.fetchall()
    q7=pd.DataFrame(results,columns=['Channel_name','Channel_views'])
    st.write(q7) 
    plot=px.bar(q7,x='Channel_name',y='Channel_views',color='Channel_name',title='Channel_views')
    st.plotly_chart(plot,use_container_width=True)
  

elif questions=='8.What are the names of all the channels that have published videos in the year 2022?':
    mycursor.execute('''Select distinct channel_name as Channel_name,video_title as video_name,video_pub as Published_on
                     FROM channels join videos on 
                    channels.channel_id=videos.channel_id where year(video_pub)=2022''')
    mydb.commit()
    results=mycursor.fetchall()
    q8=pd.DataFrame(results,columns=['Channel_name','Video_name','Published_on'])
    st.write(q8)

elif questions=='9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    mycursor.execute('''Select channel_name as Channel_name,sec_to_time (AVG(TIME_to_sec(video_dura))) as Duration
                    from videos join channels on channels.channel_id=videos.channel_id group by channel_name ''')
    mydb.commit()
    results=mycursor.fetchall()
    q9=pd.DataFrame(results,columns=['Channel_names','AVG_Duration'])
    st.write(q9)
    plot=px.bar(q9,x='Channel_names',y='AVG_Duration',color='Channel_names',title='Average_duration of videos in a channel')
    st.plotly_chart(plot,use_container_width=True   )
    

elif questions=='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
    mycursor.execute('''SELECT channel_name as Channel_name, video_title as Video_name,max(video_commcount) as Max_Comments
                     from channels join videos on channels.channel_id=videos.channel_id group by channel_name''')
    mydb.commit()
    results=mycursor.fetchall()
    q10=pd.DataFrame(results,columns=['Channel_name','Video_name','MAX_Comments'])
    st.write(q10)
    plot=px.bar(q10,x='Channel_name',y='MAX_Comments',color='Channel_name',title='Videos with highest comment count for a channel')
    st.plotly_chart(plot,use_container_width=True)


                                                        




   
                
                    

