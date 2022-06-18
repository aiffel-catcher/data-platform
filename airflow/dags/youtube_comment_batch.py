import logging
from pyyoutube import Api
import pendulum
import pandas as pd
import os

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException

import re
import numpy as np
from random import randrange
from kafka import KafkaProducer
import json


DAGBAGS_DIR = Variable.get('DAGBAGS_DIR')
VIDEO_COUNT = 500
LAST_UPDATED_AT_PATH = f'{DAGBAGS_DIR}/youtube_last_updated_at'
TEMPORARY_DATA_PATH = f'{DAGBAGS_DIR}/data'
GCS_BUCKET_NAME = 'aiffel-catcher2'

log = logging.getLogger(__name__)
api_key = 'AIzaSyCjxJ1WvvXrNUBb2UidLsyincyvK2Bllws'
api = Api(api_key=api_key)


def task_to_fail(error):
    raise AirflowException("Error message >>>> ", error)


def task_to_skip():
    raise AirflowSkipException


def get_execution_date(kwargs):
    return kwargs['execution_date'].in_timezone('Asia/Seoul')


def get_last_updated_at(path):
    with open(path) as f:
        last_updated_at= f.read()
        print('get_last_updated_at >>>> ', last_updated_at)
        return last_updated_at


def set_last_updated_at(datetime_str, path):
    dt = pendulum.parse(datetime_str, tz="Asia/Seoul")
    f = open(path, "w")
    print('set_last_updated_at >>>> ', dt.to_iso8601_string())
    f.write(dt.to_iso8601_string())
    f.close()

class MessageProducer:
    brokers = ""
    topic = ""
    producer = None

    def __init__(self, topic):
        self.brokers = ['34.82.7.168:9092']
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries = 3
        )


    def send_msg(self, msg):
        print("sending message")
        future = self.producer.send(self.topic, msg)
        self.producer.flush()
        future.get(timeout=60)
        print("message sent successfully")
        return {'status_code':200, 'error':None}
    
    def close(self):
      self.producer.close()


def preprocess_sentence(sentence):
    sentence = sentence.lower() # 영어는 모두 소문자로 변환
    sentence = re.sub(r'[\s]+', " ", sentence)# 하나 이상의 공백(개행 포함)은 하나의 공백으로 변환
    sentence = re.sub(r'[.,!?]{2,}', ".", sentence) # 문장부호가 여러 개이면 .으로 변환
    sentence = re.sub(r'[0-9]+:[0-9]+', "", sentence) # 타임라인(ex 3:40) 제거
    sentence = re.sub(r"[^0-9a-z가-힣?.,!]+", " ", sentence)# 숫자, 영문자, 한글, 문장부호를 제외한 것은 공백으로 변환 (모음, 자음도 제거)
    sentence = sentence.strip()
    # sentence = '<start> ' + sentence + ' <end>' # 앞 뒤로 토큰 추가

    # 한글이 한 글자도 없다면 문장 그대로 넣지 않고 빈 문자열을 넣음
    if bool(re.search(r'[가-힣]+', sentence)):
        pass
    else:
        sentence = np.nan
        
    return sentence


def make_review_id(dateString):
    randomNum = randrange(1000, 10000)
    datetime = pendulum.parse(dateString, tz="Asia/Seoul")
    review_id = str(datetime.int_timestamp) + '_' + str(randomNum)
    return review_id


def get_datetime_for_video(datetime_str):
    dt = pendulum.parse(datetime_str, tz="Asia/Seoul")
    dt = dt.subtract(months=1)
    print('get_datetime_for_video >>>>', dt.to_iso8601_string())
    return dt.to_iso8601_string()


def get_videos(published_after):
    q_keyword = '쏘카'
    try:
        r = api.search(
            q=q_keyword, 
            parts=["snippet"],
            search_type=["video"], 
            published_after=published_after, 
            count=VIDEO_COUNT, 
            order='date')

        videos = []
        for item in r.items:
            id = item.id
            snippet = item.snippet
            
            videos.append({
                'videoId': id.videoId,
                'videoTitle': snippet.title,
                'videoDescription': snippet.description,
                'videoPublishedAt': snippet.publishedAt
            })

        print('get_videos 데이터 갯수>>>> ', len(videos))
        return videos
    except Exception as ex:
        task_to_fail(ex)


def get_comments(videos):
    comments = []

    for v in videos:
        try:
            ct_by_video = api.get_comment_threads(video_id=v['videoId'], count=None)
            commentThread = ct_by_video.items

            for c in commentThread:
                snippet = c.snippet
                cmt = snippet.topLevelComment.snippet
                comment = v.copy()
                comment['video_title'] = v['videoTitle']
                comment['author_display_name'] = cmt.authorDisplayName
                comment['text_original'] = cmt.textOriginal
                comment['like_count'] = cmt.likeCount
                comment['comment_published_at'] = cmt.publishedAt
                comments.append(comment)
        except Exception as ex:
            print('comment에서 발생된 에러는 skip 합니다 >>> ', ex)


    print('get_comments 데이터 갯수 >>>> ', len(comments))
    return comments


def get_comments_of_socar_by_date(comments, startdate_str):
    result = []
    startDt = pendulum.parse(startdate_str)

    for c in comments:
        publishedAt = c['comment_published_at']
        publishedDt = pendulum.parse(publishedAt, tz="Asia/Seoul")

        if publishedDt > startDt:
            result.append(c)

    print('get_comments_of_socar_by_date 데이터 갯수 >>>> ', len(result))
    return result


with DAG(
    dag_id='youtube_comment_collector',
    schedule_interval='10 */12 * * *', # 12시간마다 
    start_date=pendulum.datetime(2022, 5, 23, tz="Asia/Seoul"),
    catchup=False,
    tags=['catcher', 'youtube'],
) as dag:

    # 1. 영상 데이터 수집
    # ㄴ ex) last_updated_at - 1months
    # ㄴ 영상이 영향을 미치는 기간을 기준으로 설정함
    # 2. 댓글 데이터 수집
    # ㄴ last_updated_at 이후 데이터만 가져옴집
    @task(task_id="get_youtube_comment")
    def extract(**kwargs):
        try:
            last_updated_at = get_last_updated_at(LAST_UPDATED_AT_PATH)
            if last_updated_at == None or last_updated_at == '':
                last_updated_at = get_execution_date(kwargs).to_iso8601_string()
            published_after = get_datetime_for_video(last_updated_at)

            videos = get_videos(published_after)
            comments = get_comments(videos)
            source_data = get_comments_of_socar_by_date(comments, last_updated_at)

            return source_data
        except Exception as ex:
            task_to_fail(ex)



    @task(task_id="preprocess_data")
    def transform(source_data):
        if (source_data == None or len(source_data) == 0):
            task_to_skip()

        youtube = pd.DataFrame(source_data)
        try:    
            youtube['review_id'] = youtube['comment_published_at'].apply(make_review_id)
            youtube['modified_text'] = youtube['text_original'].apply(preprocess_sentence)
            youtube = youtube.dropna(subset=['modified_text'])  # 빈 문자열(한글이 하나도 없는 경우) 처리
            youtube = youtube.sort_values(by=['comment_published_at'], ascending=True)

            print('preprocess_data 데이터 형태 >>>> ', youtube.shape)
        except Exception as ex:
            task_to_fail(ex) 

        return youtube.to_dict('records')


    @task(task_id="send_to_kafka")
    def send_to_kafka(data):
        if (data == None or len(data) == 0):
            task_to_skip()

        topic_name = 'offline.youtube.comment.0'
        try:
            message_producer = MessageProducer(topic_name)
            for d in data:
                message_producer.send_msg(d)
        except Exception as ex:
            task_to_fail(ex) 
        finally:
            message_producer.close()

        return True


    @task(task_id="record_last_updated_at")
    def record_last_updated_at(**kwargs):
        last_updated_at = get_execution_date(kwargs).to_iso8601_string()
        set_last_updated_at(last_updated_at, LAST_UPDATED_AT_PATH)


    @task(task_id="prepare_temporary_file", multiple_outputs=True)
    def prepare_temporary_file(data, **kwargs):
        if (data == None or len(data) == 0):
            task_to_skip()

        df = pd.DataFrame(data)
        execution_date = get_execution_date(kwargs)
        filename = f'youtube-{execution_date.int_timestamp}.csv'
        src_filepath = f'{TEMPORARY_DATA_PATH}/{filename}'
        try:
            df.to_csv(src_filepath)
            print('store_source_data temporary_data>>>> ', src_filepath)
            return {
                'filename': filename,
                'src_filepath': src_filepath,
                'dst_filepath': f'{execution_date.year}/{execution_date.month}/{execution_date.day}/{filename}'
            }
        except Exception as ex:
            task_to_fail(ex)


    @task(task_id="delete_file")
    def delete_file(src_filepath):
        print('delete_file >>>> ', src_filepath)
        os.remove(src_filepath)


    # [START main_flow]
    source_data = extract()
    preprocess_data = transform(source_data)

    # - store source data to GCS
    file_info = prepare_temporary_file(preprocess_data)
    upload_file = LocalFilesystemToGCSOperator(
        task_id='upload_file_to_GCS',
        src=file_info['src_filepath'],
        dst=file_info['dst_filepath'],
        bucket='catcher-bucket',
    )
    delete_temporary_file = delete_file(file_info['src_filepath'])

    send_to_kafka_task = send_to_kafka(preprocess_data)
    record_task = record_last_updated_at()
    # [END main_flow]


    source_data >> preprocess_data >> file_info >> upload_file >> delete_temporary_file
    source_data >> preprocess_data >> send_to_kafka_task >> record_task