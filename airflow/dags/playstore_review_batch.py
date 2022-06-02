import logging
import pendulum
import pandas as pd
import os
from google_play_scraper import Sort, reviews

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from utils.kafka_operator import MessageProducer
from utils.string_util import preprocess_sentence, make_review_id
from utils.time_util import get_execution_date, get_last_updated_at, set_last_updated_at
from utils.task_util import task_to_fail, task_to_skip


DAGBAGS_DIR = Variable.get('DAGBAGS_DIR')
LAST_UPDATED_AT_PATH = f'{DAGBAGS_DIR}/playstore_last_updated_at'
TEMPORARY_DATA_PATH = f'{DAGBAGS_DIR}/data'
GCS_BUCKET_NAME = 'catcher-bucket'


log = logging.getLogger(__name__)


def get_reviews():
    reviews_google, _ = reviews(
            'socar.Socar',
            lang='ko',           
            country='us',
            sort=Sort.NEWEST,
            count=1000
        )
    print('get_reviews 데이터 갯수 >>>> ', len(reviews_google))
    return reviews_google


def get_review_by_date(reviews_google, startdate_str):
    result = []
    startDt = pendulum.parse(startdate_str)

    for r in reviews_google:
        publishedAt = r['at']
        publishedDt = pendulum.instance(publishedAt, tz="Asia/Seoul")

        if publishedDt > startDt:
            result.append({
                # 'playstore_review_id': r['reviewId'],
                'user_name': r['userName'],
                'at': r['at'].isoformat(),
                'score': r['score'],
                'content': r['content']
            })

    print('get_review_by_date 데이터 갯수 >>>> ', len(result))
    return result



with DAG(
    dag_id='playstore_review_collector',
    schedule_interval='*/5 * * * *', # */5 * * * *
    start_date=pendulum.datetime(2022, 5, 23, tz="Asia/Seoul"),
    catchup=False,
    tags=['catcher', 'playstore'],
) as dag:
    # last_updated_at 이후 데이터만 가져옴
    @task(task_id="get_playstore_review")
    def extract(**kwargs):
        try:
            last_updated_at = get_last_updated_at(LAST_UPDATED_AT_PATH)
            if last_updated_at == None or last_updated_at == '':
                last_updated_at = get_execution_date(kwargs).to_iso8601_string()

            reviews_google = get_reviews()
            source_data = get_review_by_date(reviews_google, last_updated_at)

            return source_data
        except Exception as ex:
            task_to_fail(ex)        


    @task(task_id="preprocess_data")
    def transform(source_data):
        if (source_data == None or len(source_data) == 0):
            task_to_skip()

        playstore = pd.DataFrame(source_data)
        try:  
            playstore['review_id'] = playstore['at'].apply(make_review_id)
            playstore['modified_text'] = playstore['content'].apply(preprocess_sentence)
            playstore = playstore.dropna(subset=['modified_text']) # 빈 문자열(한글이 하나도 없는 경우) 처리
            playstore = playstore.sort_values(by=['at'], ascending=True)

            print('preprocess_data 데이터 형태 >>>> ', playstore.shape)
        except Exception as ex:
            task_to_fail(ex) 

        return playstore.to_dict('records')


    @task(task_id="send_to_kafka")
    def send_to_kafka(data):
        if (data == None or len(data) == 0):
            task_to_skip()

        topic_name = 'offline.playstore.review.0'
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
        filename = f'playstore-{execution_date.int_timestamp}.csv'
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

    # store source data to GCS
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