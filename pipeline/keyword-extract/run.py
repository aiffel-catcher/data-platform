import os
import sys
import math

sys.path.append(os.path.abspath('../common'))

import pandas as pd
from common.string_utils import make_hash_id
from common.kafka_consumer import MessageConsumer
from common.bigquery_operator import BigQueryClient
from common.logger import Logging
from keyword_extract import KeywordExtractor

logger = Logging('keyword-extract').getLogger()
KAFKA_TOPIC = 'streaming.socarreview.binaryclassification.0'
KAFKA_GROUP_ID = ''


def insert_data_to_BigQuery(table_name, data):
  logger.info('[BigQuery] insert data >>>> ', table_name)
  bigquery_client = BigQueryClient(table_name)
  bigquery_client.insert_rows(data)


def get_keywords_metadata(df):
  keywords_map = {}
  keywords_series = df['keywords']

  # 키워드 : keyword_id
  for keywords in keywords_series:
    if keywords == 'nan' or keywords == None or keywords != keywords:
      continue

    for key in keywords.split(' '):
      if key in keywords_map:
        continue

      keywords_map[key] = make_hash_id(key)

  keywords_meta = []
  for key in keywords_map.keys():
    d = {'keyword_id': keywords_map[key], 'keyword': key}
    keywords_meta.append(d)

  return (keywords_map, keywords_meta)


def get_keywordsreview_data(df, keywords_map):
  review_keyword_data = []

  for (i, d) in df.iterrows():
    keywords = d['keywords']
    if keywords == 'nan' or keywords == None or keywords != keywords:
      continue

    review_id = d['review_id']
    keywords_list = keywords.split(' ')

    for keyword in keywords_list:
      keyword_id = keywords_map[keyword]

      if keyword_id == None or keyword_id == '':
        continue

      data = {'review_keyword_id': make_hash_id(review_id+keyword_id), 'keyword_id': keyword_id, 'review_id': review_id}
      review_keyword_data.append(data)

  return review_keyword_data


def process_pipeline(keywordExtractor, data):
  try:
    logger.info('[Pipeline] 키워드추출 모델 파이프라인')
    # 키워드 추출 
    data_df = pd.DataFrame(data)
    data_df['keywords'] = data_df['modified_text'].apply(keywordExtractor.extract_keywords)
    logger.info('[Pipeline] 키워드추출 >>>> ', data_df['keywords'])

    # 데이터 변환
    (keywords_map, keywords_meta) = get_keywords_metadata(data_df)
    keywords_review = get_keywordsreview_data(data_df, keywords_map)
    logger.info('[Pipeline] 키워드 메타데이터 추출 >>>> ', keywords_meta)
    logger.info('[Pipeline] 리뷰-키워드 데이터 맵핑 변환>>>> ', keywords_review)

    # 데이터 삽입
    insert_data_to_BigQuery('keyword', keywords_meta)
    insert_data_to_BigQuery('review_keyword', keywords_review)
    logger.info('[Pipeline] BigQuery 데이터 저장 완료')
  except Exception as ex:
    logger.error('[Pipeline] error >>>> ', ex)


def run():
  messageConsumer = MessageConsumer(KAFKA_TOPIC)
  logger.info('[Kafka] get consumer')
  consumer = messageConsumer.getConsumer()

  keywordExtractor = KeywordExtractor()
  try:
    while True:
      message_batch = consumer.poll()

      for partition_batch in message_batch.values():
        for message in partition_batch:
          value = message.value
          logger.info('[Kafka] 데이터 Subscribe >>>> ', value)
          process_pipeline(keywordExtractor, value)
          consumer.commit()
  except Exception as ex:
    logger.error('[Kafka] error >>>> ', ex)
  finally:
    consumer.close()

run()
