import os
import sys
import math

sys.path.append(os.path.abspath('../common'))

from common.string_utils import make_hash_id
from common.kafka_consumer import MessageConsumer
from common.logger import Logging
from keyword_extract import KeywordExtractor
from common.operator_factory import insert_data_to_BigQuery, publish_kafka, select_keyword_all

logger = Logging('keyword-extract').getLogger()


def get_keywords_metadata(extracted_keywords, keywords_from_db):
  keywords_map = {}
  keywords_meta = []

  for o in keywords_from_db:
    keywords_map[o['keyword']] = o['keyword_id']
  
  for keyword in extracted_keywords:
    if keyword in keywords_map:
      continue

    keyword_id = make_hash_id(keyword)
    keywords_map[keyword] = keyword_id
    keywords_meta.append({'keyword_id': keyword_id, 'keyword': keyword})

  return (keywords_map, keywords_meta)


def get_keywordsreview_data(data, keywords_map):
  review_keyword_data = []

  keywords = data['keywords']
  if len(keywords) == 0:
    return []

  review_id = data['review_id']

  for k in keywords:
    keyword_id = keywords_map[k]

    if keyword_id == None or keyword_id == '':
      continue 

    data = {
      'review_keyword_id': make_hash_id(review_id+keyword_id),
      'keyword_id': keyword_id,
      'review_id': review_id
    }
    review_keyword_data.append(data)

  return review_keyword_data


def process_pipeline(keywordExtractor, data):
  try:
    logger.info('키워드추출 모델 파이프라인')
    # 기존 모든 키워드 가져오기
    keywords_from_db = select_keyword_all()

    # 키워드 추출 
    extracted_keywords = keywordExtractor.extract_keywords(data['modified_text'])
    data['keywords'] = extracted_keywords
    logger.info('키워드추출 >>>> ', data)

    # 데이터 변환
    (keywords_map, keywords_meta) = get_keywords_metadata(extracted_keywords, keywords_from_db)
    keywords_review = get_keywordsreview_data(data, keywords_map)
    logger.info('키워드 메타데이터 추출 >>>> ', keywords_meta)
    logger.info('리뷰-키워드 데이터 맵핑 변환>>>> ', keywords_review)

    # 데이터 삽입
    insert_data_to_BigQuery('keyword', keywords_meta)
    insert_data_to_BigQuery('review_keyword', keywords_review)
    logger.info('BigQuery 데이터 저장 완료')

    # 카프카 메세지 publish
    publish_topic = 'streaming.socarreview.keywords.0'
    logger.info('Kafka 결과 publish >>>> ', publish_topic)
    publish_kafka(publish_topic, keywords_review)
    logger.info('Kafka 결과 publish 완료')
  except Exception as ex:
    logger.error('error >>>> ', ex)


def run():
  subscribe_topic = 'streaming.socarreview.binaryclassification.0'
  messageConsumer = MessageConsumer(subscribe_topic)
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