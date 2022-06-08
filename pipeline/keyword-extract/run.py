# coding: utf-8
import sys
import json
import traceback

sys.path.insert(0, '../common')

from string_utils import make_hash_id
from kafka_consumer import MessageConsumer
from logger import Logging
from keyword_extract import KeywordExtractor
from operator_factory import insert_data_to_BigQuery, select_keyword_all
from kafka_producer import MessageProducer

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


def process_pipeline(keywordExtractor, data, messageProducer):
  try:
    logger.info('키워드추출 모델 파이프라인')
    # 기존 모든 키워드 가져오기
    keywords_from_db = select_keyword_all()

    # 키워드 추출 
    extracted_keywords = keywordExtractor.extract_keywords(data['modified_text'])
    data['keywords'] = extracted_keywords
    logger.info('키워드추출 >>>> ' + json.dumps(extracted_keywords))

    # 데이터 변환
    (keywords_map, keywords_meta) = get_keywords_metadata(extracted_keywords, keywords_from_db)
    keywords_review = get_keywordsreview_data(data, keywords_map)
    logger.info('키워드 메타데이터 추출 >>>> ' + json.dumps(keywords_meta))
    logger.info('리뷰-키워드 데이터 맵핑 변환>>>> ' + json.dumps(keywords_review))

    # 데이터 삽입
    if len(keywords_meta) != 0:
      insert_data_to_BigQuery('keyword', keywords_meta)

    insert_data_to_BigQuery('review_keyword', keywords_review)
    logger.info('BigQuery 데이터 저장 완료')

    # 카프카 메세지 publish
    publish_topic = 'streaming.socarreview.keywords.0'
    logger.info('Kafka 결과 publish >>>> ' + publish_topic)
    messageProducer.send_msg(publish_topic, keywords_review)
    logger.info('Kafka 결과 publish 완료')
  except Exception:
    logger.error(traceback.format_exc())


def run():
  subscribe_topic = 'streaming.socarreview.binaryclassification.0'
  messageConsumer = MessageConsumer(subscribe_topic, 'group-2')
  messageProducer = MessageProducer()

  logger.info('[Kafka] get consumer')
  consumer = messageConsumer.getConsumer()

  keywordExtractor = KeywordExtractor()
  try:
    while True:
      message_batch = consumer.poll()

      for partition_batch in message_batch.values():
        for message in partition_batch:
          value = message.value
          logger.info('[Kafka] 데이터 Subscribe >>>> ' + json.dumps(value))
          process_pipeline(keywordExtractor, value, messageProducer)
          consumer.commit()
  except Exception:
    logger.error(traceback.format_exc())
  finally:
    consumer.close()
    messageProducer.close()


run()