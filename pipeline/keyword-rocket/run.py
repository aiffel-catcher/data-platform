# coding: utf-8
import sys
import json
import traceback

sys.path.insert(0, '../common')

import pandas as pd
from datetime import datetime

from keyword_rocket import calc_keyword_rocket
from kafka_consumer import MessageConsumer
from logger import Logging
from operator_factory import select_keyword_rocket_data, insert_data_to_BigQuery
from string_utils import make_hash_id


logger = Logging('keyword-rocket').getLogger()


def get_keyword_map(data):
  keywords_map = {}

  for d in data:
    keywords_map[d['keyword']] = d['keyword_id']

  return keywords_map


def get_keyword_rocket(data, keywords_map):
  keyword_rocket = []

  for d in data:
    keyword = d[0]
    today = datetime.today().isoformat()
    hash_key = keyword + today + '7days'
    keyword_rocket_id = make_hash_id(hash_key)

    rocket = {
      'keyword_rocket_id': keyword_rocket_id, 
      'keyword_id': keywords_map[keyword], 
      'create_at': today, 
      'rocket_score': float(d[1]), 
      'aggregate_type': '7days'
    }
    keyword_rocket.append(rocket)

  return keyword_rocket
        

def process_pipeline():
  try:
    logger.info('키워드 급상승 모델 파이프라인')
    # 데이터 가져오기 : 현재 14일치 
    data_from_db = select_keyword_rocket_data() # 	[[keyword_id keyword create_at], ... ]
    logger.info('키워드 데이터 가져옴')

    # 키워드 급상승 
    calculated_data = calc_keyword_rocket(data_from_db, 7)
    logger.info('키워드 급상승 집계')

    # 데이터 변환
    keyword_map = get_keyword_map(data_from_db)
    keyword_rockets = get_keyword_rocket(calculated_data, keyword_map)
    logger.info('키워드 급상승 데이터 변환>>>> ' + json.dumps(keyword_rockets))

    # 데이터 삽입
    insert_data_to_BigQuery('keyword_rocket', keyword_rockets)
    logger.info('BigQuery 데이터 저장 완료')
  except Exception:
    logger.error(traceback.format_exc())


def run():
  subscribe_topic = 'streaming.socarreview.keywords.0'
  messageConsumer = MessageConsumer(subscribe_topic)
  logger.info('[Kafka] get consumer')
  consumer = messageConsumer.getConsumer()

  try:
    while True:
      message_batch = consumer.poll()

      for partition_batch in message_batch.values():
        for message in partition_batch:
          value = message.value
          logger.info('[Kafka] 데이터 Subscribe >>>> ' + json.dumps(value))
          process_pipeline()
          consumer.commit()
  except Exception:
    logger.error(traceback.format_exc())
  finally:
    consumer.close()

run()
