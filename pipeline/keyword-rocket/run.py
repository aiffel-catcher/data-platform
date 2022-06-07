import os
import sys

sys.path.append(os.path.abspath('../common'))

import pandas as pd
from datetime import datetime
from common.kafka_consumer import MessageConsumer
from common.logger import Logging
from common.operator_factory import select_keyword_rocket_data, insert_data_to_BigQuery
from keyword_rocket import calc_keyword_rocket
from common.string_utils import make_hash_id


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
    data_from_db = select_keyword_rocket_data() # 	[[keyword_id keyword create_at], ... ]
    logger.info('키워드 데이터 가져오기 >>>> ', data_from_db)

    # 키워드 급상승 
    data_df = pd.DataFrame(input, columns=['keyword_id', 'keyword', 'create_at'])
    calculated_data = calc_keyword_rocket(data_df, 7)
    logger.info('키워드 급상승  >>>> ', calculated_data)

    # 데이터 변환
    keyword_map = get_keyword_map(data_from_db)
    keyword_rocket = get_keyword_rocket(calculated_data, keyword_map)
    logger.info('키워드 급상승  데이터 변환>>>> ', keyword_rocket)

    # 데이터 삽입
    insert_data_to_BigQuery('keyword_rocket', keyword_rocket)
    logger.info('BigQuery 데이터 저장 완료')
  except Exception as ex:
    logger.error('error >>>> ', ex)


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
          logger.info('[Kafka] 데이터 Subscribe >>>> ', value)
          process_pipeline()
          consumer.commit()
  except Exception as ex:
    logger.error('[Kafka] error >>>> ', ex)
  finally:
    consumer.close()

run()
