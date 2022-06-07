import os
import sys
import pandas as pd

sys.path.append(os.path.abspath('../common'))

from common.kafka_consumer import MessageConsumer
from common.logger import Logging
from common.operator_factory import select_keyword_rocket_data
from keyword_rocket import calc_keyword_rocket
from common.string_utils import make_hash_id
from datetime import datetime

logger = Logging('keyword-rocket').getLogger()


def get_keywordrocket(rocket_rates):
  print()
  today = datetime.today().isoformat()
  hash_key = word+today+'7days'
  keyword_rocket_id = make_hash_id(hash_key)

  keyword_rocket = {
    'keyword_rocket_id': make_hash_id, 
    'keyword_id': keyword_id, 
    'create_at': today, 
    'rocket_score': float(rocket['soaring']), 
    'aggregate_type': '7days'
  }
        


def process_pipeline(model):
  try:
    logger.info('키워드 급상승 모델 파이프라인')
    data = select_keyword_rocket_data() # 	[[keyword_id keyword create_at], ... ]
    logger.info('키워드 데이터 가져오기 >>>> ', result)

    # 키워드 급상승 
    data_df = pd.DataFrame(input, columns=['keyword_id', 'create_at', 'keyword'])
    result = calc_keyword_rocket(model, [comment], model.getTok()) # comment: 하나 이상의 문장
    logger.info('키워드 급상승  >>>> ', result)

    # 데이터 변환
    # review = get_review(data, result[0])
    # logger.info('키워드 급상승  데이터 맵핑 변환>>>> ', review)

    # 데이터 삽입
    # insert_data_to_BigQuery('review', review)
    # logger.info('BigQuery 데이터 저장 완료')
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
