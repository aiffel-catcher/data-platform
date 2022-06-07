import os
import sys

sys.path.append(os.path.abspath('../common'))

from common.kafka_consumer import MessageConsumer
from common.logger import Logging
from sentiment_analysis import analysis_sentiment
from common.operator_factory import insert_data_to_BigQuery, publish_kafka
from common.string_utils import make_hash_id

logger = Logging('sentiment-analysis').getLogger()


def get_review_sentiment(data):
  hash_key = data['review_id']+data['sentiment']
  return {
    'review_sentiment_id': make_hash_id(hash_key),
    'review_id': data['review_id'],
    'sentiment': data['sentiment'],
    'positive': float(data['positive']),
    'negative': float(data['negative']),
    'neutral': float(data['neutral'])
  }


def process_pipeline(data):
  try:
    logger.info('[Pipeline] 감성분석 모델 파이프라인')
    # 감성분석 
    comment = data['modified_text']
    result = analysis_sentiment(comment)
    for key, value in result[0].items():
      data[key] = value
    logger.info('[Pipeline] 감성분석 >>>> ', data)

    # 데이터 변환
    review_sentiment = get_review_sentiment(data)
    logger.info('[Pipeline] 감성분석 데이터 변환>>>> ', review_sentiment)

    # 데이터 삽입
    insert_data_to_BigQuery('review_sentiment', [review_sentiment])
    logger.info('[Pipeline] BigQuery 데이터 저장 완료')

    # 카프카 메세지 publish
    publish_topic = 'streaming.socarreview.keywords.0'
    logger.info('Kafka 결과 publish >>>> ', publish_topic)
    publish_kafka(publish_topic, review_sentiment)
    logger.info('Kafka 결과 publish 완료')
  except Exception as ex:
    logger.error('[Pipeline] error >>>> ', ex)


def run():
  subscribe_topic = 'streaming.socarreview.binaryclassification.0'
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
          process_pipeline(value)
          consumer.commit()
  except Exception as ex:
    logger.error('[Kafka] error >>>> ', ex)
  finally:
    consumer.close()


run()