# coding: utf-8
import sys
import json
import traceback

sys.path.insert(0, '../common')

from kafka_consumer import MessageConsumer
from logger import Logging
from sentiment_analysis import analysis_sentiment
from operator_factory import insert_data_to_BigQuery
from string_utils import make_hash_id
from kafka_producer import MessageProducer

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


def process_pipeline(data, messageProducer):
  try:
    logger.info('[Pipeline] 감성분석 모델 파이프라인')
    # 감성분석 
    comment = data['modified_text']
    result = analysis_sentiment([comment])
    for key, value in result[0].items():
      data[key] = value
    logger.info('[Pipeline] 감성분석 >>>> ' + json.dumps(data))

    # 데이터 변환
    review_sentiment = get_review_sentiment(data)
    logger.info('[Pipeline] 감성분석 데이터 변환>>>> ' + json.dumps(review_sentiment))

    # 데이터 삽입
    insert_data_to_BigQuery('review_sentiment', [review_sentiment])
    logger.info('[Pipeline] BigQuery 데이터 저장 완료')

    # 카프카 메세지 publish
    publish_topic = 'streaming.socarreview.sentiment.0'
    logger.info('Kafka 결과 publish >>>> ' + publish_topic)
    messageProducer.send_msg(publish_topic, {**review_sentiment, **data})
    logger.info('Kafka 결과 publish 완료')
  except Exception:
    logger.error(traceback.format_exc())


def run():
  subscribe_topic = 'streaming.socarreview.binaryclassification.0'
  messageConsumer = MessageConsumer(subscribe_topic)
  messageProducer = MessageProducer()

  logger.info('[Kafka] get consumer')
  consumer = messageConsumer.getConsumer()

  try:
    while True:
      message_batch = consumer.poll()

      for partition_batch in message_batch.values():
        for message in partition_batch:
          value = message.value
          logger.info('[Kafka] 데이터 Subscribe >>>> ' + json.dumps(value))
          process_pipeline(value, messageProducer)
          consumer.commit()
  except Exception:
    logger.error(traceback.format_exc())
  finally:
    consumer.close()
    messageProducer.close()


run()