import sys

sys.path.insert(0, '../common')

from kafka_consumer import MessageConsumer
from logger import Logging
from binary_classification import BinaryModel, get_related_value
from string_utils import make_hash_id
from operator_factory import insert_data_to_BigQuery, publish_kafka

logger = Logging('binary-classification').getLogger()


def get_review(data, is_socar):
  review = {
    'channel': 'youtube',
    'review_id': make_hash_id(data['review_id']),
    'origin_text': data['text_original'],
    'modified_text': data['modified_text'],
    'is_socar': is_socar,
    'create_at': data['comment_published_at']
  }
  return review

 
def process_pipeline(model, data):
  print('~')
  try:
    logger.info('이진분류 모델 파이프라인')
    # 이진분류
    comment = data['modified_text']
    result = get_related_value(model, [comment], model.getTok()) # comment: 하나 이상의 문장
    is_socar = result[0]
    logger.info('이진분류 >>>> ', is_socar)

    # 데이터 변환
    review = get_review(data, is_socar)
    logger.info('리뷰 데이터 맵핑 변환>>>> ', review)

    # 데이터 삽입
    insert_data_to_BigQuery('review', [review])
    logger.info('BigQuery 데이터 저장 완료')

    # 카프카 메세지 publish
    publish_topic = 'streaming.socarreview.binaryclassification.0'
    logger.info('Kafka publish >>>> ', publish_topic)
    publish_kafka(publish_topic, review)
    logger.info('Kafka publish 완료')
  except Exception as ex:
    logger.error('error >>>> ', ex)


def run():
  subscribe_topic = 'offline.review.playstore.0'
  messageConsumer = MessageConsumer(subscribe_topic)
  logger.info('[Kafka] get consumer')
  consumer = messageConsumer.getConsumer()

  model = BinaryModel().getModel()

  try:
    while True:
      message_batch = consumer.poll()

      for partition_batch in message_batch.values():
        for message in partition_batch:
          value = message.value
          logger.info('[Kafka] 데이터 Subscribe >>>> ', value)
          process_pipeline(model, value)
          consumer.commit()
  except Exception as ex:
    logger.error('[Kafka] error >>>> ', ex)
  finally:
    consumer.close()


run()