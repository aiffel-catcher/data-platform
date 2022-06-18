# coding: utf-8
import sys
import json
import traceback

sys.path.insert(0, '../common')

import torch
from kafka_consumer import MessageConsumer
from logger import Logging
from binary_classification import BinaryModelCheckpoint, get_related_value
from string_utils import make_hash_id
from operator_factory import insert_data_to_BigQuery
from kafka_producer import MessageProducer


logger = Logging('binary-classification').getLogger()


def get_review(data, is_socar):
  review = {
    'channel': 'playstore',
    'review_id': make_hash_id(data['review_id']),
    'origin_text': data['content'],
    'modified_text': data['modified_text'],
    'is_socar': int(is_socar),
    'create_at': data['at'].replace('T', ' ').replace('Z', '')
  }
  return review


def process_pipeline(binaryModelCheckpoint, data, messageProducer, device):
  try:
    logger.info('이진분류 모델 파이프라인')

    # 이진분류
    comment = data['modified_text']
    model = binaryModelCheckpoint.getModel()
    tok = binaryModelCheckpoint.getTok()
    result = get_related_value(model, [comment], tok, device) # comment: 하나 이상의 문장
    is_socar = result[0]
    logger.info('이진분류 >>>> ' + str(is_socar))

    # 데이터 변환
    review = get_review(data, is_socar)
    logger.info('리뷰 데이터 맵핑 변환>>>> ' + json.dumps(review))

    # 데이터 삽입
    insert_data_to_BigQuery('review', [review])
    logger.info('BigQuery 데이터 저장 완료')

    # 카프카 메세지 publish
    publish_topic = 'streaming.socarreview.binaryclassification.0'
    logger.info('Kafka publish >>>> ' + publish_topic)
    messageProducer.send_msg(publish_topic, review)
    logger.info('Kafka publish 완료')
  except Exception:
    logger.error(traceback.format_exc())


def run():
  subscribe_topic = 'offline.review.playstore.0'
  messageConsumer = MessageConsumer(subscribe_topic)
  messageProducer = MessageProducer()

  logger.info('[Kafka] get consumer')
  consumer = messageConsumer.getConsumer()

  device = torch.device('cuda:0')
  binaryModelCheckpoint = BinaryModelCheckpoint(device)

  try:
    while True:
      message_batch = consumer.poll()

      for partition_batch in message_batch.values():
        for message in partition_batch:
          value = message.value
          logger.info('[Kafka] 데이터 Subscribe >>>> ' + json.dumps(value))
          process_pipeline(binaryModelCheckpoint, value, messageProducer, device)
          consumer.commit()
  except Exception:
    logger.error(traceback.format_exc())
  finally:
    consumer.close()
    messageProducer.close()


run()