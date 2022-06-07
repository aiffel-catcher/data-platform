import os
import sys
import pandas as pd
import torch

sys.path.append(os.path.abspath('../common'))

from common.kafka_consumer import MessageConsumer
from common.bigquery_operator import BigQueryClient
from common.logger import Logging
from multilabel_classification import MultilabelModel, get_category_value

logger = Logging('binary-classification').getLogger()
KAFKA_TOPIC = 'offline.review.*.0'
KAFKA_GROUP_ID = ''


def insert_data_to_BigQuery(table_name, data):
  logger.info('[BigQuery] insert data')
  bigquery_client = BigQueryClient(table_name)
  bigquery_client.insert_rows(data)


# 2. model Input/Output
# 3. DB 테이블에 맞게 데이터 변환
# 4. insert_to_BigQuery
def process_pipeline(device, model, data, label_cols): # 전체적으로 수정 필요
  print('~')
  try:
    logger.info('[Pipeline] 이진분류 모델 파이프라인')
    comment = data['modified_text']
    result = get_category_value(device, model.getTok(), model, [comment], label_cols)
    logger.info('[Pipeline] 이진분류 >>>> ', result)
    for idx, label in enumerate(result[0]):
      data[label_cols[idx]] = label

    # # 데이터 변환
    # (keywords_map, keywords_meta) = get_keywords_metadata(data_df)
    # keywords_review = get_keywordsreview_data(data_df, keywords_map)
    # logger.info('[Pipeline] 키워드 메타데이터 추출 >>>> ', keywords_meta)
    # logger.info('[Pipeline] 리뷰-키워드 데이터 맵핑 변환>>>> ', keywords_review)

    # 데이터 삽입
    # insert_data_to_BigQuery('keyword', keywords_meta)
    # insert_data_to_BigQuery('review_keyword', keywords_review)
    # logger.info('[Pipeline] BigQuery 데이터 저장 완료')
  except Exception as ex:
    logger.error('[Pipeline] error >>>> ', ex)


def run():
  messageConsumer = MessageConsumer(KAFKA_TOPIC)
  logger.info('[Kafka] get consumer')
  consumer = messageConsumer.getConsumer()

  device = torch.device('cuda:0')
  label_cols = ['사고', '서비스', '앱', '요금', '상태', '정비', '차량']
  model = MultilabelModel(device, label_cols).getModel()

  try:
    while True:
      message_batch = consumer.poll()

      for partition_batch in message_batch.values():
        for message in partition_batch:
          value = message.value # 여기에 필요한 데이터가 들어오는 건가??
          logger.info('[Kafka] 데이터 Subscribe >>>> ', value)
          process_pipeline(device, model, value, label_cols)
          consumer.commit()
  except Exception as ex:
    logger.error('[Kafka] error >>>> ', ex)
  finally:
    consumer.close()

run()


# 1. Kafka consuming
# 2. model Input/Output
# 3. DB 테이블에 맞게 데이터 변환
# 4. insert_to_BigQuery