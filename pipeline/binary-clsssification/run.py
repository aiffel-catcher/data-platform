import os
import sys

sys.path.append(os.path.abspath('../common'))

from common.kafka_consumer import MessageConsumer
from common.bigquery_operator import BigQueryClient
from common.logger import Logging

logger = Logging('binary-classification').getLogger()
KAFKA_TOPIC = 'offline.review.*.0'
KAFKA_GROUP_ID = ''


def insertDataToBigQuery(data):
  logger.info('[BigQuery] insert data')
  table_name = 'review'
  bigquery_client = BigQueryClient(table_name)
  bigquery_client.insert_rows(data)


def run():
  messageConsumer = MessageConsumer(KAFKA_TOPIC)
  logger.info('[Kafka] get consumer')
  consumer = messageConsumer.getConsumer()
  try:
    while True:
      message_batch = consumer.poll()

      for partition_batch in message_batch.values():
        for message in partition_batch:
          logger.info(message.value)
          consumer.commit()
  except Exception as ex:
    logger.error('error >>>> ', ex)
  finally:
    consumer.close()

run()
