from common.kafka_producer import MessageProducer
from common.bigquery_operator import BigQueryClient

bigquery_client = BigQueryClient()
message_producer = MessageProducer()

def select_keyword_rocket_data():
  return bigquery_client.select_keywords_for_rocket()


def insert_data_to_BigQuery(table_name, data):
  bigquery_client.insert_rows(table_name, data)


def publish_kafka(topic, data):
  resp = message_producer.send_msg(topic, data)
  print(resp)
  message_producer.close()  