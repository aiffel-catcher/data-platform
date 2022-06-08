from kafka import KafkaConsumer
import json

BROKERS = ['34.82.7.168:9092']


class MessageConsumer:
  brokers = ''
  topic = ''
  consumer = None

  def __init__(self, topic, group_id='group-1'):
    self.brokers = BROKERS
    self.topic = topic
    self.consumer = KafkaConsumer(
      self.topic,
      bootstrap_servers=BROKERS,
      auto_offset_reset='earliest',
      group_id=group_id,
      enable_auto_commit=True,
      value_deserializer=lambda x: json.loads(x.decode('utf-8')),
      consumer_timeout_ms=1000
    )

  def getConsumer(self):
    return self.consumer

  def close(self):
    self.consumer.close()