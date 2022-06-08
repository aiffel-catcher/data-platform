from kafka import KafkaProducer
import json

BROKERS = ['34.82.7.168:9092']

class MessageProducer:
    brokers = ""
    producer = None

    def __init__(self):
        self.brokers = BROKERS
        self.producer = KafkaProducer(
            bootstrap_servers=self.brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries = 3
        )


    def send_msg(self, topic, msg):
        print("sending message")
        future = self.producer.send(self.topic, msg)
        self.producer.flush()
        future.get(timeout=60)
        print("message sent successfully")
        return {'status_code':200, 'error':None}


    def close(self):
      self.producer.close()


# topic = 'test'
# message_producer = MessageProducer(topic)

# data = {'name':'abc', 'email':'abc@example.com'}
# resp = message_producer.send_msg(data)
# print(resp)

# message_producer.close()