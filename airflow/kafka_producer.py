from kafka  import KafkaProducer
import logging
from time import sleep
from json import dumps


def encode_to_json(data):
	return dumps(data.tolist())


def produce_to_kafka(topic_name, value):
	# json_comb = encode_to_json(value)
	producer = KafkaProducer(bootstrap_servers=['34.64.188.187:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))
	logging.info('Partitions: ', producer.partitions_for(topic_name))
	producer.send(topic_name, value=value)

	logging.info("Sent number: {}".format(value))
	sleep(1)

	producer.close()



produce_to_kafka('TutorialTopic', 'test')