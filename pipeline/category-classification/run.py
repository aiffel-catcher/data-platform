# coding: utf-8
import sys
import json
import traceback

sys.path.insert(0, '../common')

from string_utils import make_hash_id
from kafka_consumer import MessageConsumer
from logger import Logging
from category_classification import get_category_value
from operator_factory import insert_data_to_BigQuery, select_category_all

logger = Logging('category-classification').getLogger()


def get_review_category(data, category_map):
    categories = category_map.keys()
    result = []
    for category in categories:
        if data[category] == 0:
            continue

        category_id = category_map[category]
        review_id = data['review_id']
        result.append({
            'review_category_id': make_hash_id(review_id + category_id),
            'category_id': category_id,
            'review_id': review_id
        })
    return result


def get_category_map(category):
    category_map = {}

    for m in category:
        category_map[m['category_name']] = m['category_id']

    return category_map


def process_pipeline(data, category_map):
    print('~')
    try:
        logger.info('[Pipeline] 멀티라벨분류 모델 파이프라인')
        # 멀티라벨분류 모델
        comment = data['modified_text']
        result = get_category_value([comment])
        logger.info('[Pipeline] 멀티라벨분류 >>>> ' + json.dumps(result))
        for column_name, label in result[0].items():
            data[column_name] = label

        # 데이터 변환
        review_category = get_review_category(data, category_map)
        logger.info('[Pipeline] 리뷰-키워드 데이터 변환>>>> ' + json.dumps(review_category))

        # 데이터 삽입
        insert_data_to_BigQuery('review_category', review_category)
        logger.info('[Pipeline] BigQuery 데이터 저장 완료')
    except Exception:
        logger.error(traceback.format_exc())


def run():
    subscribe_topic = 'streaming.socarreview.sentiment.0'
    messageConsumer = MessageConsumer(subscribe_topic)
    logger.info('[Kafka] get consumer')
    consumer = messageConsumer.getConsumer()

    category_all = select_category_all()
    category_map = get_category_map(category_all)

    try:
        while True:
            message_batch = consumer.poll()

            for partition_batch in message_batch.values():
                for message in partition_batch:
                    value = message.value
                    logger.info('[Kafka] 데이터 Subscribe >>>> ' + json.dumps(value))
                    if value['sentiment'] == 'negative':
                        process_pipeline(value, category_map)
                    consumer.commit()
    except Exception:
        logger.error(traceback.format_exc())
    finally:
        consumer.close()


run()