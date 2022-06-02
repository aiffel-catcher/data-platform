import re
import numpy as np
import pendulum
from random import randrange


def preprocess_sentence(sentence):
    sentence = sentence.lower() # 영어는 모두 소문자로 변환
    sentence = re.sub(r'[\s]+', " ", sentence)# 하나 이상의 공백(개행 포함)은 하나의 공백으로 변환
    sentence = re.sub(r'[.,!?]{2,}', ".", sentence) # 문장부호가 여러 개이면 .으로 변환
    sentence = re.sub(r'[0-9]+:[0-9]+', "", sentence) # 타임라인(ex 3:40) 제거
    sentence = re.sub(r"[^0-9a-z가-힣?.,!]+", " ", sentence)# 숫자, 영문자, 한글, 문장부호를 제외한 것은 공백으로 변환 (모음, 자음도 제거)
    sentence = sentence.strip()
    # sentence = '<start> ' + sentence + ' <end>' # 앞 뒤로 토큰 추가

    # 한글이 한 글자도 없다면 문장 그대로 넣지 않고 빈 문자열을 넣음
    if bool(re.search(r'[가-힣]+', sentence)):
        pass
    else:
        sentence = np.nan
        
    return sentence


def make_review_id(dateString):
    randomNum = randrange(1000, 10000)
    datetime = pendulum.parse(dateString, tz="Asia/Seoul")
    review_id = str(datetime.int_timestamp) + '_' + str(randomNum)
    return review_id