import requests
import json


def get_response(content):
    client_id = ""
    client_secret = ""
    url = "https://naveropenapi.apigw.ntruss.com/sentiment-analysis/v1/analyze"
    headers = {
        "X-NCP-APIGW-API-KEY-ID": client_id,
        "X-NCP-APIGW-API-KEY": client_secret,
        "Content-Type": "application/json"
    }
    data = {"content": content}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    rescode = response.status_code
    if(rescode == 200):
        return response
    else:
        print("Error : " + response.text)


def analysis_sentiment(contents):
    result = []
    for content in contents:
        response = get_response(content)
        response = eval(response.text)
        sentiment_value = {
           'sentiment': response['document']['sentiment'], \
           'positive': response['document']['confidence']['positive'], \
           'negative': response['document']['confidence']['negative'], \
           'neutral': response['document']['confidence']['neutral']
        }
        result.append(sentiment_value)
    return result




# result = analysis_sentiment(comments) # result: [[text, sentiment, positive값, negative값, neutral값], ...]