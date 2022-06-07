import requests
import json


def get_response(content):
    data = {
    "content": content
    }
    response = requests.post(url, data=json.dumps(data), headers=headers)
    rescode = response.status_code
    if(rescode == 200):
        return response
    else:
        print("Error : " + response.text)


def get_result(contents):
    result = []
    for content in contents:
        response = get_response(content)
        response = eval(response.text)
        a = [response['document']['sentiment'], \
           response['document']['confidence']['positive'], \
           response['document']['confidence']['negative'], \
           response['document']['confidence']['neutral']]
        result.append([content, *a])
    return result


client_id = ""
client_secret = ""
url="https://naveropenapi.apigw.ntruss.com/sentiment-analysis/v1/analyze"
headers = {
    "": client_id,
    "": client_secret,
    "Content-Type": "application/json"
}

result = get_result(comments) # result: [[text, sentiment, positive값, negative값, neutral값], ...]