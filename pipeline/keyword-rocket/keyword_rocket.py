import pandas as pd
from collections import Counter
from datetime import datetime


def create_df(data_df):
    data_df['date'] = data_df['create_at'].apply(
        lambda c: datetime.fromisoformat(c).strftime('%Y-%m-%d')
    )

    data_df = data_df.groupby('date')['keyword'].apply(lambda x: ' '.join(x))
    data_df = data_df.reset_index().sort_index()
    data_df['counter'] = data_df.keyword.str.split()
    data_df['counter'] = data_df.counter.apply(Counter)  # 각 단어들의 빈도수가 딕셔너리 형태로 만들어짐
    data_df = data_df.sort_values(by='date', ascending=True)

    return data_df


def sum_counter(counters):
    total = Counter([])
    for c in counters:
        total += c
    return total


def calc_keyword_rocket(data, days, max_count=None):
    data_df = create_df(data)
    print(data_df)

    LEN = len(data_df) - days # days를 제외한 나머지

    target = data_df.iloc[-1 * days:]['counter']
    target = sum_counter(target)

    remain = data_df.iloc[:-1 * days]['counter']
    remain = sum_counter(remain)

    rate = {}
    for keyword, value in target.items():
        try:
            standard = remain[keyword] / LEN * days
            rate[keyword] = (value - standard) / standard
        except:
            rate[keyword] = 0

    rate = sorted(rate.items(), key=lambda x: x[1], reverse=True)

    if max_count == None:
        return rate
    else:
        return rate[:max_count]


# data = [
#     {'keyword_id': '76c2df5653433c069fa4364d801c3b85', 'keyword': '보상', 'create_at': '2022-05-01T03:34:27'},
#     {'keyword_id': '76c2df5653433c069fa4364d801c3b85', 'keyword': '보상', 'create_at': '2022-05-01T03:34:27'},
#     {'keyword_id': 'dda39dd9ee4fb28c12ec360ac7431072', 'keyword': '카드', 'create_at': '2022-05-01T03:34:27'},
#     {'keyword_id': '76c2df5653433c069fa4364d801c3b85', 'keyword': '보상', 'create_at': '2022-05-02T03:34:27'},
#     {'keyword_id': 'dda39dd9ee4fb28c12ec360ac7431072', 'keyword': '카드', 'create_at': '2022-05-02T03:34:27'},
# ]
# data_df = pd.DataFrame.from_dict(data)
# calc_keyword_rocket(data_df, 1)