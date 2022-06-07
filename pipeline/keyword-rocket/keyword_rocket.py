import pandas as pd
from collections import Counter


def create_df(input):
    df = pd.DataFrame(input, columns=['date', 'keywords'])
    df = df.dropna(subset=['keywords'])  # keyword가 하나도 존재하지 않으면 행 삭제
    df['date'] = df['date'].str.replace('T', ' ')
    df['date'] = df['date'].str.split().str[0]

    df = df.groupby('date')['keywords'].apply(lambda x: x.sum())
    df = df.reset_index().sort_index()
    df['counter'] = df.keywords.str.split()  # 공백을 기준으로 쪼개짐
    df['counter'] = df.counter.apply(Counter)  # 각 단어들의 빈도수가 딕셔너리 형태로 만들어짐

    return df


def sum_counter(counters):
    total = Counter([])
    for c in counters:
        total += c
    return total


def func(max_count=None, days, input):
    df = create_df(input)

    LEN = len(df) - days # days를 제외한 나머지

    target = df.iloc[-1 * days:]['counter']
    target = sum_counter(target)

    remain = df.iloc[:-1 * days]['counter']
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


result = func(max_count, days, input) # input: ['date', 'keyword1 keyword2 ...']