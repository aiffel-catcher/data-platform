import pandas as pd
import numpy as np
import torch
from konlpy.tag import Mecab
from transformers import BertModel
from kobert_tokenizer import KoBERTTokenizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity


model = BertModel.from_pretrained('skt/kobert-base-v1')
tokenizer = KoBERTTokenizer.from_pretrained('skt/kobert-base-v1')


class KeywordExtractor:
  mecab = None
  texts = []
  top_n = 5
  ngram_range = (2,3)
  stop_words = []


  def __init__(self, texts):
    self.mecab = Mecab()
    self.texts = texts
    self.setStopwords()


  def setStopwords(self):
    path = './stopwords.txt'
    stop_words = []

    stop_file = open(path, 'r', encoding='utf-8')
    for line in stop_file.readlines():
      stop_words.append(line.rstrip())
    stop_file.close()

    self.stop_words = stop_words 


  def runKoBERTmodel(self, texts):
    inputs = tokenizer.batch_encode_plus(texts)
    out = model(input_ids = torch.tensor(inputs['input_ids']),
              attention_mask = torch.tensor(inputs['attention_mask']))
    
    return out.pooler_output


  def extractNouns(self, sentence) :
    tokenized = self.mecab.nouns(sentence)
    return tokenized


  def extractKeyphrases(self, text):
    try:
      count = CountVectorizer(ngram_range=self.ngram_range, stop_words=self.stop_words).fit([text])
      candidates = count.get_feature_names_out()

      doc_embedding = self.runKoBERTmodel([text]).detach().numpy()
      candidate_embeddings = np.concatenate([self.runKoBERTmodel([c]).detach().numpy() for c in candidates])

      distances = cosine_similarity(doc_embedding, candidate_embeddings)
      keyphrases = [
          (candidates[index], round(float(distances[0][index]), 4))
          for index in distances.argsort()[0][-self.top_n:]
      ][::-1]

      return keyphrases
    except ValueError as err:
      print('Error >>>>> ', err)
      return []


  def extract_keywords(self):
    keywords_list = []

    for text in self.texts:
      try:
        keyphrases = self.extractKeyphrases(text)
        keyphrases_str = ' '.join([k[0] for k in keyphrases])

        keywords = self.extractNouns(keyphrases_str)
        keywords = list(set(keywords))

        # 형태소 분석 후 생긴 불용어 필터링
        for w in keywords:
          if w in self.stop_words:
            keywords.remove(w)
        
        if len(keywords) == 0:
          keywords_list.append('')
        else:
          keywords_list.append(keywords)
      except ValueError as err:
        print(err)
        keywords_list.append('')

    return keywords_list


# data = ['아 왜 안될까 나는 제대로 하고있는걸까 자동차 고장 났다.']
# keywordExtractor = KeywordExtractor(data)
# keywords = keywordExtractor.extract_keywords()
# print(keywords)


# pip install 'git+https://github.com/SKTBrain/KoBERT.git#egg=kobert_tokenizer&subdirectory=kobert_hf'
# pip install sklearn
# pip install pandas
# pip install numpy
# pip install torch
# pip install transformers
# pip install konlpy
# pip install sentencepiece
# sudo apt-get install curl git (optional)
# bash <(curl -s https://raw.githubusercontent.com/konlpy/konlpy/master/scripts/mecab.sh)
