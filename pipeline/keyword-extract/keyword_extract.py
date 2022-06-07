import pandas as pd
import numpy as np
import torch
from konlpy.tag import Mecab
from transformers import BertModel
from kobert_tokenizer import KoBERTTokenizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity




class KeywordExtractor:
  model = None
  tokenizer = None
  mecab = None
  top_n = 5
  ngram_range = (2,3)
  stop_words = []


  def __init__(self):
    self.model = BertModel.from_pretrained('skt/kobert-base-v1')
    self.tokenizer = KoBERTTokenizer.from_pretrained('skt/kobert-base-v1')
    self.mecab = Mecab()
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
    inputs = self.tokenizer.batch_encode_plus(texts)
    out = self.model(input_ids = torch.tensor(inputs['input_ids']),
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


  def extract_keyword(self, text):
    try:
      keyphrases = self.extractKeyphrases(text)
      keyphrases_str = ' '.join([k[0] for k in keyphrases])

      keywords = self.extractNouns(keyphrases_str)
      keywords = list(set(keywords))

      # 형태소 분석 후 생긴 불용어 필터링
      for w in keywords:
        if w in self.stop_words:
          keywords.remove(w)
      
      if keywords == None or len(keywords) == 0:
        return []
      else:
        return keywords
    except ValueError as err:
      print(err)
      return []


# pip install 'git+https://github.com/SKTBrain/KoBERT.git#egg=kobert_tokenizer&subdirectory=kobert_hf'
# pip install sklearn
# pip install pandas
# pip install numpy
# pip install transformers
# pip install konlpy
# pip install sentencepiece
# pip install torch -f https://download.pytorch.org/whl/torch_stable.html
# sudo apt-get install curl git (optional)
# bash <(curl -s https://raw.githubusercontent.com/konlpy/konlpy/master/scripts/mecab.sh)
# pip install mxnet
# pip install gluonnlp