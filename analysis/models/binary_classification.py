# binary classification

# input으로 하나 또는 여러 개의 문장이 들어옴
# 모델 학습 코드 필요 없음. 체크포인트를 불러오고, 해당 모델로 예측한 결과를 반환하는 과정만 필요.

import torch
from torch import nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import gluonnlp as nlp
import numpy as np
import pandas as pd
from kobert import get_tokenizer
from kobert import get_pytorch_kobert_model


# Dataset
class BERTDataset(Dataset):
    def __init__(self, dataset, sent_idx, label_idx, bert_tokenizer, max_len,
                 pad, pair):
        transform = nlp.data.BERTSentenceTransform(
            bert_tokenizer, max_seq_length=max_len, pad=pad, pair=pair)

        self.sentences = [transform([i[sent_idx]]) for i in dataset]
        self.labels = [np.int32(i[label_idx]) for i in dataset]

    def __getitem__(self, i):
        return (self.sentences[i] + (self.labels[i], ))

    def __len__(self):
        return (len(self.labels))


# Classifier
class BERTClassifier(nn.Module):
    def __init__(self,
                 bert,
                 hidden_size=768,
                 num_classes=2,
                 dr_rate=None,
                 params=None):
        super(BERTClassifier, self).__init__()
        self.bert = bert
        self.dr_rate = dr_rate

        self.classifier = nn.Linear(hidden_size, num_classes)
        if dr_rate:
            self.dropout = nn.Dropout(p=dr_rate)

    def gen_attention_mask(self, token_ids, valid_length):
        attention_mask = torch.zeros_like(token_ids)
        for i, v in enumerate(valid_length):
            attention_mask[i][:v] = 1
        return attention_mask.float()

    def forward(self, token_ids, valid_length, segment_ids):
        attention_mask = self.gen_attention_mask(token_ids, valid_length)

        _, pooler = self.bert(input_ids=token_ids, token_type_ids=segment_ids.long(),
                              attention_mask=attention_mask.float().to(token_ids.device))
        if self.dr_rate:
            out = self.dropout(pooler)
        else:
            out = pooler
        return self.classifier(out)


def get_checkpoint(device, path):
    # 체크포인트 불러오기
    model = BERTClassifier(bertmodel, dr_rate=0.5).to(device)
    checkpoint = torch.load(path)
    model.load_state_dict(checkpoint['model_state_dict'])
    return model


def getRelatedValue(model, comment, checkpoint_path, tok, max_len, batch_size, device):
    commnetslist = []  # 텍스트 데이터를 담을 리스트
    rel_list = []  # 관련 여부 값을 담을 리스트
    for c in comment:  # 모든 댓글
        commnetslist.append([c, 5])  # [댓글, 임의의 양의 정수값] 설정

    pdData = pd.DataFrame(commnetslist, columns=[['modified_text', 'related']])
    pdData = pdData.values
    test_set = BERTDataset(pdData, 0, 1, tok, max_len, True, False)
    test_input = torch.utils.data.DataLoader(test_set, batch_size=batch_size, num_workers=2)

    for batch_id, (token_ids, valid_length, segment_ids, label) in enumerate(test_input):
        token_ids = token_ids.long().to(device)
        segment_ids = segment_ids.long().to(device)
        valid_length = valid_length
        out = model(token_ids, valid_length, segment_ids)

        pred = torch.argmax(out, 1)
        pred = pred.detach().cpu().numpy()
        pred = list(pred)
        rel_list.extend(pred)  # 예측 결과 리스트

    return rel_list  # 텍스트 데이터가 쏘카와 관련되는지 여부를 담은 결과

CHECKPOINT_PATH = '' # binary classification 모델 체크포인트 경로
MAX_LEN = 64
BATCH_SIZE = 64
DEVICE = torch.device("cuda:0")

bertmodel, vocab = get_pytorch_kobert_model(cachedir=".cache")
tokenizer = get_tokenizer()
tok = nlp.data.BERTSPTokenizer(tokenizer, vocab, lower=False)
model = get_checkpoint(DEVICE, CHECKPOINT_PATH)

result = getRelatedValue(model, comment, CHECKPOINT_PATH, tok, MAX_LEN, BATCH_SIZE, DEVICE) # comment: 하나 이상의 문장