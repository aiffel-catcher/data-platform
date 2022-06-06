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
class Data_for_BERT(Dataset):
    def __init__(self, dataset, max_len, pad, pair, label_cols):
        transform = nlp.data.BERTSentenceTransform(tok, max_seq_length=max_len, pad=pad, pair=pair)
        self.sentences = [transform([txt]) for txt in dataset.modified_text]
        self.labels = dataset[label_cols].values

    def __getitem__(self, i):
        return (self.sentences[i] + (self.labels[i],))

    def __len__(self):
        return (len(self.labels))


# Classifier
class BERTClassifier(nn.Module):
    def __init__(self, hidden_size=768, num_classes=1, dr_rate=None, params=None):
        super(BERTClassifier, self).__init__()
        self.bert = bertmodel
        self.dr_rate = dr_rate

        self.classifier = nn.Linear(hidden_size, num_classes)

        if dr_rate:
            self.dropout = nn.Dropout(p=dr_rate)

    def generate_attention_mask(self, token_ids, valid_length):
        attention_mask = torch.zeros_like(token_ids)

        for i, v in enumerate(valid_length):
            attention_mask[i][:v] = 1
        return attention_mask.float()

    def forward(self, token_ids, valid_length, segment_ids):
        attention_mask = self.generate_attention_mask(token_ids, valid_length)

        _, pooler = self.bert(input_ids=token_ids, token_type_ids=segment_ids.long(),
                              attention_mask=attention_mask.float().to(token_ids.device))
        if self.dr_rate:
            out = self.dropout(pooler)
        else:
            out = pooler
        return self.classifier(out)


def get_checkpoint(label_cols, device, path):
    num_classes = len(label_cols)
    model = model = BERTClassifier(num_classes=num_classes).to(device)
    checkpoint = torch.load(path)
    model.load_state_dict(checkpoint['model_state_dict'])
    return model


def getCategoryLabel(model, comment, tok, max_len, batch_size, device, label_cols, threshold):
    comments_list = []
    for c in comment:
        comments_list.append([c, 5, 5, 5, 5, 5, 5, 5])

    pdData = pd.DataFrame(comments_list, columns=['modified_text', *label_cols])
    data_inference = Data_for_BERT(pdData, MAX_LEN, True, False, label_cols=label_cols)
    inference_loader = DataLoader(data_inference, batch_size=BATCH_SIZE,
                                  num_workers=4, shuffle=False, pin_memory=True)

    category_pred = []
    for batch_id, (token_ids, valid_length, segment_ids, inference_label) in enumerate(inference_loader):
        token_ids = token_ids.long().to(device)
        segment_ids = segment_ids.long().to(device)
        valid_length = valid_length

        inference_out = model(token_ids, valid_length, segment_ids)

        inference_pred = torch.sigmoid(inference_out).detach().cpu().numpy()
        inference_pred = np.array(inference_pred > threshold, dtype=float)
        category_pred.append(inference_pred)

    category_pred = np.concatenate(category_pred)

    result = []
    for idx in range(len(category_pred)):
        result.append([comment[idx], *category_pred[idx]])

    return result


CHECKPOINT_PATH = '' # multi-label classification 모델 체크포인트 경로
MAX_LEN = 64
BATCH_SIZE = 64
THRESHOLD = 0.5
DEVICE = torch.device("cuda:0")
label_cols = ['사고', '서비스', '앱', '요금', '상태', '정비', '차량']

bertmodel, vocab = get_pytorch_kobert_model(cachedir=".cache")
tokenizer = get_tokenizer()
tok = nlp.data.BERTSPTokenizer(tokenizer, vocab, lower=False)
model = get_checkpoint(label_cols, DEVICE, CHECKPOINT_PATH)

result = getCategoryLabel(model, comment, tok, MAX_LEN, BATCH_SIZE, DEVICE, label_cols, THRESHOLD)