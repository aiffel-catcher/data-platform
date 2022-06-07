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
    def __init__(self, dataset, tok, max_len, pad, pair, label_cols):
        transform = nlp.data.BERTSentenceTransform(tok, max_seq_length=max_len, pad=pad, pair=pair)
        self.sentences = [transform([txt]) for txt in dataset.modified_text]
        self.labels = dataset[label_cols].values

    def __getitem__(self, i):
        return (self.sentences[i] + (self.labels[i],))

    def __len__(self):
        return (len(self.labels))


# Classifier
class BERTClassifier(nn.Module):
    def __init__(self, bertmodel, hidden_size=768, num_classes=1, dr_rate=None, params=None):
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


class MultilabelModel():
    CHECKPOINT_PATH = './checkpoint.pt'  # multi-label classification 모델 체크포인트 경로
    bertmodel = None
    vocab = None
    tokenizer = None
    tok = None
    model = None

    def __init__(self, device, label_cols):
        self.device = device
        self.num_classes = len(label_cols)
        self.bertmodel, self.vocab = get_pytorch_kobert_model(cachedir="~/.cache")
        self.tokenizer = get_tokenizer()
        self.tok = nlp.data.BERTSPTokenizer(self.tokenizer, self.vocab, lower=False)
        self.loadModel(self.device, self.CHECKPOINT_PATH)


    def loadModel(self, device, path):
        model = BERTClassifier(self.bertmodel, num_classes=self.num_classes).to(device)
        checkpoint = torch.load(path)
        model.load_state_dict(checkpoint['model_state_dict'])
        self.model = model

    def getModel(self):
        return self.model

    def getTok(self):
        return self.tok


def get_category_value(device, tok, model, comment, label_cols):
    max_len= 64
    batch_size = 64
    threshold = 0.5

    comments_list = []
    for c in comment:
        comments_list.append([c, 5, 5, 5, 5, 5, 5, 5])

    pdData = pd.DataFrame(comments_list, columns=['modified_text', *label_cols])
    data_inference = Data_for_BERT(pdData, tok, max_len, True, False, label_cols=label_cols)
    inference_loader = DataLoader(data_inference, batch_size=batch_size,
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
        result.append(category_pred[idx])

    return result