# !pip3 install ipywidgets  # for vscode
# !pip3 install git+https://git@github.com/SKTBrain/KoBERT.git@master

import torch
from torch import nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import gluonnlp as nlp
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score

from kobert import get_tokenizer
from kobert import get_pytorch_kobert_model

from transformers import AdamW
from transformers.optimization import get_cosine_schedule_with_warmup


## Setting parameters
max_len = 64
batch_size = 64
warmup_ratio = 0.1
num_epochs = 100
max_grad_norm = 1
log_interval = 200
learning_rate =  5e-5

device = torch.device("cuda:0")
bertmodel, vocab = get_pytorch_kobert_model(cachedir=".cache")
tokenizer = get_tokenizer()
tok = nlp.data.BERTSPTokenizer(tokenizer, vocab, lower=False)


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


# tsv 파일 경로
trainset_path = ''
valset_path = ''
testset_path = ''

dataset_train = nlp.data.TSVDataset(trainset_path, field_indices=[0,1], num_discard_samples=1)
dataset_val = nlp.data.TSVDataset(valset_path, field_indices=[0,1], num_discard_samples=1)
dataset_test = nlp.data.TSVDataset(testset_path, field_indices=[0,1], num_discard_samples=1)

data_train = BERTDataset(dataset_train, 0, 1, tok, max_len, True, False)
data_val = BERTDataset(dataset_val, 0, 1, tok, max_len, True, False)
data_test = BERTDataset(dataset_test, 0, 1, tok, max_len, True, False)

train_dataloader = torch.utils.data.DataLoader(data_train, batch_size=batch_size, num_workers=2, shuffle=True)
val_dataloader = torch.utils.data.DataLoader(data_val, batch_size=batch_size, num_workers=2, shuffle=False)
test_dataloader = torch.utils.data.DataLoader(data_test, batch_size=batch_size, num_workers=2, shuffle=False)


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


# EarlyStopping
class EarlyStopping:
    """주어진 patience 이후로 validation loss가 개선되지 않으면 학습을 조기 중지"""
    def __init__(self, patience=7, verbose=False, delta=0, path='checkpoint.pt'):
        """
        Args:
            patience (int): validation loss가 개선된 후 기다리는 기간
                            Default: 7
            verbose (bool): True일 경우 각 validation loss의 개선 사항 메세지 출력
                            Default: False
            delta (float): 개선되었다고 인정되는 monitered quantity의 최소 변화
                            Default: 0
            path (str): checkpoint저장 경로
                            Default: 'checkpoint.pt'
        """
        self.patience = patience
        self.verbose = verbose
        self.counter = 0
        self.best_score = None
        self.early_stop = False
        self.val_loss_min = np.Inf
        self.delta = delta
        self.path = path

    def __call__(self, epoch, optimizer, val_loss, model):

        score = -val_loss

        if self.best_score is None:
            self.best_score = score
            self.save_checkpoint(epoch, optimizer, val_loss, model)
        elif score < self.best_score + self.delta:
            self.counter += 1
            print(f'EarlyStopping counter: {self.counter} out of {self.patience}')
            if self.counter >= self.patience:
                self.early_stop = True
        else:
            self.best_score = score
            self.save_checkpoint(epoch, optimizer, val_loss, model)
            self.counter = 0

    def save_checkpoint(self, epoch, optimizer, val_loss, model):
        '''validation loss가 감소하면 모델을 저장한다.'''
        if self.verbose:
            print(f'Validation loss decreased ({self.val_loss_min:.6f} --> {val_loss:.6f}).  Saving model ...')
        torch.save({'epoch':epoch,
                    'model_state_dict': model.state_dict(),
                    'optimizer_state_dict': optimizer.state_dict(),
                    'loss': val_loss
                    }, self.path)
        self.val_loss_min = val_loss


# Prepare optimizer and schedule (linear warmup and decay)
no_decay = ['bias', 'LayerNorm.weight']

# no_decay에 해당하는 파라미터명을 가진 레이어들은 decay에서 배제하기 위해 weight_decay를 0으로 셋팅, 그 외에는 0.01로 decay
# weight decay란 l2 norm으로 파라미터 값을 정규화해주는 기법

# 최적화할 파라미터의 그룹
optimizer_grouped_parameters = [
    {'params': [p for n, p in model.named_parameters() if not any(nd in n for nd in no_decay)], 'weight_decay': 0.01},
    {'params': [p for n, p in model.named_parameters() if any(nd in n for nd in no_decay)], 'weight_decay': 0.0}
]

optimizer = AdamW(optimizer_grouped_parameters, lr=learning_rate)
loss_fn = nn.CrossEntropyLoss()

t_total = len(train_dataloader) * num_epochs
warmup_step = int(t_total * warmup_ratio)
scheduler = get_cosine_schedule_with_warmup(optimizer, num_warmup_steps=warmup_step, num_training_steps=t_total)

model = BERTClassifier(bertmodel, dr_rate=0.5).to(device)

checkpoint_path = '' # checkpoint 저장할 경로
early_stopping = EarlyStopping(patience=5, verbose=True, path=checkpoint_path)


# train model
for e in range(num_epochs):
    running_train_loss, running_train_acc, running_train_f1 = 0, 0, 0
    running_val_loss, running_val_acc, running_val_f1 = 0, 0, 0

    model.train()
    for token_ids, valid_length, segment_ids, label in train_dataloader:
        optimizer.zero_grad()
        token_ids = token_ids.long().to(device)
        segment_ids = segment_ids.long().to(device)
        valid_length = valid_length
        label = label.long().to(device)
        out = model(token_ids, valid_length, segment_ids)
        train_loss = loss_fn(out, label)
        train_loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_grad_norm)
        optimizer.step()
        scheduler.step()  # Update learning rate schedule

        train_pred = torch.argmax(out, 1)
        train_pred = train_pred.detach().cpu().numpy()
        train_real = label.detach().cpu().numpy()

        running_train_acc += accuracy_score(y_pred=train_pred, y_true=train_real)
        running_train_loss += train_loss.item()  # track the loss value

    train_loss_value = running_train_loss / len(train_dataloader)
    train_acc_value = running_train_acc / len(train_dataloader)

    model.eval()
    for token_ids, valid_length, segment_ids, label in val_dataloader:
        token_ids = token_ids.long().to(device)
        segment_ids = segment_ids.long().to(device)
        valid_length = valid_length
        label = label.long().to(device)
        out = model(token_ids, valid_length, segment_ids).to(device)
        loss = loss_fn(out, label)

        valid_pred = torch.argmax(out, 1)
        valid_pred = valid_pred.detach().cpu().numpy()
        valid_real = label.detach().cpu().numpy()

        running_val_acc += accuracy_score(y_pred=valid_pred, y_true=valid_real)
        running_val_f1 += f1_score(np.array(valid_pred), np.array(valid_real))
        running_val_loss += loss.item()

    val_loss_value = running_val_loss / len(val_dataloader)
    val_acc_value = running_val_acc / len(val_dataloader)
    val_f1_value = running_val_f1 / len(val_dataloader)

    print(
        "epoch {}/{} | train acc: {:.5f} | train loss: {:.5f} | validation acc: {:.5f} | validation f1: {:.5f} | validation loss: {:.5f}" \
        .format(e + 1, num_epochs, train_acc_value, train_loss_value, val_acc_value, val_f1_value, val_loss_value))

    # early_stopping는 validation loss가 감소하였는지 확인이 필요하며,
    # 만약 감소하였을경우 현재 모델을 checkpoint로 만든다.
    early_stopping(e, optimizer, val_loss_value, model)

    if early_stopping.early_stop:
        print("Early stopping")
        break


# 체크포인트 불러오기
model = BERTClassifier(bertmodel,  dr_rate=0.5).to(device)
PATH = '' # checkpoint 경로
checkpoint = torch.load(PATH)
epoch = checkpoint['epoch']
model.load_state_dict(checkpoint['model_state_dict'])


# test model
running_test_acc, running_test_f1 = 0.0, 0.0
model.eval()
for token_ids, valid_length, segment_ids, label in test_dataloader:
    token_ids = token_ids.long().to(device)
    segment_ids = segment_ids.long().to(device)
    valid_length = valid_length
    label = label.long().to(device)
    out = model(token_ids, valid_length, segment_ids)

    test_pred = torch.argmax(out, 1)
    test_pred = test_pred.detach().cpu().numpy()
    test_real = label.detach().cpu().numpy()

    running_test_acc += accuracy_score(y_pred=test_pred, y_true=test_real)
    running_test_f1 += f1_score(y_pred=test_pred, y_true=test_real)

test_acc_value = running_test_acc / len(test_dataloader)
test_f1_value = running_test_f1 / len(test_dataloader)
print(f"test accuracy: {test_acc_value} | validation f1: {test_f1_value}")