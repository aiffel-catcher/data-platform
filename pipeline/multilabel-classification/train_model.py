# !pip3 install ipywidgets  # for vscode
# !pip3 install git+https://git@github.com/SKTBrain/KoBERT.git@master

import torch
from torch import nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import gluonnlp as nlp
import numpy as np
from tqdm.notebook import tqdm
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from transformers import get_linear_schedule_with_warmup

from kobert import get_tokenizer
from kobert import get_pytorch_kobert_model

from transformers import AdamW
from transformers.optimization import get_cosine_schedule_with_warmup

import warnings
warnings.filterwarnings(action='ignore')


## Setting parameters
MAX_LEN = 70
BATCH_SIZE = 64
EPOCHS = 100
LEARNING_RATE = 5e-8
DROPOUT_RATE = 0.45
MAX_GRAD_NORM = 1
WARMUP_RATIO = 0.1

label_cols = ['사고', '서비스', '앱', '요금', '상태', '정비', '차량'] # 카테고리 컬럼
num_classes = len(label_cols) # 카테고리 수

device = torch.device("cuda:0")
bertmodel, vocab = get_pytorch_kobert_model(cachedir=".cache")
tokenizer = get_tokenizer()
tok = nlp.data.BERTSPTokenizer(tokenizer, vocab, lower=False)


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


df = pd.read_csv('') # 사용할 데이터
train_set, test_set = train_test_split(df, test_size=0.2, random_state=42)
train_set, valid_set = train_test_split(train_set, test_size=0.25, random_state=42)

data_train = Data_for_BERT(train_set, MAX_LEN, True, False, label_cols=label_cols)
data_valid = Data_for_BERT(valid_set, MAX_LEN, True, False, label_cols=label_cols)
data_test = Data_for_BERT(test_set, MAX_LEN, True, False, label_cols=label_cols)

train_loader = DataLoader(data_train, batch_size=BATCH_SIZE,
                          num_workers=4, shuffle=True, pin_memory=True)
valid_loader = DataLoader(data_valid, batch_size=BATCH_SIZE,
                          num_workers=4, shuffle=False, pin_memory=True)
test_loader = DataLoader(data_test, batch_size=BATCH_SIZE,
                          num_workers=4, shuffle=False, pin_memory=True)


# Classifier
class BERTClassifier(nn.Module):
    def __init__(self, hidden_size=768, num_classes=1, dr_rate=None, params=None):
        super(BERTClassifier, self).__init__()
        self.bert = bertmodel
        self.dr_rate = dr_rate

        # fully connected layer
        self.classifier = nn.Linear(hidden_size, num_classes) # output layer 의 사이즈를 num_classes 로 설정

        # dr_rate 가 정의되어 있을 경우, 넣어준 비율에 맞게 weight 를 drop-out 시켜줌
        if dr_rate:
            self.dropout = nn.Dropout(p=dr_rate)

    def generate_attention_mask(self, token_ids, valid_length):
        # token_id를 인풋으로 받아, 버트 모델에 사용할 attention_mask 를 만듦
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
    {"params": [p for n, p in model.named_parameters() if not any(nd in n for nd in no_decay)], 'weight_decay' : 0.01},
    {'params': [p for n, p in model.named_parameters() if any(nd in n for nd in no_decay)], 'weight_decay': 0.0}
]

optimizer = AdamW(optimizer_grouped_parameters, lr=LEARNING_RATE)
loss_fn = nn.BCEWithLogitsLoss()

t_total = len(train_loader) * EPOCHS
warmup_step = int(t_total * WARMUP_RATIO)
scheduler = get_linear_schedule_with_warmup(optimizer, num_warmup_steps=warmup_step, num_training_steps=t_total)


# train model
def train_model(model, batch_size, n_epochs, threshold, patience, path):

    train_losses = [] # to track the training loss as the model trains
    valid_losses = [] # to track the validation loss as the model trains
    avg_train_losses = [] # to track the average training loss per epoch as the model trains
    avg_valid_losses = [] # to track the average validation loss per epoch as the model trains

    early_stopping = EarlyStopping(patience=patience, verbose=True, delta=0.01, path=path)

    for epoch in range(1, n_epochs + 1):

        # initialize the early_stopping object
        model.train()
        train_epoch_pred = []
        train_loss_record = []

        for batch_id, (token_ids, valid_length, segment_ids, label) in enumerate(train_loader):
            optimizer.zero_grad()

            token_ids = token_ids.long().to(device)
            segment_ids = segment_ids.long().to(device)
            valid_length = valid_length

            label = label.float().to(device)
            out = model(token_ids, valid_length, segment_ids)

            loss = loss_fn(out, label)
            train_loss_record.append(loss)
            train_losses.append(loss.item())

            train_real = label.detach().cpu().numpy()

            train_pred = torch.sigmoid(out).detach().cpu().numpy()  # sigmoid 결과값
            train_pred = np.array(train_pred > threshold, dtype=float)  # 0.5를 기준으로 하여 1 또는 0으로 변환한 값
            train_pred = train_pred.flatten()
            train_epoch_pred.append(train_pred)

            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), MAX_GRAD_NORM)
            optimizer.step()
            scheduler.step()  # Update learning rate schedule

        train_epoch_pred = np.concatenate(train_epoch_pred)
        train_epoch_target = train_loader.dataset.labels
        train_epoch_target = np.concatenate(train_epoch_target)
        train_epoch_result = accuracy_score(y_true=train_epoch_target, y_pred=train_epoch_pred)

        valid_epoch_pred = []
        valid_loss_record = []

        model.eval()
        with torch.no_grad():
            for batch_id, (token_ids, valid_length, segment_ids, valid_label) in enumerate(valid_loader):
                token_ids = token_ids.long().to(device)
                segment_ids = segment_ids.long().to(device)
                valid_length = valid_length

                valid_label = valid_label.float().to(device)
                valid_out = model(token_ids, valid_length, segment_ids)

                loss = loss_fn(valid_out, valid_label)
                valid_loss_record.append(loss)
                valid_losses.append(loss.item())

                valid_real = valid_label.detach().cpu().numpy()

                valid_pred = torch.sigmoid(valid_out).detach().cpu().numpy()
                valid_pred = np.array(valid_pred > threshold, dtype=float)
                valid_pred = valid_pred.flatten()
                valid_epoch_pred.append(valid_pred)

        valid_epoch_pred = np.concatenate(valid_epoch_pred)
        valid_epoch_target = valid_loader.dataset.labels
        valid_epoch_target = np.concatenate(valid_epoch_target)

        valid_epoch_accuracy = accuracy_score(y_true=valid_epoch_target, y_pred=valid_epoch_pred)
        valid_epoch_precision = precision_score(y_true=valid_epoch_target, y_pred=valid_epoch_pred)
        valid_epoch_recall = recall_score(y_true=valid_epoch_target, y_pred=valid_epoch_pred)
        valid_epoch_f1 = f1_score(y_true=valid_epoch_target, y_pred=valid_epoch_pred)

        train_loss = np.average(train_losses)
        valid_loss = np.average(valid_losses)
        avg_train_losses.append(train_loss)
        avg_valid_losses.append(valid_loss)

        # clear lists to track next epoch
        train_losses = []
        valid_losses = []

        print(
            "epoch {}/{} | train acc: {:.5f} | train loss: {:.5f} | validation acc: {:.5f} | validation precision: {:.5f} | validation recall: {:.5f} | validation f1: {:.5f} | validation loss: {:.5f}" \
            .format(epoch, EPOCHS, train_epoch_result, train_loss, valid_epoch_accuracy, valid_epoch_precision,
                    valid_epoch_recall, valid_epoch_f1, valid_loss))

        early_stopping(epoch, optimizer, valid_loss, model)
        if early_stopping.early_stop:
            print("Early stopping")
            break


PATIENCE = 5
CHECKPOINT_PATH = '' # checkpoint를 저장할 경로
THRESHOLD = 0.5
model = BERTClassifier(num_classes=num_classes, dr_rate=DROPOUT_RATE).to(device)

train_model(model, BATCH_SIZE, EPOCHS, THRESHOLD, PATIENCE, CHECKPOINT_PATH)


# 체크포인트 불러오기
model = BERTClassifier(num_classes=num_classes, dr_rate=DROPOUT_RATE).to(device)
CHECKPOINT_PATH = '' # checkpoint 경로
checkpoint = torch.load(CHECKPOINT_PATH)
epoch = checkpoint['epoch']
model.load_state_dict(checkpoint['model_state_dict'])

# test model
test_epoch_pred=[]
model.eval()
with torch.no_grad():
    for batch_id, (token_ids, valid_length, segment_ids, test_label) in enumerate(test_loader):
        token_ids = token_ids.long().to(device)
        segment_ids = segment_ids.long().to(device)
        valid_length = valid_length

        test_label = test_label.float().to(device)
        test_out = model(token_ids, valid_length, segment_ids)

        test_target = test_label.detach().cpu().numpy()

        test_pred = torch.sigmoid(test_out).detach().cpu().numpy()
        test_pred = np.array(test_pred > THRESHOLD, dtype=float)
        test_pred = test_pred.flatten()
        test_epoch_pred.append(test_pred)

test_epoch_pred = np.concatenate(test_epoch_pred)
test_epoch_target = test_loader.dataset.labels
test_epoch_target = np.concatenate(test_epoch_target)

test_epoch_accuracy = accuracy_score(y_true=test_epoch_target, y_pred=test_epoch_pred)
test_epoch_precision = precision_score(y_true=test_epoch_target, y_pred=test_epoch_pred)
test_epoch_recall = recall_score(y_true=test_epoch_target, y_pred=test_epoch_pred)
test_epoch_f1 = f1_score(y_true=test_epoch_target, y_pred=test_epoch_pred)

print("test accuracy: {} | test precision: {} | test recall: {} | test f1: {}".format(test_epoch_accuracy, test_epoch_precision, test_epoch_recall, test_epoch_f1))



