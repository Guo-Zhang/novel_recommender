# -*-coding:utf-8-*-
# Reference:
#   https://docs.python.org/3.6/library/queue.html#queue.Queue
#   https://docs.python.org/3.6/library/multiprocessing.html
#   http://stackoverflow.com/questions/18337407/saving-utf-8-texts-in-json-dumps-as-utf8-not-as-u-escape-sequence

import codecs
import os
import time
import sys
import json
import random
from functools import partial

import jieba
import nltk
from nltk import FreqDist
from pymongo.errors import DuplicateKeyError

from config import RAW_PATH, FEATURE_PATH, set_mongo
from tools import runtime
from accelerator import pro_con


# -- words counting --

def read_uncounted(collection):
    # check '_count'
    result = collection.find_one({'_count':0})
    if not result:
        collection.update_many({'_f': 1}, {'$set': {'_count': 0}})

    # update
    update = 0
    for fname in os.listdir(FEATURE_PATH):
        id = os.path.splitext(fname)[0]
        result = collection.update_one({'_id': id, '_count':0}, {'$inc': {'_count': 1}})
        if result.modified_count:
            update+=1
    print("Update %d records..."%update)

    # statstics
    total = collection.count({'_f': 1})
    finish = collection.count({'_f': 1, '_count': 1})
    print("Finish %d of %d..."%(finish, total))

    # read
    docs = collection.find({'_f':1,'_count':0},['filename'])
    return docs


def count_words(doc):
    id = doc['_id']
    fname = doc['filename']
    path = os.path.join(RAW_PATH,fname)

    try:
        print("Reading %s...\r"%(id), end='')
        with codecs.open(path,'r',encoding='GB18030') as f:
            text = f.read()
    except UnicodeDecodeError as e:
        print('%s: %s'%(id, e))
        return None

    print("Counting %s...\r"%(id), end='')
    seg = jieba.cut(text)
    fdist = FreqDist(seg)
    fdist['_id'] = id

    return fdist


def save_count(doc):
    if doc is None:
        return None
    try:
        id = doc['_id']
        fname = id +'.json'
        path = os.path.join(FEATURE_PATH, fname)

        print("Saving %s...\r" % (id), end='')
        with open(path, 'w') as f:
            doc = json.dumps(doc, ensure_ascii=False)
            f.write(doc)

    except Exception as e:
        print("%s: %s"%(id,e))


# -- TF-IDF --

def tf_idf():
    pass


# -- classification --

def gender_types(col):
    docs = col.find({'_count':1},['_id'])
    for doc in docs:
        type = random.choice(['train','devtest','test'])
        col.update({'_id':doc['_id']},{'$set':{'_type':type}})


def gender_features(col, type):
    docs = col.find({'_count':1,'_type':type},['分类'])
    for doc in docs:
        id = doc['_id']
        cat = doc['分类']
        fname = id+'.json'
        path = os.path.join(FEATURE_PATH, fname)
        with codecs.open(path, 'r', encoding='utf-8') as f:
            text = f.read()
        feature = json.loads(text)
        yield (feature, cat)


def classify(col):
    gender_types(col)
    train_set = gender_features(col,type='train')
    devtest_set = gender_features(col,type='devtest')
    test_set = gender_features(col,type='test')
    print('Gendering feature data...')
    classifier = nltk.NaiveBayesClassifier.train(train_set)
    print(nltk.classify.accuracy(classifier, devtest_set))


# -- main --

@runtime
def main():
    collections = set_mongo()
    info = collections['novel_info']

    # words counting
    # docs = read_uncounted(info)
    # pro_con(count_words, save_count, docs)

    # TF-IDF

    # classification
    classify(info)


if __name__=="__main__":
    main()
