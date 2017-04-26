# -*-coding:utf-8-*-

import os
import subprocess

from pymongo import MongoClient


IP_POOL = [
'117.90.3.101:9000',
'120.76.79.21:80',
'111.73.241.110:9000',
'121.232.145.231:9000',
'103.199.147.54:8080',
'117.90.6.41:9000',
'121.232.144.247:9000',
]


DATA_PATH = '/Users/zhangguo/Data/novels'
if not os.path.exists(DATA_PATH):
    os.mkdir(DATA_PATH)

RAW_PATH = DATA_PATH+'/raw_qsw'
if not os.path.exists(RAW_PATH):
    os.mkdir(RAW_PATH)

FEATURE_PATH = DATA_PATH + '/feature_qsw'
if not os.path.exists(FEATURE_PATH):
    os.mkdir(FEATURE_PATH)


def set_mongo():
    'MongoDB settings'

    log = open('novel_recommender.log', 'w')
    p = subprocess.Popen("kill -2 `pgrep mongo`", shell=True, stdout=log)
    p.kill()
    p = subprocess.Popen(['mongod', '--dbpath', DATA_PATH], stdout=log)
    client = MongoClient()
    novel_qsw = client.novel_qsw
    # novel_qsw.drop_collection("page_urls")
    # novel_qsw.drop_collection("novel_urls")
    # novel_qsw.drop_collection("novel_info")
    page_urls = novel_qsw.page_urls
    try:
        page_urls.create_index('url')
    except:
        pass
    novel_urls = novel_qsw.novel_urls
    try:
        novel_urls.create_index('url')
    except:
        pass
    novel_info = novel_qsw.novel_info
    novel_features = novel_qsw.novel_feature
    novel_features.drop()
    collections = {
        'page_urls':page_urls,
        'novel_urls':novel_urls,
        'novel_info':novel_info,
    }
    return collections