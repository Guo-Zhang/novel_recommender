# -*-coding:utf-8-*-

import os
import subprocess

from pymongo import MongoClient


DATA_PATH = '/Users/zhangguo/Data/novels'
if not os.path.exists(DATA_PATH):
    os.mkdir(DATA_PATH)

RAW_PATH = DATA_PATH+'/raw_qsw'
if not os.path.exists(RAW_PATH):
    os.mkdir(RAW_PATH)


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
    return page_urls, novel_urls, novel_info