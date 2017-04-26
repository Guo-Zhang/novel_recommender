#! usr/bin/python
# -*-coding:utf-8-*-

# Reference:
#  http://xlambda.com/gevent-tutorial/#gevent-zeromq
#  http://api.mongodb.com/python/current/api/pymongo/collection.html
#  http://www.runoob.com/mongodb/mongodb-update.html

# Author: Guo Zhang
# Email: zhangguo@stu.xmu.edu.cn
# Created Date: 2017-04-21
# Updated Date: 2017-04-21
# Python Version: 3.6.0

from datetime import datetime
import functools
import os
import time
import random
from urllib.request import urlretrieve

from gevent import monkey; monkey.patch_all()
import gevent
from gevent.pool import Pool

import requests
from lxml import etree
from tqdm import tqdm

# data path
from config import DATA_PATH, RAW_PATH, set_mongo, IP_POOL


# -- tool functions --

def create_caturls():
    urls = ['http://www.qswtxt.com/class_%d.html'%(i) for i in range(1,13)]
    urls.append('http://www.qswtxt.com/class_15.html')
    return urls


def match_pages(raw):
    return int(raw.split('/')[1].split('页')[0])


def parse_novelid(href):
    return href.split('/')[-1].split('.')[0]


def parse_novelurl(url):
    html = etree.HTML(requests.get(url).content)
    hrefs = html.xpath('//*[@id="listbox"]//*[@class="mainListInfo"]//*[@class="mainSoftName"]/a/@href')
    hrefs = map(parse_novelid, hrefs)
    pages = html.xpath('//*[@class="mainNextPage"]//*[@title="页次"]/text()')[0]
    pages = match_pages(str(pages))
    return hrefs, pages


def create_nexturls(url, pages):
    return [url.replace('.html','_'+str(i)+'.html') for i in range(2,pages+1)]


def save_urls(urls, collection):
    docs = []
    for url in urls:
        doc = {}
        doc['url'] = url
        doc['_used'] = 0
        print(doc)
        docs.append(doc)
    collection.insert_many(docs)


def update_used_ids(idcol, datacol):
    docs = datacol.find({}, ['_id'])
    for doc in docs:
        idcol.update_one({'url': doc['_id'],'_used':0,}, {'$inc': {'_used': 1}})


def read_urls(collection):
    docs = collection.find({'_used':0}, ['url'])
    urls = []
    for doc in docs:
        urls.append(doc['url'])
    return urls


def parse_novelinfo(id):
    url = 'http://www.qswtxt.com/{id}.html'.format(id=id)
    html = etree.HTML(requests.get(url, timeout=5).content)

    info = {'_id': id, '_raw': 0}

    try:
        name = html.xpath('//*[@id="downInfoArea"]/h1/font/b/text()')[0]
    except IndexError:
        return None
    info['书籍名称'] = name

    cat = html.xpath('//*[@class="crumbleft"]/a[2]/text()')[0]
    # /html/body/div[2]/div/div/div[1]/div[1]/span/a[2]
    info['分类'] = cat

    info_title = html.xpath('//*[@id="downInfoArea"]/p/node()')
    for key, value in zip(info_title[0::4],info_title[1::4]):
        key = key.xpath('text()')[0].split('：')[0]
        if not isinstance(value, str):
            value = value.xpath('@src')[0]
            value = int(value.split('/')[-1].split('star')[0])
        info[key] = value

    info['下载次数'] = int(info['下载次数'])
    try:
        info['更新时间'] = datetime.strptime(info['更新时间'], '%Y-%m-%d %H:%M:%S')
    except ValueError:
        info['更新时间'] = datetime.strptime(info['更新时间'], '%Y-%m-%d')

    downurl = html.xpath('//*[@id="downAddress"]/a/@href')[0]
    info['下载链接'] = downurl

    return info


def save_novelinfo(docs, collection):
    if isinstance(docs, list):
        collection.insert_many(docs)
    elif isinstance(docs, dict):
        collection.insert_one(docs)


def update_file_record(collection):
    for fname in os.listdir(RAW_PATH):
        collection.update_one({'_id': os.path.splitext(fname)[0], '_raw': 0}, {'$inc': {'_raw': 1}})


def get_raw_urls(collection):
    docs = collection.find({'_raw':0}, ['下载链接'])
    urls = list(docs)
    return urls


def download_novel(doc):
    url = doc['下载链接']
    id = doc['_id']
    fname = id+'.rar'
    path = os.path.join(RAW_PATH, fname)

    headers = {
'Host': 'www.qswtxt.com',
'Connection': 'keep-alive',
'Cache-Control': 'max-age=0',
'Upgrade-Insecure-Requests': '1',
'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
'Accept-Encoding': 'gzip, deflate',
'Accept-Language': 'zh-CN,zh;q=0.8',
    }
    ip = random.choice(IP_POOL)
    proxies = {'http':ip}

    try:
        # print("Requesting... %s"%(id))
        r = requests.get(url, timeout=5)
        if r.status_code!='200':
            print("Fail: %s"%url)
            return None
    except Exception as e:
        print(e)
        return None

    # print('Saving... %s'%(id))
    with open(path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)


def cbk(a, b, c):
    '''
    回调函数 
    @a: 已经下载的数据块 
    @b: 数据块的大小 
    @c: 远程文件的大小 
    '''
    per = 100.0 * a * b / c
    if per > 100:
        per = 100
    print('%.2f%%'%per)


def download_novel_2(doc):
    url = doc['下载链接']
    id = doc['_id']
    fname = id + '.rar'

    path = os.path.join(RAW_PATH, fname)
    urlretrieve(url=url, filename=path)


# -- work functions --

def save_page_urls(page_urls, novel_urls):
    urls = create_caturls()
    for url in urls:
        hrefs, pages = parse_novelurl(url)
        save_urls(hrefs, novel_urls)
        urls_next = create_nexturls(url, pages)
        save_urls(urls_next, page_urls)


def save_novel_urls(page_urls, novel_urls):
    urls = read_urls(page_urls)
    for url in urls:
        try:
            hrefs, pages = parse_novelurl(url)
            save_urls(hrefs, novel_urls)
            page_urls.update_one({'url': url}, {'$inc': {'_used': 1}})
            print('Total Novel Number: {}'.format(novel_urls.count()))
        except Exception as e:
            print(e)


def save_info_unit(id, novel_urls, novel_info):
    try:

        print('begin: %s'%id)
        info = parse_novelinfo(id)
        if info is None:
            return None
        save_novelinfo(info, novel_info)
        print('end: %s' % id)
    except Exception as e:
        print(e)


def save_novel_info(novel_urls, novel_info):
    update_used_ids(novel_urls, novel_info)
    ids = read_urls(novel_urls)

    print('Total Tasks: %d'%(len(ids)))
    pbar = tqdm(total=len(ids))
    unit = functools.partial(save_info_unit, novel_urls=novel_urls, novel_info=novel_info)
    pool = Pool(100)
    pool.map(unit, ids)
    pbar.close()


def save_novel_raw(novel_info):
    update_file_record(novel_info)
    urls = get_raw_urls(novel_info)

    # pbar = tqdm(total=len(urls))
    # pool = Pool(5)
    # pool.map(download_novel, urls)
    # pbar.close()
    for url in tqdm(urls):
        download_novel_2(url)


def main():
    collections = set_mongo()
    # page_urls = collections['page_urls']
    # novel_urls = collections['novel_urls']
    novel_info = collections['novel_info']

    # finished steps
    # save_page_urls(page_urls, novel_urls)
    # save_novel_urls(page_urls, novel_urls)
    # save_novel_info(novel_urls, novel_info)

    # current steps
    save_novel_raw(novel_info)


if __name__=="__main__":
    main()

