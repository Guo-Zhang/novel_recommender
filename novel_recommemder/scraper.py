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


import requests
from lxml import etree
from gevent.pool import Pool

# data path
from config import DATA_PATH, RAW_PATH, set_mongo


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
    usedIDs = []
    for doc in docs:
        usedIDs.append({'url': doc['_id']})
    result = idcol.update_many({'_used':0, '$or': usedIDs}, {'$inc': {'_used': 1}})
    print('Updated %d IDs'%(result.modified_count))


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

    name = html.xpath('//*[@id="downInfoArea"]/h1/font/b/text()')[0]
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
    cond = []
    for fname in os.listdir(RAW_PATH):
        cond.append({'_id': os.path.splitext(fname)[0]})

    if not cond:
        return None

    result = collection.update_many({'_raw': 0, '$or': cond}, {'$inc': {'_raw': 1}})
    print('Updated %d info of novels' %(result.modified_count))


def get_raw_urls(collection):
    docs = collection.find({'_raw':0}, ['下载链接'])
    urls = list(docs)
    return urls


def download_novel(doc):
    url = doc['下载链接']
    fname = doc['_id']+'.rar'
    path = os.path.join(RAW_PATH, fname)

    try:
        r = requests.get(url, timeout=5)
    except Exception as e:
        print(e)
        return None

    with open(path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)
    print('%s saved'%(fname))


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
        save_novelinfo(info, novel_info)
        print('end: %s' % id)
    except Exception as e:
        print(e)


def save_novel_info(novel_urls, novel_info):
    update_used_ids(novel_urls, novel_info)
    ids = read_urls(novel_urls)
    print('Total Tasks: %d'%(len(ids)))
    pool = Pool(10)
    unit = functools.partial(save_info_unit, novel_urls=novel_urls, novel_info=novel_info)
    pool.map(unit, ids)


def save_novel_raw(novel_info):
    update_file_record(novel_info)
    urls = get_raw_urls(novel_info)
    print('Total Raw: %d'%(len(urls)))
    pool = Pool(10)
    pool.map(download_novel, urls)


def main():
    page_urls, novel_urls, novel_info = set_mongo()
    # save_page_urls(page_urls, novel_urls)
    # save_novel_urls(page_urls, novel_urls)
    save_novel_info(novel_urls, novel_info)
    save_novel_raw(novel_info)


if __name__=="__main__":
    main()

