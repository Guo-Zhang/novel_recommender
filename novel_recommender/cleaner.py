#! usr/bin/python
# -*-coding:utf-8-*-

# Reference:
#  https://rarfile.readthedocs.io/en/latest/api.html
#  http://arylo.me/2016/03/27/other/command_unrar-much-files/
#  http://blog.jobbole.com/99063/

# Notes:
#   Read data with _id and {'_f':1}

# Author: Guo Zhang
# Email: zhangguo@stu.xmu.edu.cn
# Created Date: 2017-04-22
# Updated Date: 2017-04-22
# Python Version: 3.6.0


import os
import shutil

from rarfile import RarFile, BadRarFile
from tqdm import tqdm

from config import set_mongo, RAW_PATH


def extract_rar(fname, collection):
    print("extracting: %s"%fname)
    id = fname.replace('.rar','')
    path = os.path.join(RAW_PATH, fname)
    path_new = path.replace('.rar', '')

    try:
        rf = RarFile(path)
    except BadRarFile as e:
        collection.update_one({'_id': id, '_raw': 1}, {"$inc": {'_raw': -1}})
        print('Bad RAR File... %s'%fname)
        input()
        os.remove(path)
        return None

    rflist = rf.namelist()
    rf.extractall(path=path_new)

    if len(rflist)==1:
        txtname = rflist[0]
        txtpath = os.path.join(path_new, txtname)
        txtname_new = '_'.join([fname.replace('.rar',''),txtname])
        txtpath_new = os.path.join(RAW_PATH, txtname_new)
        os.rename(txtpath, txtpath_new)
        os.rmdir(path_new)
        collection.update_one({'_id': id, '_raw': 1}, {"$set": {'filename': txtname_new, '_f': 1}, "$inc": {'_raw': 1}})
        print('Extracted... %s to %s'%(fname,txtname_new))
    elif len(rflist)>1:
        rflist_new = []
        for rfname in rflist:
            if ('.txt' in rfname) and ('说明' not in rfname):
                rflist_new.append(rfname)

        if len(rflist_new)==1:
            txtname = rflist_new[0]
            txtpath = os.path.join(path_new, txtname)
            txtname_new = '_'.join([fname.replace('.rar', ''), txtname])
            txtpath_new = os.path.join(RAW_PATH, txtname_new)

            os.rename(txtpath, txtpath_new)
            shutil.rmtree(path_new)
            collection.update_one({'_id': id, '_raw': 1}, {"$set": {'filename': txtname_new, '_f': 1}, "$inc": {'_raw': 1}})
            print('Extracted... %s to %s' % (fname, txtname_new))
        else:
            collection.update_one({'_id': id, '_raw': 1}, {"$set": {'_f': 0}})
            print('Extracted... %s'%fname)
    else:
        collection.update_one({'_id': id, '_raw': 1}, {"$inc": {'_raw': -1}})
        print('Unknown problems... %s' % fname)
    os.remove(path)


def check_txts(collection):
    docs = collection.find({'_f':1},['filename'])
    for doc in docs:
        id = doc['_id']
        fname = doc['filename']
        if not os.path.exists(os.path.join(RAW_PATH,fname)):
            print('%s not exists'%id)


def main():
    collections = set_mongo()
    novel_info = collections['novel_info']

    flist = os.listdir(RAW_PATH)
    for fname in flist:
        if '.rar' in fname:
            extract_rar(fname, novel_info)

    check_txts(novel_info)


if __name__=='__main__':
    main()


