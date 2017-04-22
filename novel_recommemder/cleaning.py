#! usr/bin/python
# -*-coding:utf-8-*-

# Reference:
#  https://rarfile.readthedocs.io/en/latest/api.html
#  http://arylo.me/2016/03/27/other/command_unrar-much-files/
#  http://blog.jobbole.com/99063/

# Author: Guo Zhang
# Email: zhangguo@stu.xmu.edu.cn
# Created Date: 2017-04-22
# Updated Date: 2017-04-22
# Python Version: 3.6.0


import os
import shutil

from rarfile import RarFile

from config import set_mongo


RAW_PATH = '/Users/zhangguo/Data/test'


def extract_rar(fname):
    print('extracting %s ...'%fname)
    path = os.path.join(RAW_PATH, fname)
    path_new = path.replace('.rar', '')

    rf = RarFile(path)
    rflist = rf.namelist()
    rf.extractall(path=path_new)

    if len(rflist)==1:
        txtname = rflist[0]
        txtpath = os.path.join(path_new, txtname)
        txtname_new = '_'.join([fname.replace('.rar',''),txtname])
        txtpath_new = os.path.join(RAW_PATH, txtname_new)
        os.rename(txtpath, txtpath_new)
        os.rmdir(path_new)
        print('extraced %s'%txtname_new)
    else:
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
            print('extraced %s' % txtname_new)

    os.remove(path)


def update_rawinfo(path, collection):
    for fname in path:
        if '.txt' in fname:
            try:
                id = fname.split('_')[0]
                collection.update_one({'_id':id},{"$set":{'_f':fname}, "$inc":{'_raw':1}})
            except Exception as e:
                print('%s: %s'%(fname,e))


def main():
    for fname in os.listdir(RAW_PATH):
        if '.rar' in fname:
            extract_rar(fname)

    page_urls, novel_urls, novel_info = set_mongo()
    update_rawinfo(RAW_PATH, novel_info)


if __name__=='__main__':
    main()
