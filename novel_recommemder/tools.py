# -*-coding:utf-8-*-

import time


def runtime(func):
    def _wrapper(*args, **kargs):
        begin = time.time()
        func(*args,**kargs)
        end = time.time()
        print('-> run time %0.2f s'%(end-begin))
    return _wrapper