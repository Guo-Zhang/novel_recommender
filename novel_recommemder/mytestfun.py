import random
import time

import gevent
from gevent.pool import Pool

seconds = [random.uniform(1, 2) for i in range(100)]
total = sum(seconds)

pool = Pool(5)

def hello_from(i):
    print("begin: %s, pool with %s" % (i,len(pool)))
    second = seconds.pop()
    gevent.sleep(second)
    print('end: %s, time with %s' %(i,second))

begin = time.time()
pool.map(hello_from, range(100))
end = time.time()
print(total)
print(end-begin)