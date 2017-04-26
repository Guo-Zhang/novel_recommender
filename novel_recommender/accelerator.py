# -*-coding:utf-8-*-
# Reference:
#  http://leonard-peng.github.io/2016/07/02/how-to-end-consumer/
#  https://tracholar.github.io/wiki/python/python-multiprocessing-tutorial.html

from multiprocessing import Process, Queue
from queue import Empty
from functools import partial

class Producer(Process):
    def __init__(self, i, producer, data, queue):
        Process.__init__(self)
        self.data = data
        self.producer = producer
        self.queue = queue
        self.i = i

    def run(self):
        print('Producer %d begin to work...'%self.i)
        while not self.data.empty():
            try:
                input_doc = self.data.get_nowait()
                output_doc = self.producer(input_doc)
                if output_doc is None:
                    continue
                self.queue.put(output_doc)
            except (Empty,KeyboardInterrupt):
                print("Producer %d stopped..." % self.i)
                self.queue.put(None)
                break


class Consumer(Process):
    def __init__(self, consumer, queue, n):
        Process.__init__(self)
        self.queue = queue
        self.consumer = consumer
        self.stats = 0
        self.n = n

    def run(self):
        print('Consumer begin to work...')
        while True:
            try:
                doc = self.queue.get(timeout=300)
                if doc is None:
                    self.stats += 1
                    print('%d producers stop...'%self.stats)
                else:
                    self.consumer(doc)
                if self.stats == self.n:
                    break
            except KeyboardInterrupt:
                break
        print("Consumer stopped....")


def manager(data):
    queue = Queue()
    task = 0
    for doc in data:
        queue.put(doc)
        task+=1
    print('Total Work: %s' % task)
    return queue


def pro_con(producer, consumer, data, n=4):
    queue = Queue()
    data = manager(data)

    processes = []
    for i in range(n):
        p = Producer(i+1, producer = producer, data = data, queue = queue)
        processes.append(p)
    c = Consumer(consumer, queue, n)
    processes.append(c)

    for p in processes:
        p.start()

    for p in processes:
        p.join()


def main():
    producer = lambda x: x+1
    consumer = lambda x: print("This is %d"%x)
    data = range(100)
    pro_con(producer, consumer, data)


if __name__=="__main__":
    main()