import os
import re
import datetime
import threading
import requests
import multiprocessing

o = re.compile(r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) .* .* \[(?P<time>.*)\] "(?P<method>\w+) (?P<url>[^\s]*) (?P<version>[\w|/\.\d]*)" (?P<status>\d{3}) (?P<length>\d+) "(?P<referer>[^\s]*)" "(?P<ua>.*)"')


# 读文件
def read_log(path, q):
    offset = 0
    event = threading.Event()
    while not event.is_set():
        with open(path) as f:
            if offset > os.stat(path).st_size:
                offset = 0
            f.seek(offset)
            for line in f:
                q.put(line)
            offset = f.tell()
        event.wait(0.1)


# 每个路径启用一个线程来读
def read_worker(path, q):
    t = threading.Thread(target=read_log, name='read-{}'.format(path), args=(path,q))
    t.start()


# 解析数据
def parse(in_queue, out_queue):
    while True:
        line = in_queue.get()
        name_data = o.search(line.rstrip('\n'))
        if name_data:
            data = name_data.groupdict()
            out_queue.put(data)


# 做数据聚合
def agg(q, interval=10):
    count = 0
    traffic = 0
    error = 0
    start = datetime.datetime.now()
    while True:
        item = q.get()
        print(item)
        count +=1
        traffic += int(item['length'])
        if int(item['status']) >= 300:
            error += 1
        current = datetime.datetime.now()
        if (current - start).total_seconds() >= interval:
            error_rate = error / count
            send(count, traffic, error_rate)
            start = current
            count = 0
            traffic = 0
            error = 0


# 发送数据到influxdb
def send(count, traffic, error_rate):  # send to influxdb
    line = 'access_log count={},traffic={},error_rate={}'.format(count, traffic, error_rate)
    res = requests.post('http://127.0.0.1:8086/write', data=line, params={'db': 'logdb'})
    if res.status_code >= 300:
        print(res.content)


# 管理多进程
def manager(*paths):
    read_queue = multiprocessing.Queue()
    parse_queue = multiprocessing.Queue()
    for path in paths:
        read_worker(path, read_queue)
    for _ in range(4):
        p = multiprocessing.Process(target=parse, name='parse', args=(read_queue, parse_queue))
        p.start()
    agg(parse_queue)

if __name__ == "__main__":
    import sys
    manager(*sys.argv[1:])
