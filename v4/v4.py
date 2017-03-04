import os
import re
import datetime
import threading
import queue
import requests


name_compile = re.compile(r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) .* .* \[(?P<time>.*)\] "(?P<method>\w+) (?P<url>[^\s]*) (?P<version>[\w|/\.\d]*)" (?P<status>\d{3}) (?P<length>\d+) "(?P<referer>[^\s]*)" "(?P<ua>.*)"')
event = threading.Event()


# 读日志
def read_log(path, q):
    offset = 0
    while not event.is_set():
        with open(path) as f:
            if offset > os.stat(path).st_size:
                offset = 0
            f.seek(offset)
            for line in f:
                q.put(line)
            offset = f.tell()
        event.wait(0.1)


# 把读日志放到一个线程中执行
def read_worker(path, q):
    p = threading.Thread(target=read_log, name='read-{}'.format(path), args=(path, q))
    p.start()


# 解析数据
def parse(q):
    while not event.is_set():
        line = q.get()
        m = name_compile.search(line)
        if m:
            data = m.groupdict()
            yield data


# 聚合数据
def agg(q, interval=10):
    count = 0
    traffic = 0
    error = 0
    start = datetime.datetime.now()
    for item in parse(q):
        print(item)
        count += 1
        traffic += int(item['length'])
        if int(item['status']) >= 300:
            error += 1
        current = datetime.datetime.now()
        if (current - start).total_seconds() >= interval:
            error_rate = error/count
            send(count, traffic, error_rate)
            start = current
            count = 0
            traffic = 0
            error = 0


# 发送数据到influxdb
def send(count, traffic, error_rate):
    line = 'access_log count={},traffic={},error_rate={}'.format(count, traffic, error_rate)
    requests.post('http://127.0.0.1:8086/write', data=line, params={'db': 'logdb'})


# 管理多线程
def manager(*paths):
    q = queue.Queue()
    for path in paths:
        read_worker(path, q)
    agg(q)


if __name__ == '__main__':
    import sys
    manager(*sys.argv[1:])
