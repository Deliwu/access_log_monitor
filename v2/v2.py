import re
import os
import requests
import datetime
import threading


re_compile = re.compile(r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) .* .* \[(?P<datetime>.*)\] "(?P<method>\w+) (?P<url>[^\s]*) (?P<version>[\w|/\.\d]*)" (?P<status>\d{3}) (?P<size>\d+) "(?P<referer>[^\s]*)" "(?P<ua>.*)"')


# 读日志
def read_log(path):
    offset = 0
    event = threading.Event()
    while not event.is_set():
        with open(path) as f:
            if offset > os.stat(path).st_size:
                offset = 0
            f.seek(offset)
            yield from f
            offset = f.tell()
        event.wait(0.1)


# 解析日志
def parse_log(path):
    for lines in read_log(path):
        op = re_compile.search(lines.rstrip('\n'))
        if op:
            data = op.groupdict()
            yield data


# 做数据聚合
def agg(path, interval=10):
    count = 0
    traffic = 0
    error = 0
    start = datetime.datetime.now()
    for item in parse_log(path):
        count += 1
        traffic += int(item['size'])
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
    requests.post('http://127.0.0.1:8086/write', data=line, params={'db':'logdb'})


# 用来计算的
def count(key, data):
    if key not in data.keys():
        data[key] = 0
    data[key] += 1
    return data


if __name__ == '__main__':
    import sys
    agg(sys.argv[1])
