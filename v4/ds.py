import re
import datetime
import threading
import random


# 文件中读出数据
def read_log(path):
    with open(path) as f:
        yield from f


# 解析数据
def parse(path):
    name_compile = re.compile(r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) .* .* \[(?P<time>.*)\] "(?P<method>\w+) (?P<url>[^\s]*) (?P<version>[\w|/\.\d]*)" (?P<status>\d{3}) (?P<length>\d+) "(?P<referer>[^\s]*)" "(?P<ua>.*)"')
    for line in read_log(path):
        name_data = name_compile.search(line.rstrip('\n'))
        if name_data:
            data = name_data.groupdict()
            now = datetime.datetime.now()
            data['time'] = now.strftime('%d/%b/%Y:%H:%M:%S %z')
            yield data


# 产生多个数据源
def data_source(event, src, *dst):
    paths = []
    while not event.is_set():
        for path in dst:
            paths.append(open(path, 'a'))
        for item in parse(src):
            line = '{ip} - - [{time}] "{method} {url} {version}" {status} {length} "{referer}" "{ua}"\n'.format(**item)
            f = random.choice(paths)
            f.write(line)
            event.wait(0.01)
    for f in paths:
        f.close()


if __name__ == '__main__':
    import sys
    event = threading.Event()
    try:
        data_source(event, sys.argv[1], *sys.argv[2:])
    except KeyboardInterrupt:
        event.set()
