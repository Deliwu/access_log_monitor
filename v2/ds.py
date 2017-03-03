import re
import datetime
import threading


# 读日志
def read_log(path):
    with open(path) as f:
        yield from f


# 解析日志
def parse(path):
    name_complie = re.compile(r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) .* .* \[(?P<time>.*)\] "(?P<method>\w+) (?P<url>[^\s]*) (?P<version>[\w|/\.\d]*)" (?P<status>\d{3}) (?P<length>\d+) "(?P<referer>[^\s]*)" "(?P<ua>.*)"')
    for line in read_log(path):
        opert = name_complie.search(line.rstrip('\n'))
        if opert:
            data = opert.groupdict()
            now = datetime.datetime.now()
            data['time'] = now.strftime('%d/%b/%Y:%H:%M:%S %z')
            yield data


# 生成实时日志
def data_source(src, dst, event):
    while not event.is_set():
        with open(dst, 'a') as f:
            for item in parse(src):
                line = '{ip} - - [{time}] "{method} {url} {version}" {status} {length} "{referer}" "{ua}"\n'.format(**item)
                f.write(line)
                event.wait(0.1)


if __name__ == '__main__':
    import sys
    e = threading.Event()
    try:
        data_source(sys.argv[1], sys.argv[2], e)
    except KeyboardInterrupt:
        e.set()
