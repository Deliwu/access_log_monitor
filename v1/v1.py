import re
import sys


# 读日志
def read_log(path):
    with open(path) as f:
        yield from f


# 解析日志
def parse_log(path):
    line = r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) .* .* \[(?P<datetime>.*)\] "(?P<method>\w+) (?P<url>[^\s]*) (?P<version>[\w|/\.\d]*)" (?P<status>\d{3}) (?P<size>\d+) "(?P<referer>[^\s]*)" "(?P<ua>.*)"'
    re_compile = re.compile(line)
    for lines in read_log(path):
        op = re_compile.search(lines.rstrip('\n'))
        if op:
            yield op.groupdict()


# 用来计算的
def count(key, data):
    if key not in data.keys():
        data[key] = 0
    data[key] += 1
    return data


# 分析日志
def analyze(path):
    data = {
        'ip': {},
        'url': {},
        'ua': {},
        'status': {},
        'traffic': 0
    }

    for item in parse_log(path):
        for key, value in data.items():
            if key != 'traffic':
                data[key] = count(item[key], value)
        data['traffic'] += int(item['size'])
    return data


# 主函数
def main():
    data = analyze(sys.argv[1])
    print('traffic is {0}'.format(data['traffic']))
    item = list(data['ip'].items())
    item.sort(key=lambda x:x[1], reverse=True)
    print(item[:10])
    return


if __name__ == '__main__':
    main()
