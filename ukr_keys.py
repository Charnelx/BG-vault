__author__ = 'Acheron'

import asyncio
import aiohttp
import re
from lxml import html as parser
from datetime import datetime

class GetProxy:

    def __init__(self, proxies):
        self.proxies = proxies

    @property
    def get_proxy(self):
        proxy = self.proxies.pop()
        self.proxies.insert(0, proxy)
        return proxy

class UKeys:

    def __init__(self, codes, proxies=[], limit_concurrent=20, timeout=10, retry=10):
        self.codes = codes
        self.proxies = proxies
        self.limit_concurrent = limit_concurrent
        self.timeout = timeout
        self.retry = retry if retry > 1 else retry + 1

        # patterns
        self.pat_inn = re.compile('^(\d+)')
        self.pat_date = re.compile('[|]?(\d+-\D+-\d+)[|]?')

    def start(self):
        data = asyncio.get_event_loop().run_until_complete(self.get_data())
        result = asyncio.get_event_loop().run_until_complete(self.parse_data(data))

        return result

    @asyncio.coroutine
    def parse_data(self, data):
        tasks = []
        result = []

        for d in data:
            code, content = d
            tasks.append(self.parse(code, content))

        for task in asyncio.as_completed(tasks):
            data = yield from task
            result.append(data)
        return result

    @asyncio.coroutine
    def parse(self, code, content):
        max_dt = datetime.strptime('01/01/1970', '%d/%m/%Y')

        if content == 'err':
            return (code, 'err', max_dt.strftime('%d/%m/%Y'))

        lines = content.split('\n')

        if lines[0] == '' and len(lines) < 2:
            return (code, 0, max_dt.strftime('%d/%m/%Y'))

        matched = re.search(self.pat_inn, lines[0])
        if matched:
            for line in lines[:-1]:
                date = re.findall(self.pat_date, line)
                dt_date = datetime.strptime(date[1], '%d-%b-%y')
                if dt_date > max_dt:
                    max_dt = dt_date

            return (code, len(lines)-1, max_dt.strftime('%d/%m/%Y'))
        else:
            return (code, 'err', max_dt.strftime('%d/%m/%Y'))


    @asyncio.coroutine
    def get_data(self):
        semaphore = asyncio.Semaphore(self.limit_concurrent)
        tasks = []
        result = []

        if len(self.proxies) == 0:
            proxy = None
        else:
            proxy = GetProxy(self.proxies)

        for i in range(len(self.codes)):
            tasks.append(self.fetch(self.codes[i], semaphore, proxy))

        for task in asyncio.as_completed(tasks):
            response = yield from task
            result.append(response)
        return result

    @asyncio.coroutine
    def fetch(self, code, semaphore, proxy):
        headers = {"User-Agent": "medoc1001119",
                   "Host":"uakey.com.ua"}
        url = 'http://uakey.com.ua/files/cert_list.php?edrpo=%s' % code

        counter = 0
        with (yield from semaphore):
            while True:
                counter += 1
                if counter >= self.retry:
                    break
                with aiohttp.Timeout(self.timeout):
                    try:
                        if self.proxies:
                            p = proxy.get_proxy
                            conn = aiohttp.ProxyConnector(proxy=p)
                        else: conn = None
                        with aiohttp.ClientSession(connector=conn) as session:
                            response = yield from session.get(url, headers=headers)
                            body = yield from response.read()
                            break
                    except Exception as err:
                        body = 'err'.encode('utf-8')
                        continue
        return (code, body.decode('utf-8', errors='ignore'))

# proxy = []
# codes = ['35294300']
# a = UKeys(codes)
# res = a.start()
# print(res)