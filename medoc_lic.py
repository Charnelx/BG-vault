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

class Medlic:

    def __init__(self, codes, proxies=[], limit_concurrent=20, timeout=10, retry=10):
        self.codes = codes
        self.proxies = proxies
        self.limit_concurrent = limit_concurrent
        self.timeout = timeout
        self.retry = retry if retry > 1 else retry + 1
        self.proxy_list = None

        # patterns
        self.pat_lic = re.compile('(LIC_Type.*\d+/\d+/\d+)\s+')
        self.pat_dt = re.compile('\d+\s+([\D\s-]*\s?\d+/\d+/\d+)', re.IGNORECASE)
        self.pat_title = re.compile('([\D\s-]+)', re.IGNORECASE)
        self.pat_date = re.compile('(\d+/\d+/\d+)')

    def start(self):

        response = asyncio.get_event_loop().run_until_complete(self.getLic())

        data = asyncio.get_event_loop().run_until_complete(self.get_data(response))
        return data


    @asyncio.coroutine
    def getLic(self):
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
    def get_data(self, response):
        # semaphore = asyncio.Semaphore(self.limit_concurrent)
        tasks = []
        result = []

        for d in response:
            code, content = d
            tasks.append(self.parse(code, content))

        for task in asyncio.as_completed(tasks):
            data = yield from task
            result.append(data)
        return result

    @asyncio.coroutine
    def parse(self, code, content):
        max_dt = datetime.strptime('01/01/1970', '%d/%m/%Y')
        title = 'Відсутня'

        if content == 'err':
            return (code, 'err', max_dt.strftime('%d/%m/%Y'))

        matched = re.findall(self.pat_dt, content)

        for element in matched:
            title = re.search(self.pat_title, element)
            date = re.search(self.pat_date, element).group()
            title = re.sub('[\t\n\r\f\v]', '', title.group())

            dt_date = datetime.strptime(date, '%d/%m/%Y')
            if dt_date > max_dt:
                max_dt = dt_date

        return (code, title, max_dt.strftime('%d/%m/%Y'))

    @asyncio.coroutine
    def fetch(self, code, semaphore, proxy):
        headers = {"User-Agent": "medoc1001118",
                   "Host":"lic.bestzvit.com.ua"}
        url = 'http://lic.bestzvit.com.ua/key_medoc_test.php?edrpo=%s' % code

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
                        body = 'err'.encode('cp1251')
                        continue
        bd = body.decode('cp1251', errors='ignore')
        if bd == '':
            # print('Null')
            return (code, '')
        elif '<head>' in bd:
            # print('Shit')
            return (code, 'err')
        return (code, body.decode('cp1251', errors='ignore'))

# p = ['http://115.31.183.94:80']
# l = []
# with open('./lic.txt', 'r') as f:
#     while True:
#         code = f.readline().strip()
#         if not code:
#             break
#         l.append(code)
# print(l)
# a = Medlic(l)
# data = a.start()
#
# for d in data:
#     print(d)