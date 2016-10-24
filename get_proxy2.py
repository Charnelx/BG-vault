__author__ = 'Acheron'

import asyncio
import aiohttp
import re
from random import shuffle
from lxml import html
from urllib.parse import urlparse

class Proxy:

    def __init__(self, validate=True, http_only=False, limit_concurrent=20, timeout=10, retry=10, sleep=0):
        self.validate = validate
        self.http_only = http_only
        self.limit_concurrent = limit_concurrent
        self.timeout = timeout
        self.retry = retry
        self.sleep = sleep

        if http_only:
            self.allowed_ports = [80, 8080]
        else:
            self.allowed_ports = [80, 8080, 8888, 8898, 1080, 3128, 9150, 9750, 9000]

        self.PROXY_LIST_URLS = []
        self.PROXY_LIST_URLS.extend(['https://best-proxy.com/english/index.php?p=%d' % p for p in range(1,10)])
        self.PROXY_LIST_URLS.extend(['http://hideme.ru/proxy-list/?maxtime=1500&ports=%d' %
                                port for port in [80, 8080, 8888, 8898, 1080, 3128, 9150, 9750, 9000]])
        self.PROXY_LIST_URLS.append('http://www.prime-speed.ru/proxy/free-proxy-list/all-working-proxies.php')

    def start(self):
        pages = asyncio.get_event_loop().run_until_complete(self.getPages())

        proxies = asyncio.get_event_loop().run_until_complete(self.parser(pages))

        if self.validate:
            proxies = asyncio.get_event_loop().run_until_complete(self.verify_proxy(proxies))

        shuffle(proxies)

        with open('proxylist.txt', 'w') as fh:
            for server in proxies:
                fh.write(server+'\n')

        return proxies

    @asyncio.coroutine
    def verify_proxy(self, proxies):
        semaphore = asyncio.Semaphore(self.limit_concurrent*10)

        tasks = []
        result = []

        for i in range(len(proxies)):
            tasks.append(self.check(proxies[i], semaphore))

        for task in asyncio.as_completed(tasks):
            response = yield from task
            if response != None:
                result.append(response)
        return result



    @asyncio.coroutine
    def check(self, proxy, semaphore):
        # ip, port = proxy.split(':')
        # if int(port) not in [80, 8080]:
        #     return None

        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
               "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
               "Origin":"https://www.youtube.com",
               "Referer":"https://www.youtube.com"}

        url = 'https://www.google.com'
        proxy = 'http://' + proxy

        flag = True
        with (yield from semaphore):
            while True:
                with aiohttp.Timeout(self.timeout):
                    conn = aiohttp.ProxyConnector(proxy=proxy)
                    with aiohttp.ClientSession(connector=conn) as session:
                        try:
                            response = yield from session.get(url, headers=headers)
                            body = yield from response.read()
                            if body:
                                # print('Working proxy: {0}'.format(proxy))
                                flag = True
                            break
                        except Exception as err:
                            flag = False
                            break
        if flag:
            return proxy
        else: return None


    @asyncio.coroutine
    def parser(self, pages):
        tasks = []
        result = []

        for p in pages:
            for k,v in p.items():
                domain, query = k
                content = v

                if 'https://best-proxy.com/' in domain:
                    tasks.append(self.parse_best_proxy(content))
                elif 'http://hideme.ru/' in domain:
                    tasks.append(self.parse_hideme(content))
                elif 'http://www.prime-speed.ru/' in domain:
                    tasks.append(self.parse_prime(content))

        for task in asyncio.as_completed(tasks):
            response = yield from task
            if len(response) > 0:
                result.extend(response)

        result = list(set(result))

        for addr in result:
            ip, port = addr.split(':')
            if int(port) not in self.allowed_ports:
                result.remove(addr)
        return result

    @asyncio.coroutine
    def getPages(self):
        semaphore = asyncio.Semaphore(self.limit_concurrent)

        tasks = []
        result = []

        for i in range(len(self.PROXY_LIST_URLS)):
            tasks.append(self.fetch(self.PROXY_LIST_URLS[i], semaphore))

        for task in asyncio.as_completed(tasks):
            response = yield from task
            result.append(response)
        return result

    @asyncio.coroutine
    def fetch(self, url, semaphore):
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
               "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
               "Origin":"https://www.youtube.com",
               "Referer":"https://www.youtube.com",
               "Upgrade-Insecure-Requests":"1"}

        counter = 0
        with (yield from semaphore):
            while True:
                counter += 1
                if counter >= self.retry:
                    break
                with aiohttp.Timeout(self.timeout):
                    with aiohttp.ClientSession() as session:
                        try:
                            response = yield from session.get(url, headers=headers)
                            body = yield from response.read()
                            break
                        except Exception as err:
                            body = 'err'.encode('utf-8')
                            continue
        url_full = urlparse(url)
        domain_name = '{uri.scheme}://{uri.netloc}/'.format(uri=url_full)
        query = '{uri.query}'.format(uri=url_full)

        return {(domain_name, query):body.decode('utf-8', errors='ignore')}

    @asyncio.coroutine
    def parse_best_proxy(self, content):
        proxy_list = []
        doc = html.fromstring(content)
        for element in doc.xpath('//div[@class="table"]/ul/li[@class="proxy"]/text()'):
            proxy_list.append(element)
        return proxy_list

    @asyncio.coroutine
    def parse_hideme(self, content):
        patter = re.compile('ports=(\d+)', re.IGNORECASE)
        port = re.search(patter, content).group(1)
        proxy_list = []
        doc = html.fromstring(content)
        for element in doc.xpath('//td[@class="tdl"]/text()'):
            proxy_list.append(element + ':%s' % str(port))
        return proxy_list

    @asyncio.coroutine
    def parse_prime(self, content):
        pattern = re.compile('\d+.\d+.\d+.\d+:\d+', re.IGNORECASE)
        proxy_list = []
        doc = html.fromstring(content)
        for element in doc.xpath('//pre/text()'):
            for address in re.findall(pattern, element):
                proxy_list.append(address)
        return proxy_list

# a = Proxy()
# a.start()