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

class SKeys:

    def __init__(self, codes, proxies=[], limit_concurrent=20, timeout=10, retry=10):
        self.codes = codes
        self.proxies = proxies
        self.limit_concurrent = limit_concurrent
        self.timeout = timeout
        self.retry = retry if retry > 1 else retry + 1

        #patterns
        self.pat_code = re.compile('(\d+)')
        self.pat_none = re.compile('(Немає записів з такими параметрами пошуку.)')

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
        if content == 'err':
            return (code, 'err')

        page = parser.fromstring(content)
        serts = page.xpath('//td[@class="textDisable"]/text()')
        if len(serts) > 0:
            c = re.search(self.pat_code, serts[0])
            keys = int(c.group())/2
            return (code, keys)
        else:
            error = page.xpath('//font[@class="error"]/text()')
            if len(error) > 0:
                c = re.search(self.pat_none, error[0])
                if c.group() == 'Немає записів з такими параметрами пошуку.':
                    return (code, 0)
                else:
                    return (code, 'err')
            else:
                return (code, 'err')

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
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
               "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
               "Origin":"http://www.acskidd.gov.ua",
               "Referer":"http://www.acskidd.gov.ua/certificates-search",
               "Upgrade-Insecure-Requests":"1",
               'Cookie': '_ym_uid=1461405089410619382; iit=v3fkegmpio51ulctv11pbolia3; __utmt=1; _ym_isad=1;'
                         ' __utma=241803538.744639680.1461405089.1461407685.1463689057.3; '
                         '__utmb=241803538.8.10.1463689057; __utmc=241803538; '
                         '__utmz=241803538.1463689057.3.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)'}
        data = {
                    'searchtype':'1',
                    'pid':'9',
                    'itIssuerEDRPOU':code,
                    'itIssuerDRFO':'',
                    'g-recaptcha-response':'03AHJ_VuutzS6ENT6H35FEUG42KhFn79BjU8FDBoAMRTATIG0XsEa5iP_6a2l5fe20sTnLYVEkYgBYNW0EOSxadQN9Rt8Hv7_HkitL7eCUB7VUygsKrAJ59NmIeAS-hdMEO4xbpO0GymOFDU2-bAixVwWeLOlywV_7nntgPCxMLuR-0HkIw6CZR6fIdA5UCmeOkfnSWS0x_4DCDIDvABPxCB0Rsat4aZkW8AZupVW2WY-pvRoFwxUguCtGrZMTsFa4n1PZ9CRWSBuo8OUSZ-UMYLRzcvgHLHZ08ggBZVdRbnnLe5oFWAz3ILU104UUXbXNzhLM2HCZl4hz1cDALWBUvEdGH0qCOqBESLibrIugQ-LSY9p6LcEHRe7T943WLKxS-Bzjrupyp_G7r4a2HozqMRzcfiB8elGyShNiA4ozRFBUlyXQGly1SG_88ihiUVVssjWafyOyrZTVVHoS9QITCTNlurbStahKHOQhhROKEZqpARLAbbT07bKRQJFHJArM8Uv6W5lDqCrqgmxMgNGobmxwBkN46PkfiIn7wgjHFFnnrqYUcPF4QqQT8OuGGUmMEufh3-9Jcs0XVqBrFGeTN87j1mL8CQhZhgq6pkwE7njsavwDQceanli2yWNMjJhX2uEtU095DOKY3RATfm6cFEC-WyXzitWei8w2zVBdxKIaR-m55qeuxhZlXZnO5SsoEjcUaWxKkne61Yy-NWDXzSEGXxEq1CDQ8cdLCz2Fw0UFmyw2EldI8Jic31T8leSWeYwBVYeoTsUU7n724T--0TAphvNZrYJNlpPRwhDNC9WGLdJ2_EwhqkKcW7mJ2mWmOs8uACvvY88jLd4DREzOkLO_dq-gX26NyP7XkiUw5ecWC4dVGKKuYFTT9LWO-tj1nSnTvQHOadhJzRasE3vIxDNv2zNSxvVI9xsTvuY82LGWj0yQ9B9RjaQ'
                }

        url = 'http://www.acskidd.gov.ua/certificates-search-results'

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
                                response = yield from session.post(url, data=data, headers=headers)
                                body = yield from response.read()
                                print(body.decode())
                                break
                    except Exception as err:
                        body = 'err'.encode('utf-8')
                        continue
        return (code, body.decode('utf-8', errors='ignore'))

# proxy = ['http://181.211.166.47:8080']
# codes = ['38345394']
# a = SKeys(codes)
# res = a.start()
# print(res)