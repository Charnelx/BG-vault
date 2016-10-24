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

class YC_Crawler:

    def __init__(self, codes, proxies=[], limit_concurrent=20, timeout=10, retry=1):
        self.codes = codes
        self.proxies = proxies
        self.limit_concurrent = limit_concurrent
        self.timeout = timeout
        self.retry = retry if retry > 1 else retry + 1

        # patterns
        self.pat_former = r'([\D]{0,100}) (?=Адр)'
        self.pat_former_alt = r'^([А-ЯІЄ\s"\'\-]+)\s'
        self.pat_head = r'([\D]{0,100})(?=\s?)-\s?\D+([\d.]+)'
        self.pat_head_alt = r'([^а-я-\t\r\nі]+) - ([а-яі]+)'

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
            return (code, 'err', {'type': 'request error or bad status'})

        # print(content)

        try:
            page = parser.fromstring(content)
            blocks = page.xpath('//div[@class="contain-block"]')
        except:
            return (code, 'err', {'type': 'build error'})

        if blocks:
            # Searching head
            head_raw = blocks[4].xpath('./descendant::*/text()')
            if head_raw:
                raw_record = ''.join(head_raw)
                pre_record = ' '.join(raw_record.split())

                try:
                    head = re.search(self.pat_head, pre_record).group(1).strip()
                    head_since = re.search(self.pat_head, pre_record).group(2).strip()
                except AttributeError:
                    head = ''
                    combined = re.findall(self.pat_head_alt, pre_record)
                    for p in combined:
                        person = ' '.join(p)
                        head += person + '\n'
                    head_since = '01/01/1970'
            else:
                head = 'Невідомо'
                head_since = '01/01/1970'

            # Searching former
            former_raw = blocks[5].xpath('./div[@class="block-right text-grey"]/text()')
            if former_raw:
                raw_record = ''.join(former_raw)
                pre_record = ' '.join(raw_record.split())
                try:
                    former = re.search(self.pat_former_alt, pre_record).group().strip()
                except AttributeError:
                    former = 'Невідомо'
            else:
                former = 'Невідомо'

            # Searching phone number
            phone_raw = blocks[6].xpath('./div[@class="block-right text-bold"]/text()')
            if phone_raw:
                phone = ''.join(phone_raw).strip()
            else:
                phone = ''

            data = {
                'head' : head,
                'head_since': head_since,
                'former': former,
                'phone': phone
            }

            return (code, 'good', data)
        else:
            return (code, 'err', {'type': 'parse error'})





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

    def fetch(self, code, semaphore, proxy):
        headers = {"User-Agent": "Mozilla/5.1 (compatible; Googlebot/2.1; +http://www.googIe.com/bot.html)",
               "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
               "Origin":"https://youcontrol.com.ua",
               "Referer":"https://www.google.com.ua/url?sa=t&rct=j&q=&esrc=s&source=web&cd=11&sqi=2&ved=0CFgQFjAK&url=https://youcontrol.com.ua//catalog/company_details/{0}/&ei=NastA2_WMI-O7QautoOXIg&usg=AFQjakahZhWGZeIoJGtAOoSgSG25l4GREeri".format(str(code)),
               "Accept-language":"ua-UA,ua;q=0.8,ru-RU;q=0.6,ru;q=0.4",
               "Upgrade-Insecure-Requests":"1",
               }

        url = 'https://youcontrol.com.ua/catalog/company_details/{0}/'.format(str(code))

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

                                if 'recaptcha' in body.decode():
                                    raise Exception
                                elif '404.css' in body.decode():
                                    raise Exception

                                break
                    except Exception as err:
                        body = 'err'.encode('utf-8')
                        continue
        return (code, body.decode('utf-8', errors='ignore'))




# '22902418'
# proxy = ['http://43.242.104.43:80']
# codes = ['22902418', '39471013', '40426922']
# a = YC_Crawler(codes)
# res = a.start()

