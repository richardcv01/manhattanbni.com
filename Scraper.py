import asyncio

import itertools
from aiohttp import ClientSession
from lxml import etree
from time import sleep
from lxml import html
import urllib
import urllib.request
import csv
import io


class get_urls():
    def __init__(self, urls_page):
        self.result = []
# Сюда будем складывать результат.
        self.total_checked = 0
# Сколько всего страниц наша программа запарсила на данный момент.
        self.urls_page =  urls_page

    async def get_one(self, url, session):
        #global
        #self.total_checked
        proxy = "http://10.24.100.210:3128"

        async with session.get(url, proxy=proxy) as response:
        # Ожидаем ответа и блокируем таск.
            page_content = await response.read()
            item = self.get_item(page_content, url)
            self.result.append(item)
            self.total_checked += 1
            print('Inserted: ' + url + '  - - - Total checked: ' + str(self.total_checked))

    async def bound_fetch(self, sm, url, session):
        try:
            async with sm:
                await self.get_one(url, session)
        except Exception as e:
            print(e)
            # Блокируем все таски на 30 секунд в случае ошибки 429.
            sleep(10)

    async def run(self, urls):
        tasks = []
        # Выбрал лок от балды. Можете поиграться.
        sm = asyncio.Semaphore(10)
        headers = {"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}
        # Опять же оставляем User-Agent, чтобы не получить ошибку от Metacritic
        async with ClientSession(
                headers=headers) as session:
            for url in urls:
                # Собираем таски и добавляем в лист для дальнейшего ожидания.
                task = asyncio.ensure_future(self.bound_fetch(sm, url, session))
                tasks.append(task)
        # Ожидаем завершения всех наших задач.
            await asyncio.gather(*tasks)


    def get_item(self, page_content, url):
       # Получаем корневой lxml элемент из html страницы.
        document = html.fromstring(page_content)
        def get(xpath):
            item = document.xpath(xpath)
            if item:
                return item
            # Если вдруг какая-либо часть информации на странице не найдена, то возвращаем None
            return None
        #urls = get("//div[@class='pro-cover-photos']/a/@href")
        urls = get('//a[@class="linkone"]/@href')
        list_urls = [('http://manhattanbni.com/' + url).replace(' ', '%') for url in urls]
        #urls_full.append(urls)
        return list_urls

    def main(self):
        # Запускаем наш парсер.
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.run(self.urls_page))
        loop.run_until_complete(future)

        # Выводим результат. Можете сохранить его куда-нибудь в файл.
        list_urls = list(itertools.chain.from_iterable(self.result))
        return list_urls

def get_url_page():
    """
    :param count: кількість сторінок
    :return: Список urls сторінок з темами
    """

    request = urllib.request.Request('http://manhattanbni.com/chapterlist.php?chapterName=manhattan&chapterCity=&chapterArea=&chapterMeetingDay=&chapterMeetingTime=')

    hnds = []
    proxy_map = {'http': '10.24.100.210:3128'}

    hnd = urllib.request.ProxyHandler(proxy_map)
    hnds.append(hnd)

    opener = urllib.request.build_opener(*hnds)
    urllib.request.install_opener(opener)

    response = urllib.request.urlopen(request, timeout=20)
    tree = etree.HTML(response.read())
    urls = tree.xpath('//a[@class="linkone"]/@href')
    list_urls = [('http://manhattanbni.com/' + url).replace(' ', '%') for url in urls]
    return list_urls

class get_data():
    def __init__(self, urls_page):
        self.result = []
# Сюда будем складывать результат.
        self.total_checked = 0
# Сколько всего страниц наша программа запарсила на данный момент.
        self.urls_page =  urls_page
        self.list_data = []

    async def get_one(self, url, session):
        #global
        #self.total_checked
        proxy = "http://10.24.100.210:3128"
        async with session.get(url, proxy=proxy) as response:
        # Ожидаем ответа и блокируем таск.
            page_content = await response.read()
            item = self.get_item(page_content, url)
            self.result.append(item)
            self.total_checked += 1
            print('Inserted: ' + url + '  - - - Total checked: ' + str(self.total_checked))

    async def bound_fetch(self, sm, url, session):
        try:
            async with sm:
                await self.get_one(url, session)
        except Exception as e:
            print(e)
            # Блокируем все таски на 30 секунд в случае ошибки 429.
            sleep(10)

    async def run(self, urls):
        tasks = []
        # Выбрал лок от балды. Можете поиграться.
        sm = asyncio.Semaphore(15)
        headers = {"User-Agent": "Mozilla/5.001 (windows; U; NT4.0; en-US; rv:1.0) Gecko/25250101"}
        # Опять же оставляем User-Agent, чтобы не получить ошибку от Metacritic
        async with ClientSession(
                headers=headers) as session:
            for url in urls:
                # Собираем таски и добавляем в лист для дальнейшего ожидания.
                task = asyncio.ensure_future(self.bound_fetch(sm, url, session))
                tasks.append(task)
        # Ожидаем завершения всех наших задач.
            await asyncio.gather(*tasks)


    def get_item(self, page_content, url):
       # Получаем корневой lxml элемент из html страницы.
        document = html.fromstring(page_content)

        def get(xpath):
            item = document.xpath(xpath)
            if item:
                return item
            # Если вдруг какая-либо часть информации на странице не найдена, то возвращаем None
            return None
        #urls = get("//div[@class='pro-cover-photos']/a/@href")
        print(url)
        name = get("//td[@align='left']/h1/text()")
        if name == None:
            name = [""]

        company = get("//td[@align='left']/text()")
        if company == None:
            company = [""]

        address = get("//div[@class='leftcol']/div/p/text()")
        if address == None:
            address = ""
        else:
            if len(address) == 4:
                address = address[2] + address[3]
            if len(address) == 5:
                address = address[2] + address[3] + address[4]
            if len(address) == 2:
                address = address[0] + address[1]

        profession = get("//p[@class='categories']/text()")
        if profession == None:
            profession = [""]


        phone = get("//div[@class='leftcol']/p/text()")
        if phone == None:
            phone = [""]
        phone = ', '.join(phone)

        chapter = get("//td[@align='left']/a/text()")
        if chapter == None:
            chapter = [""]

        web = get("//a[@class='link']/text()")
        if web == None:
            web = [""]


        dic = dict()
        dic['web'] = web[0]
        dic['name'] = name[0]
        dic['address'] = address
        dic["company"] = company[0]
        dic["profession"] = profession[0]
        dic["phone"] = phone
        dic["chapter"] = chapter[0]

        print(dic)
        self.list_data.append(dic)
        # urls_full.append(urls)
        return urls


    def main(self):
        # Запускаем наш парсер.
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.run(self.urls_page))
        loop.run_until_complete(future)
        # Выводим результат. Можете сохранить его куда-нибудь в файл.
        #list_urls = list(itertools.chain.from_iterable(self.result))
        data = self.list_data
        return data

def write_svc(data):
    FILENAME = "users.csv"
    with io.open(FILENAME, "w", encoding="utf-8") as file:
        #f.write(html)
    #with open(FILENAME, "w", newline="") as file:
        columns = list(data[0].keys())
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        # запись нескольких строк
        writer.writerows(data)
        print("write Ok!")

urls_page = get_url_page()
S = get_urls(urls_page)
urls = S.main()
print(urls)
SS = get_data(urls)
data = SS.main()
write_svc(data)




