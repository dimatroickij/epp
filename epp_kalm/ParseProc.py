import csv
import os
import re
import uuid
from random import choice
from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup


class ParseProc:

    def __init__(self, totalPages, endPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter, source):
        self.source = source
        self.desktop_agents = [
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 '
            'Safari/602.2.14',
            'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 '
            'Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 '
            'Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0']

        # totalPages - номер последней страницы; от totalPages до 1
        self.totalPages = totalPages
        self.endPages = endPages
        self.url = url
        self.urlToNews = urlToNews

        self.resDir = 'F:\\parseProc\\proc\\%s' % nameFolder
        self.picDir = 'F:\\parseProc\\proc\\%s\\images' % nameFolder
        self.deltaName = os.getcwd() + '\\epp_parse\\fromEppNews_proc_%s.xlsx' % numberProc

        self.pathProc = '\\Новости-%s-' % codeProc

        #  временные рамки год-месяц-день-час-минута
        self.startDate = datetime(2030, 7, 24, 0, 0)
        self.endDate = datetime(2000, 4, 24, 0, 0)

        # delta
        self.df = pd.read_excel(self.deltaName)
        self.headerNameList = self.df['Заголовок'].tolist()
        self.deltaDateList = self.df['Дата создания'].tolist()

        self.mapDict = dict()
        self.feedDict = dict()

        # создаем датафрейм
        self.table = pd.DataFrame(
            columns=['uuid', 'Заголовок', 'Текст', 'Дата создания', 'Источник новости', 'Лента', 'Рубрика', 'Тэги',
                     'Файл приложения к новости - путь внутри архива с данным Excel файлом'])

        # dataFrame с новостями, которые выдали ошибку
        self.error = pd.DataFrame(columns=['href', 'data', 'source'])

        # dataFrame с новостями, где есть медиаэлементы
        self.frame = pd.DataFrame(columns=['href', 'Заголовок', 'Тег'])

        # num - нумерация для файлов
        self.num = num

        # номер папки
        self.dirCounter = dirCounter

        self.curDir = self.resDir + self.pathProc
        if not os.path.exists(self.curDir + str(self.dirCounter)):
            os.mkdir(self.curDir + str(self.dirCounter))
        if not os.path.exists(self.curDir + str(self.dirCounter) + '\\images'):
            os.mkdir(self.curDir + str(self.dirCounter) + '\\images')

        with open('map_%s.csv' % nameFolder, 'r', encoding='UTF-8') as file:
            readCSV = csv.reader(file, delimiter=';')
            for row in readCSV:
                self.mapDict[row[1]] = row[0]
                self.feedDict[row[1]] = row[2]

    def random_headers(self):
        return {'User-Agent': choice(self.desktop_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'}

    def new_session(self):
        session = requests.Session()
        session.headers = self.random_headers()
        # retry = Retry(connect=10, backoff_factor=0.5)
        # adapter = HTTPAdapter(max_retries=retry)
        # session.mount('http://', adapter)
        # session.mount('https://', adapter)
        return session

    def parsePages(self):
        anonses = list()
        for i in range(self.totalPages, self.endPages, -1):
            print("Номер страницы: %i" % i)
            try:
                anonses.extend(self.getNews(i))
            except:
                print("Страница %i завершилась с ошибкой. Перезапуск данной страницы." % i)
                anonses.extend(self.getNews(i))
        print('###########')
        print('Обработано страниц: %i, всего новостей: %i' % (self.totalPages - self.endPages, len(anonses)))
        print('###########')
        return anonses

    def getNews(self, i):
        anonses = list()
        if self.source == '':
            formula = str((i - 1) * 10)
        else:
            formula = str((i - 1) * 15)
        pageContent = self.new_session().get(self.url + self.urlToNews + formula, timeout=90)
        tree = BeautifulSoup(pageContent.content, features='lxml')

        if self.source == '':
            anonses.extend(tree.find('div', {'class': 'items-leading'}).find_all('div', {'class', re.compile("^leading")}))
        else:
            anonses.extend(tree.find('div', {'class': 'blog'}).find_all('div', {'class', 'column-1'}))
        pageContent.close()
        return anonses

    def selectionNews(self, anonses):
        href = ''
        newsDict = dict()
        # фильтруем и заносим в дикт

        for anons in anonses:
            try:
                dateStr = anons.find('span', {'class', 'create'}).get_text().replace('Создано: ', '').strip()
                date = datetime.strptime(dateStr.split(' ')[0], '%d.%m.%Y')

                header0 = anons.find('h2').find('a').get_text().strip()

                lenNews = len(self.df.loc[(self.df['Заголовок'].str.lower() == header0.lower()) &
                                          (self.df['Дата создания'].str.contains(dateStr.split(' ')[0]))])
                if (self.startDate >= date >= self.endDate) and lenNews == 0:
                    href = anons.find('h2').find('a').get('href')
                    dateNews = datetime.strptime(dateStr, '%d.%m.%Y %H:%M')
                    newsDict[href] = {'dateNews': dateNews}
            except:
                print("ERROR in " + href + " ")
        print('###########')
        print('Пришло новостей: %i, осталось для обработки: %i' % (len(anonses), len(newsDict)))
        print('###########')
        return newsDict

    def iterator(self, newsDict):
        # prevNews - ключ для предыдущей новости (нужно для сдвига даты)
        prevNews = list(newsDict)[0]
        for ref in newsDict:
            ln = len(self.table)
            if ln == 1000:
                self.table.to_excel(os.path.join(self.curDir + str(self.dirCounter), 'Новости.xlsx'), index=False,
                                    verbose=False, engine='xlsxwriter')
                self.table = pd.DataFrame(
                    columns=['uuid', 'Заголовок', 'Текст', 'Дата создания', 'Источник новости', 'Лента', 'Рубрика',
                             'Тэги',
                             'Файл приложения к новости - путь внутри архива с данным Excel файлом'])
                self.dirCounter += 1
                if not os.path.exists(self.curDir + str(self.dirCounter)):
                    os.mkdir(self.curDir + str(self.dirCounter))
                if not os.path.exists(self.curDir + str(self.dirCounter) + '\\images'):
                    os.mkdir(self.curDir + str(self.dirCounter) + '\\images')

            if newsDict[ref]['dateNews'].timestamp() == newsDict[prevNews]['dateNews'].timestamp():
                newsDict[ref]['dateNews'] = newsDict[prevNews]['dateNews'] - timedelta(minutes=1)
            publicDate = newsDict[ref]['dateNews'].strftime('%d.%m.%Y %H:%M')
            try:
                try:
                    newsPage = self.new_session().get(self.url + ref, timeout=90)
                except:
                    newsPage = self.new_session().get(self.url + ref, timeout=90)
                newsTree = BeautifulSoup(newsPage.content, features='lxml')
                print("========================")
                print("Обрабатывается: " + ref)

                try:
                    header = newsTree.find('div', {'class': 'item-page'}).find('h2').get_text().replace('\n', '').strip()
                except:
                    self.error.loc[len(self.error)] = [ref, newsDict[ref]['dateNews'], self.source]
                    header = '!!!ОШИБКА!!!'
                # парсим текст
                content = newsTree.find('div', {'class': 'item-page'})

                try:
                    content.find('span', {'class': 'create'}).decompose()
                except:
                    pass

                try:
                    content.find('h2').decompose()
                except:
                    pass

                try:
                    content.find('dd').decompose()
                except:
                    pass

                if content.find('iframe'):
                    content.find('iframe').decompose()
                    self.frame.loc[len(self.frame)] = [ref, header, 'iframe']
                if content.find('audio'):
                    content.find('audio').decompose()
                    self.frame.loc[len(self.frame)] = [ref, header, 'audio']
                if content.find('video'):
                    content.find('video').decompose()
                    self.frame.loc[len(self.frame)] = [ref, header, 'video']

                imgBlock = []
                try:
                    for img in content.find('div', {'class', 'sigplus-gallery'}).find_all('img'):
                        imgBlock.append({'src' : img.get('src')})
                    content.find('div', {'class', 'sigplus-gallery'}).decompose()
                except:
                    pass

                try:
                    fancyBox = content.find('ul', {'class': 'sigProContainer'}).find_all('li', {'class', 'sigProThumb'})
                    for img in fancyBox:
                        imgBlock.append({'src': img.find('a').get('href')})
                    content.find('ul', {'class': 'sigProContainer'}).decompose()
                except:
                    pass

                try:
                    content.find('h1').decompose()
                except:
                    pass

                text = ''
                for child in content.children:
                    if str(child).strip() != "":
                        try:
                            if child.get_text().strip() != "":
                                text += str(child).strip()
                        except:
                            pass

                prevNews = ref
                publicDate = newsDict[ref]['dateNews'].strftime('%d.%m.%Y %H:%M')

                # pics
                pics = content.find_all('img')

                if len(imgBlock) != 0:
                    pics.extend(imgBlock)

                newsPics = ''
                if len(pics) != 0:
                    print('КАРТИНКИ!!!!!!!!!!!!!!!!!!')
                    for pic in pics:
                        link = pic['src']
                        try:
                            if link.startswith('http') or link.startswith('https'):
                                tmp = link
                            elif link.startswith(' '):
                                link = link.replace(' ', '')
                            else:
                                tmp = self.url + link

                            img = self.new_session().get(tmp, timeout=90, verify=False, headers=self.random_headers())
                            imgDir = self.curDir + str(self.dirCounter) + '\\images'
                            with open(os.path.join(imgDir, 'pic_' + str(self.num) + '.jpg'), 'wb') as im:
                                im.write(img.content)
                                if len(newsPics) != 0:
                                    newsPics += '|||'
                                newsPics += 'images\\' + 'pic_' + str(self.num) + '.jpg'
                                im.close()
                            self.num += 1
                            img.close()
                        except:
                            print('Unable to load ' + str(pic))

                # uuid
                uid = uuid.uuid1()

                feed = self.feedDict[self.source]
                source = self.mapDict[self.source]

                if (len(header) <= 400):
                    # лист для аппенда
                    newRow = [str(uid), header, text, str(publicDate), source, feed, '', '', newsPics]
                    self.table.loc[ln] = newRow
                else:
                    print("ДЛИННЫЙ ЗАГОЛОВОК, НЕ СОХРАНЕНА")

                print('UUID: ' + str(uid))
                print("Заголовок: " + header)
                print("Дата: " + str(publicDate))
                print(text)
                print("Источник: " + source)
                print("Лента: " + feed)
                print("Картинки: " + newsPics)
                print(ln)
                newsPage.close()
            except None:
                self.error.loc[len(self.error)] = [ref, newsDict[ref]['dateNews'], self.source]
                print("новость %s вернула ошибку" % ref)

        self.saveReport()

    def saveReport(self):
        self.table.to_excel(os.path.join(self.curDir + str(self.dirCounter), 'Новости.xlsx'), index=False,
                            verbose=False, engine='openpyxl')
        self.error.to_excel(os.path.join(self.curDir + str(self.dirCounter), 'Ошибки.xlsx'), index=False, verbose=False,
                            engine='openpyxl')
        self.frame.to_excel(os.path.join(self.curDir + str(self.dirCounter), 'Страницы с iframe, audio, video.xlsx'),
                            index=False,
                            verbose=False, engine='openpyxl')
