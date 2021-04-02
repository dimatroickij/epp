import csv
import os
import re
import uuid
from random import choice
from datetime import datetime, timedelta

import numpy
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
        self.startDate = datetime(2020, 5, 28, 23, 00)
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

        # мап месяцов
        self.monDict = dict()
        with open('month_map.csv', 'r', encoding='UTF-8') as file1:
            readCSV = csv.reader(file1, delimiter=';')
            for row in readCSV:
                self.monDict[row[0]] = row[1]

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
        body = {'view_name': 'nt_news',
                'view_display_id': self.source,
                'page': str(i)}
        pageContent = self.new_session().post(self.url + self.urlToNews, data=body, timeout=90)
        tree = BeautifulSoup(pageContent.json()[1]['data'], features='lxml')
        anonses.extend(tree.find_all('div', {'class': 'views-row'}))
        pageContent.close()
        return anonses

    def selectionNews(self, anonses):
        href = ''
        newsDict = dict()
        # фильтруем и заносим в дикт

        for anons in anonses:
            # try:
            dateStr = anons.find('p', {'class', 'main-slider__date'}).get_text().replace(',', '').strip()
            monName = re.sub(r'\d+', '', dateStr).replace(':', '').strip()
            dateStr = dateStr.replace(' ' + monName + ' ', '.' + self.monDict[monName] + '.')

            date = datetime.strptime(dateStr.split(' ')[0], '%d.%m.%Y')

            header0 = anons.find('h2').find('a').get_text().strip()
            lenNews = len(self.df.loc[(self.df['Заголовок'].str.lower() == header0.lower()) &
                                      (self.df['Дата создания'].str.contains(dateStr.split(' ')[0]))])
            if (self.startDate >= date >= self.endDate) and lenNews == 0:
                href = anons.find('h2').find('a').get('href')

                dateNews = datetime.strptime(dateStr, '%d.%m.%Y %H:%M')
                newsDict[href] = {'dateNews': dateNews}
        # except:
        #    print("ERROR in " + href + " ")
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

                # парсим текст
                content = newsTree.find('div', {'class': 'row blNewsNode'})

                header = content.find('span', {'class': 'main-slider__title'}).get_text().replace('\n', '').strip()

                if content.find('iframe'):
                    content.find('iframe').decompose()
                    self.frame.loc[len(self.frame)] = [ref, header, 'iframe']
                if content.find('audio'):
                    content.find('audio').decompose()
                    self.frame.loc[len(self.frame)] = [ref, header, 'audio']
                if content.find('video'):
                    content.find('video').decompose()
                    self.frame.loc[len(self.frame)] = [ref, header, 'video']

                text = ''
                try:
                    for child in content.find('div', {'class': 'blBody'}).find('div', {'class', 'field-item even'}).children:
                        if str(child).strip() != "":
                            try:
                                if child.get_text().strip() != "":
                                    text += str(child).strip()
                            except:
                                pass
                except:
                    pass

                prevNews = ref
                publicDate = newsDict[ref]['dateNews'].strftime('%d.%m.%Y %H:%M')

                # pics
                pics = content.find_all('img')

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

                if self.source == 'page_1':
                    sourceText = ''
                else:
                    sourceText = ''
                    block = newsTree.find('span', {'class', 'news-news-article__date'})
                    block.find('span', {'class', 'blue'}).decompose()
                    for source in block.get_text().strip().split('|'):
                        if self.feedDict.get(source.strip()) is not None:
                            sourceText = source.strip()

                feed = self.feedDict[sourceText]
                source = self.mapDict[sourceText]

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
            except:
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

    def loadErrorNews(self):
        errorNews = pd.read_excel(self.resDir + '\\error.xlsx')

        for index, row in errorNews.iterrows():
            ln = len(self.table)
            if row['source'] is numpy.nan or len(row['source']) == 0:
                row['source'] = ''
            else:
                row['source'] = row['source']
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

            publicDate = errorNews.loc[index]['data'].strftime('%d.%m.%Y %H:%M')
            try:
                try:
                    newsPage = self.new_session().get(self.url + errorNews.loc[index]['href'], timeout=90)
                except:
                    newsPage = self.new_session().get(self.url + errorNews.loc[index]['href'], timeout=90)
                newsTree = BeautifulSoup(newsPage.content, features='lxml')
                print("========================")
                print("Обрабатывается: " + row['href'])

                # парсим текст
                content = newsTree.find('div', {'class': 'row blNewsNode'})

                header = content.find('span', {'class': 'main-slider__title'}).get_text().replace('\n', '').strip()

                if content.find('iframe'):
                    content.find('iframe').decompose()
                    self.frame.loc[len(self.frame)] = [errorNews.loc[index]['href'], header, 'iframe']
                if content.find('audio'):
                    content.find('audio').decompose()
                    self.frame.loc[len(self.frame)] = [errorNews.loc[index]['href'], header, 'audio']
                if content.find('video'):
                    content.find('video').decompose()
                    self.frame.loc[len(self.frame)] = [errorNews.loc[index]['href'], header, 'video']

                text = ''
                try:
                    for child in content.find('div', {'class': 'blBody'}).find('div',
                                                                               {'class', 'field-item even'}).children:
                        if str(child).strip() != "":
                            try:
                                if child.get_text().strip() != "":
                                    text += str(child).strip()
                            except:
                                pass
                except:
                    pass

                prevNews = errorNews.loc[index]['href']
                publicDate = errorNews.loc[index]['data'].strftime('%d.%m.%Y %H:%M')

                # pics
                pics = content.find_all('img')

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

                if self.source == 'page_1':
                    sourceText = ''
                else:
                    sourceText = ''
                    block = newsTree.find('span', {'class', 'news-news-article__date'})
                    try:
                        block.find('span', {'class', 'blue'}).decompose()
                    except:
                        pass
                    for source in block.get_text().strip().split('|'):
                        if self.feedDict.get(source.strip()) is not None:
                            sourceText = source.strip()

                feed = self.feedDict[sourceText]
                source = self.mapDict[sourceText]

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
                self.error.loc[len(self.error)] = [errorNews.loc[index]['href'], errorNews.loc[index]['data'], self.source]
                print("новость %s вернула ошибку" % row['href'])

        self.saveReport()