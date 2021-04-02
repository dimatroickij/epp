import csv
import os
import re
import uuid
from random import choice
from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup


class ParseProcDistricts:

    def __init__(self, nameFolder, numberProc, codeProc, dirCounter):

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

        self.resDir = 'F:\\parseProc\\proc\\%s' % nameFolder
        self.picDir = 'F:\\parseProc\\proc\\%s\\images' % nameFolder
        self.deltaName = os.getcwd() + '\\epp_parse\\fromEppNews_proc_%s.xlsx' % numberProc

        self.pathProc = '\\Новости-%s-' % codeProc

        #  временные рамки год-месяц-день-час-минута
        self.startDate = datetime(2020, 5, 28, 0, 0)
        self.endDate = datetime(2000, 4, 24, 0, 0)

        # delta
        self.df = pd.read_excel(self.deltaName)
        self.headerNameList = self.df['Заголовок'].tolist()
        self.deltaDateList = self.df['Дата создания'].tolist()

        self.mapDict = dict()
        self.feedDict = dict()
        self.urlProcDict = dict()

        # создаем датафрейм
        self.table = pd.DataFrame(
            columns=['uuid', 'Заголовок', 'Текст', 'Дата создания', 'Источник новости', 'Лента', 'Рубрика', 'Тэги',
                     'Файл приложения к новости - путь внутри архива с данным Excel файлом'])

        # dataFrame с новостями, которые выдали ошибку
        self.error = pd.DataFrame(columns=['href', 'data', 'proc'])

        # dataFrame с новостями, где есть медиаэлементы
        self.frame = pd.DataFrame(columns=['href', 'Заголовок', 'Тег'])

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

        with open('map_districts.csv', 'r', encoding='UTF-8') as file:
            readCSV = csv.reader(file, delimiter=';')
            for row in readCSV:
                self.urlProcDict[row[2]] = {'url': row[0], 'path': row[1]}

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
        for proc in self.urlProcDict:
            try:
                anonses.append(self.getNews(proc))
            except:
                print("Страница %s завершилась с ошибкой. Перезапуск данной страницы." % self.urlProcDict[proc])
                anonses.append(self.getNews(proc))
        print('###########')
        print('Всего новостей: %i' % sum(map(lambda x: len(x[0]), anonses)))
        print('###########')
        return anonses

    def getNews(self, i):
        anonses = list()
        pageContent = self.new_session().get(self.urlProcDict[i]['url'] + self.urlProcDict[i]['path'], timeout=90)
        tree = BeautifulSoup(pageContent.content, features='lxml')
        listAnonses = list()
        listAnonses.extend(tree.find('div', {'class': 'news-list'}).find_all('p', {'class': 'news-item'}))
        anonses = [listAnonses, i]
        pageContent.close()
        print('%s) Обработана: %s, всего новостей: %i' % (i, self.urlProcDict[i]['url'], len(anonses[0])))
        return anonses

    def selectionNews(self, anonses):
        href = ''
        newsDict = dict()
        # фильтруем и заносим в дикт
        for anonsProc in anonses:
            for anons in anonsProc[0]:
                try:
                    dateStr = anons.find('small').get_text().replace('Дата создания:', '').strip()
                    date = datetime.strptime(dateStr, '%d.%m.%Y %H:%M:%S')
                    header0 = anons.find('a').get_text().strip()

                    lenNews = len(self.df.loc[(self.df['Заголовок'].str.lower() == header0.lower())])
                    if (self.startDate >= date >= self.endDate) and lenNews == 0:
                        href = anons.find('a').get('href')
                        dateNews = date
                        newsDict[href] = {'dateNews': dateNews, 'header': header0, 'proc': anonsProc[1], 'urlProc': self.urlProcDict[anonsProc[1]]['url']}
                except:
                    print("ERROR in " + href + " ")
        print('###########')
        print('Пришло новостей: %i, осталось для обработки: %i' % (sum(map(lambda x: len(x[0]), anonses)), len(newsDict)))
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

            # if newsDict[ref]['dateNews'].date() == newsDict[prevNews]['dateNews'].date():
            #     newsDict[ref]['dateNews'] = newsDict[prevNews]['dateNews'] - timedelta(minutes=10)
            publicDate = newsDict[ref]['dateNews'].strftime('%d.%m.%Y %H:%M')
            try:
                try:
                    newsPage = self.new_session().get(newsDict[ref]['urlProc'] + '/news/' + ref, timeout=90)
                except:
                    newsPage = self.new_session().get(newsDict[ref]['urlProc'] + '/news/' + ref, timeout=90)
                newsTree = BeautifulSoup(newsPage.content, features='lxml')
                print("========================")
                print("Обрабатывается: " + ref)

                header = newsDict[ref]['header']
                # print(header)

                # парсим текст
                content = newsTree.find('div', {'class': 'news-detail'})

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
                    content.find('span', {'class', 'news-date-time'}).decompose()
                except:
                    pass
                try:
                    content.find('span').decompose()
                except:
                    pass

                for child in content.children:
                    if str(child).strip() != "":
                        try:
                            if child.get_text().strip() != "":
                                text += str(child).strip()
                        except:
                            text += str(child).strip()

                prevNews = ref
                publicDate = newsDict[ref]['dateNews'].strftime('%d.%m.%Y %H:%M')

                # pics
                pics = newsTree.find('div', {'class', 'content-wrap'}).find_all('img')
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
                                tmp = newsDict[ref]['urlProc'] + link

                            img = self.new_session().get(tmp, timeout=90, verify=False, headers=self.random_headers())
                            imgDir = self.curDir + str(self.dirCounter) + '\\images'
                            with open(os.path.join(imgDir, str(uuid.uuid1()) + '.jpg'), 'wb') as im:
                                im.write(img.content)
                                if len(newsPics) != 0:
                                    newsPics += '|||'
                                newsPics += '/images/' + str(uuid.uuid1()) + '.jpg'
                                im.close()
                            img.close()
                        except:
                            print('Unable to load ' + str(pic))

                # uuid
                uid = uuid.uuid1()

                feed = self.feedDict[newsDict[ref]['proc']]
                source = self.mapDict[newsDict[ref]['proc']]

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
                self.error.loc[len(self.error)] = [ref, newsDict[ref]['dateNews'], newsDict[ref]['proc']]
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
