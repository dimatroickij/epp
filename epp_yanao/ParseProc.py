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

    def __init__(self, totalPages, endPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter):
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
        self.error = pd.DataFrame(columns=['href', 'date'])

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
        anonseHeader = list()
        anonseDate = list()
        for i in range(self.totalPages, self.endPages, -1):
            print("Номер страницы: %i" % i)
            try:
                anonseDate.extend(self.getNews(i)[0])
                anonseHeader.extend(self.getNews(i)[1])
            except:
                print("Страница %i завершилась с ошибкой. Перезапуск данной страницы." % i)
                anonseDate.extend(self.getNews(i)[0])
                anonseHeader.extend(self.getNews(i)[1])
        print('###########')
        print('Обработано страниц: %i, всего новостей: %i' % (self.totalPages - self.endPages, len(anonseHeader)))
        print('###########')
        return [anonseDate, anonseHeader]

    def getNews(self, i):
        anonseHeader = list()
        anonseDate = list()
        body = []
        pageContent = self.new_session().get(self.url + self.urlToNews + str(i) + '&SIZEN_1=20', timeout=90)
        tree = BeautifulSoup(pageContent.content, features='lxml')
        tree.find('div', {'class', 'formatName newsDate'}).decompose()
        anonseDate.extend(tree.find_all('div', {'class', 'formatDate'}))
        anonseHeader.extend(tree.find_all('div', {'class', 'formatName'}))
        pageContent.close()
        return [anonseDate, anonseHeader]

    def selectionNews(self, anonses):
        href = ''
        newsDict = dict()
        # фильтруем и заносим в дикт
        anonsesDate = anonses[0]
        anonsesHeader = anonses[1]
        for i in range(0, len(anonsesDate)):
            try:
                dateStr = anonsesDate[i].get_text().strip().lower().strip()
                monName = re.sub(r'\d+', '', dateStr.split(', ')[0]).strip()
                dateStr = dateStr.replace(' ' + monName + ' ', '.' + self.monDict[monName] + '.').replace(', ', ' ')
                date = datetime.strptime(dateStr, '%d.%m.%Y %H:%M:%S')
                header0 = anonsesHeader[i].find('a').get_text().strip()
                lenNews = len(self.df.loc[(self.df['Заголовок'].str.lower() == header0.lower())])
                if (self.startDate >= date >= self.endDate) and lenNews == 0:
                    href = anonsesHeader[i].find('a').get('href')
                    dateNews = date
                    newsDict[href] = {'dateNews': dateNews, 'header': header0}
            except:
                print("ERROR in " + href + " ")
        print('###########')
        print('Пришло новостей: %i, осталось для обработки: %i' % (len(anonses[0]), len(newsDict)))
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

            #if newsDict[ref]['dateNews'].timestamp() == newsDict[prevNews]['dateNews'].timestamp():
            #    newsDict[ref]['dateNews'] = newsDict[prevNews]['dateNews'] - timedelta(minutes=1)
            publicDate = newsDict[ref]['dateNews'].strftime('%d.%m.%Y %H:%M')
            print(publicDate)
            try:
                try:
                    newsPage = self.new_session().get(self.url + ref, timeout=90)
                except:
                    newsPage = self.new_session().get(self.url + ref, timeout=90)
                newsTree = BeautifulSoup(newsPage.content, features='lxml').find('div', {'class', 'news-detail'})
                print("========================")
                print("Обрабатывается: " + ref)

                header = newsDict[ref]['header']
                print(header)
                if newsTree.find('iframe'):
                    newsTree.find('iframe').decompose()
                    self.frame.loc[len(self.frame)] = [ref, header, 'iframe']
                if newsTree.find('audio'):
                    newsTree.find('audio').decompose()
                    self.frame.loc[len(self.frame)] = [ref, header, 'audio']
                if newsTree.find('video'):
                    newsTree.find('video').decompose()
                    self.frame.loc[len(self.frame)] = [ref, header, 'video']

                # парсим текст
                text = ''
                for child in newsTree.find('div', {'class', 'formatContent'}).children:
                    if str(child).strip() != "":
                        try:
                            if child.get_text().strip() != "":
                                text += str(child).strip()
                        except:
                            text += str(child).strip()

                prevNews = ref
                publicDate = newsDict[ref]['dateNews'].strftime('%d.%m.%Y %H:%M')

                # pics
                pics = newsTree.find_all('img')
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
                                newsPics += '/images/' + 'pic_' + str(self.num) + '.jpg'
                                im.close()
                            self.num += 1
                            img.close()
                        except:
                            print('Unable to load ' + str(pic))

                # uuid
                uid = uuid.uuid1()

                feed = self.feedDict['']
                source = self.mapDict['']

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
                self.error.loc[len(self.error)] = [ref, newsDict[ref]['dateNews']]
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
