import re

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandas import ExcelWriter
from requests.auth import HTTPBasicAuth

from tqdm import tqdm

number_epp = ['89']
url = 'mass-media/news/archive?p_p_id=FeedsPortlet_INSTANCE_XrdDLKA9Uj6O&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&_FeedsPortlet_INSTANCE_XrdDLKA9Uj6O_delta=10&_FeedsPortlet_INSTANCE_XrdDLKA9Uj6O_resetCur=false&_FeedsPortlet_INSTANCE_XrdDLKA9Uj6O_cur='
month = {}
month['июня'] = '06'
month['июля'] = '07'
month['мая'] = '05'
month['марта'] = '03'
month['апреля'] = '04'
month['февраля'] = '02'
month['января'] = '01'
month['декабря'] = '12'
month['ноября'] = '11'
month['августа'] = '08'
month['сентября'] = '09'
month['октября'] = '10'

for num_proc in tqdm(number_epp):
    url_auth = "https://epp.genproc.gov.ru/"
    baseUrl = url_auth + 'web/'

    url_proc = baseUrl + 'proc_' + str(num_proc) + '/'
    mass_headers = []
    mass_date = []
    numberOfPageNews = 1
    path_news = url + str(numberOfPageNews)
    urlPageProc = url_proc + path_news
    page = requests.get(urlPageProc, auth=HTTPBasicAuth('login', 'password'))
    soup = BeautifulSoup(page.text + "news", 'html.parser')
    numPage = soup.find("ul", {'class': 'pagination'})
    max_m = 0
    for i in numPage.findAll("li"):
        tag = i.text.strip()
        try:
            max_m = int(tag) + 1
        except:
            continue
    for n in tqdm(range(1, max_m)):
        numberOfPageNews = n
        path_news = url + str(numberOfPageNews)
        urlPageProc = url_proc + path_news

        page = requests.get(urlPageProc, auth=HTTPBasicAuth('login', 'password'))
        soup = BeautifulSoup(page.text + "news", 'html.parser')
        for divs in soup.findAll("div", attrs={'class': 'proc__news__text'}):
            tag_a = divs.find("a")
            header_text = tag_a.text.strip()
            mass_headers.append(header_text)
        for divs in soup.findAll("span", attrs={'class': 'proc__news__category'}):
            dateStr = divs.get_text()
            monName = re.sub(r'\d+', '', dateStr.split(',')[0]).strip()
            dateStr = dateStr.replace(' ' + monName + ' ', '.' + month[monName] + '.')
            mass_date.append(dateStr.replace(',', ''))
    df = pd.DataFrame({'Заголовок': mass_headers, 'Дата создания': mass_date})
    with ExcelWriter("fromEppNews_" + 'proc_' + str(num_proc) + '.xlsx') as writer:
        df.to_excel(writer)
