import sqlite3
import uuid

from ParseProc import ParseProc

#554 - всего у главной
#totalPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter):
#parseProc = ParseProc(1, 0, 'https://bashprok.ru', '/news/?PAGEN_1=', 'bashprok', '02', '2699603', 0, 1)
from ParseProcDistricts import ParseProcDistricts

#последний параметр - номер района
parseProc = ParseProcDistricts('bashprok', '02', '2699603', 13)

anonses = parseProc.parsePages()
newsDict = parseProc.selectionNews(anonses)
parseProc.iterator(newsDict)