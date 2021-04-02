import sqlite3
import uuid

from ParseProc import ParseProc
from ParseProcDistricts import ParseProcDistricts

#всего 452, но нумерация с 0
#есть прокуратуры районов на отдельной странице c 1030, но нумерация с 0
#totalPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter):
#parseProc = ParseProc(1, -1, 'http://www.prokurorhbr.ru', '/%D0%BD%D0%BE%D0%B2%D0%BE%D1%81%D1%82%D0%B8?page=0%2C0%2C0%2C0%2C0%2C0%2C', 'hbr', '27', '3709791', 0, 1)
parseProc = ParseProcDistricts(1029, -1, 'http://www.prokurorhbr.ru', '/newsgorod?page=', 'hbr', '27', '3709791', 646, 6)
anonses = parseProc.parsePages()
newsDict = parseProc.selectionNews(anonses)
parseProc.iterator(newsDict)