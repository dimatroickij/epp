import sqlite3
import uuid

from ParseProc import ParseProc

#главная прокуратура - 48, нумерация с 0
#районы - 142, нумерация с 0
#totalPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter, source): source: 2 - районы, 1 - главная
parseProc = ParseProc(141, -1, 'http://rkproc.ru', '/views/ajax', 'krym', '91', '3269663', 1063, 8, 'page_2')
#anonses = parseProc.parsePages()
#newsDict = parseProc.selectionNews(anonses)
#parseProc.iterator(newsDict)
parseProc.loadErrorNews()