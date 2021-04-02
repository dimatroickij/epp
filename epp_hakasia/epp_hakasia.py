import sqlite3
import uuid

from ParseProc import ParseProc

#всего 306 стриницы
#totalPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter):
parseProc = ParseProc(306, 0, 'http://www.prokrh.ru', '/novosti/?PAGEN_1=', 'hakasia', '19', '3376805', 0, 1)
anonses = parseProc.parsePages()
newsDict = parseProc.selectionNews(anonses)
parseProc.iterator(newsDict)