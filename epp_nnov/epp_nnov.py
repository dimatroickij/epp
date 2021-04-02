import sqlite3
import uuid

from ParseProc import ParseProc

#611 - всего, но нумерация от 0 до 610
#totalPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter):
parseProc = ParseProc(610, -1, 'http://proc-nn.ru/ru', '/news/?p=', 'nnov', '52', '2602268', 0, 1)
anonses = parseProc.parsePages()
newsDict = parseProc.selectionNews(anonses)
parseProc.iterator(newsDict)