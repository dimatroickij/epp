import sqlite3
import uuid

from ParseProc import ParseProc

#1055 - всего, но нумерация от 0 до 1054
#totalPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter):
parseProc = ParseProc(1054, -1, 'http://www.adygproc.ru', '/smi/news?page=', 'adyg', '01', '3193154', 0, 1)
anonses = parseProc.parsePages()
newsDict = parseProc.selectionNews(anonses)
parseProc.iterator(newsDict)