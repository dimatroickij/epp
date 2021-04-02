import sqlite3
import uuid

from ParseProc import ParseProc

#всего 1222 страницы, нумерация с 0
#totalPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter):
parseProc = ParseProc(1, -1, 'http://www.ulproc.ru', '/news?page=', 'ulyanovsk', '73', '2828794', 913, 26)
#anonses = parseProc.parsePages()
#newsDict = parseProc.selectionNews(anonses)
#parseProc.iterator(newsDict)
parseProc.loadErrorNews()