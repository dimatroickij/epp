import sqlite3
import uuid

from ParseProc import ParseProc

#всего 583
#totalPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter):
parseProc = ParseProc(583, 0, 'http://www.prokyanao.ru', '/news/?PAGEN_1=', 'yanao', '89', '2757432', 0, 1)
anonses = parseProc.parsePages()
newsDict = parseProc.selectionNews(anonses)
parseProc.iterator(newsDict)