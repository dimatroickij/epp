import sqlite3
import uuid

from ParseProc import ParseProc

#всего 273 стриницы
#totalPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter):
parseProc = ParseProc(273, 0, 'http://www.procuror-osetia.ru', '/news/P', 'osetia', '15', '3354131', 0, 1)
anonses = parseProc.parsePages()
newsDict = parseProc.selectionNews(anonses)
parseProc.iterator(newsDict)