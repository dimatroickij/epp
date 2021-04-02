import sqlite3
import uuid

from ParseProc import ParseProc

conn = sqlite3.connect("chechproc.db")
cursor = conn.cursor()

#363 - всего
#totalPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter):
parseProc = ParseProc(364, 0, 'https://kamprok.ru', '/category/novosti/page/', 'kamchatka', '41', '1581711', 0, 1)
anonses = parseProc.parsePages()
newsDict = parseProc.selectionNews(anonses)
parseProc.iterator(newsDict)