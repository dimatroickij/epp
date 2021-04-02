import sqlite3
import uuid

from ParseProc import ParseProc

conn = sqlite3.connect("chechproc.db")
cursor = conn.cursor()

#totalPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter):
parseProc = ParseProc(804, 0, 'http://chechproc.ru', '/news/page-', 'chechproc', '20', '3385500', 450, 6)

anonses = parseProc.parsePages()
# zz = parseProc.totalPages
# for anons in anonses:
#     cursor.execute("""INSERT INTO anonses
#                   VALUES ('%s', %i, '%s')""" % (anons, zz, uuid.uuid1()))
#     zz -= 1
#     conn.commit()

newsDict = parseProc.selectionNews(anonses)

parseProc.iterator(newsDict)