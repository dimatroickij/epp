import sqlite3
import uuid

from ParseProc import ParseProc

#471 - всего, формула страницы: (i-1)*10
#общая прокуратура: /news?start=
#Элиста : /novosti-gorodovikovskogo-rajona?start=
# /iki-burulskij-rajon
#Кетченеровский район: ketchenerovskij-rajon
#laganskij-rajon
#maloderbetovskij-rajon
#oktyabrskij-rajon
#priyutnenskij-rajon
#sarpinskij-rajon
#/tselinnyj-rajon
#chernozemelskij-rajon
#yustinskij-rajon
#yashaltinskij-rajon
#yashkulskij-rajon
#totalPages, url, urlToNews, nameFolder, numberProc, codeProc, num, dirCounter, source):
parseProc = ParseProc(271, 270, 'http://www.kalmprok.ru', '/news?start=', 'kalm', '08', '3242507', 1498, 8, '')
anonses = parseProc.parsePages()
newsDict = parseProc.selectionNews(anonses)
parseProc.iterator(newsDict)