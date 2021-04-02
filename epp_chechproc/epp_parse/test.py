import lxml.html
from lxml.etree import XPath
url = "http://gbgfotboll.se/information/?scr=table&ftid=51168"
date = '2014-09-27'

rows_xpath = XPath("//*[@id='content-primary']/table[3]/tbody/tr[td[1]/span/span//text()='%s']" % (date))
time_xpath = XPath("td[1]/span/span//text()[2]")
team_xpath = XPath("td[2]/a/text()")

html = lxml.html.parse(url)

for row in rows_xpath(html):
    time = time_xpath(row)[0].strip()
    team = team_xpath(row)[0]
    print (time)
