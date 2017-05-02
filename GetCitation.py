# encoding:utf-8
# 爬取引文信息
import MySQLdb
import MySQLdb.cursors
import requests
from bs4 import BeautifulSoup
from bs4 import NavigableString
from time import sleep
import re
import random
import sys

db = MySQLdb.connect(host='shhr.online', user='jingfei', port=33755, passwd='hanjingfei007', db='test_citation',
                     charset='utf8')
cursor = db.cursor()

# 镜像headers
headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch, br',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Connection': 'keep-alive',
    'Host': 'b.ggkai.men',
    'Referer': 'https://b.ggkai.men/extdomains/scholar.google.com/schhp?hl=en&num=20&as_sdt=0',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.36',
    'Cache-Control': 'no-cache',
    'Cookie': 'NID=102=kTgVWnf69NtGxvJomkeHQ_UKDGRhHSiCF-L0z6KG-YggDp3p_ZODpzBfvvKJCiwGKuLYW3Gvl7Ft1W51RvzmvW6RSYvB_R2UkYstMPUSIYOo6GIT5bbKiK0dlN1XT6sX; GSP=LM=1493101829:S=9mWglRPfw-XEsULC; Hm_lvt_df11358c4b6a37507eca01dfe919e040=1493101830,1493121092,1493121120; Hm_lpvt_df11358c4b6a37507eca01dfe919e040=1493709352',
}

index = 0

sql_cnt = "SELECT COUNT(*) FROM test_paper_shi WHERE paper_nbCitation is NULL"
'''
#sql_cnt = "SELECT COUNT(*) FROM paper\
			WHERE venue_venue_id IN(\
				SELECT venue_id FROM venue\
				WHERE dblp_dblp_id IN(\
					SELECT dblp_id\
					FROM ccf, dblp\
					WHERE CCF_dblpname = dblp_name\
					AND CCF_type = '%s'\
					AND CCF_classification = '%s'\
				)\
			)" % (CCF_type, CCF_classification)
'''


cursor.execute(sql_cnt)
total = cursor.fetchone()[0]  # 总共的论文数量

'''
cursor.execute(sql_cnt_index)
index = cursor.fetchone()[0]
index += 1  # 当前条数
'''

info_cnt = "Total: %5d" % (total)
print info_cnt
# fp.write(info_print + "\n")
# fp.write(info_cnt + "\n")

# sql_select = "SELECT paper_id, paper_title\
#			FROM citation.paper WHERE paper_nbCitation =-1"

sql_select = "SELECT paper_id, paper_title, paper_citationURL FROM test_paper_shi WHERE has_get_citation is NULL AND paper_citationURL <> '' limit 100"

cursor.execute(sql_select)
res_set = cursor.fetchall()

url = "https://e.ggkai.men"
url1 = "https://g.zmirrordemo.com"


def warnInfo(string):
    # with open("venue_log.txt","a") as fp:
    #	fp.write(string+'\n')
    print string


class extractCitation(object):
    def __init__(self, url, headers, paper_title, paper_id):
        # print "__init__"
        self.url = url
        self.headers = headers
        self.paper_title = paper_title
        self.paper_id = paper_id
        self.start = 0
        self.is_end = 0
        self.nbCitation = 0
        self.sql = ""
        self.sql_cnt = 0

    def _requestWeb(self):
        # print "_requestWeb"
        cnt_res = 1
        print "page %d OK " % (self.start/20)
        query_url = self.url+"&start="+str(self.start)+"&num=20"
        while cnt_res <= 5:
            # print "VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV"
            try:
                response = requests.get(query_url, headers=self.headers, timeout=10)
                return response
            except:
                cnt_res += 1
                continue
                raise Exception  # 如果链接失败，则抛出异常，被调用函数捕获

    def _parseGoogle(self, response):
        # 解析google页面
        targetPaper_scholarTitle = ""
        targetPaper_scholarInfo = ""
        targetPaper_citationURL = ""
        targetPaper_pdfURL = ""
        targetPaper_rawInfo = ""
        targetPaper_rawLink = ""
        targetPaper_nbCitation = -1
        targetpaper_publicationYear = -1
        targetPaper_isseen = -1
        paper_paper_id = self.paper_id

        soup = BeautifulSoup(response.text, "html.parser")

        # 获取总引用数
        if self.start == 0:
            self.nbCitation = int(soup.find("div", attrs={"id": "gs_ab_md"}).get_text().strip('About ').split(' ',1)[0])

        # 获取
        for paper in soup.find_all("div", attrs={"class": "gs_r"}):
            targetPaper_scholarTitle = ""
            targetPaper_scholarInfo = ""
            targetPaper_citationURL = ""
            targetPaper_pdfURL = ""
            targetPaper_rawInfo = ""
            targetPaper_rawLink = ""
            targetPaper_nbCitation = -1
            targetpaper_publicationYear = -1
            targetPaper_isseen = -1
            paper_paper_id = self.paper_id

            title = paper.find("h3", attrs={"class": "gs_rt"})
            # 有些引文没有链接
            try:
                targetPaper_scholarTitle = title.a.text
                targetPaper_rawLink = title.a['href']
                targetPaper_scholarInfo = paper.find("div", attrs={"class": "gs_a"}).get_text()
                # 获取年份数据
                targetpaper_publicationYear = int(targetPaper_scholarInfo.split(" - ",3)[1][-4:])
                targetPaper_rawInfo = unicode(paper.find("div", attrs={"class": "gs_rs"}).get_text())
            except:
                for c in title.children:
                    if isinstance(c, NavigableString):
                        targetPaper_scholarTitle += unicode(c)
                targetPaper_scholarInfo = paper.find("div", attrs={"class": "gs_a"}).get_text()

            try:
                link_raw = paper.find("a", text=re.compile("Cited"))
                link = link_raw.get_text()
                targetPaper_citationURL = link_raw.get('href')  # 获得引用链接
                targetPaper_nbCitation = int(link.strip('Cited by'))  # 获得引用数
            except:
                targetPaper_nbCitation = 0  # 没有找到Cited by， 则引用数为0

            try:
                paper.find("span", attrs={"class": "gs_ctg2"}).get_text()  # 找到[pdf]
                link_pdf = paper.find("div", attrs={"class": "gs_ggsd"})
                targetPaper_pdfURL = link_pdf.a['href']  # 获得pdf链接
                targetPaper_isseen = 1
            except:
                targetPaper_isseen = 0  # 没有找到[pdf],则pdf不可得到

            self.addSQL(targetPaper_scholarTitle, targetPaper_scholarInfo, targetPaper_citationURL,
                                 targetPaper_pdfURL, targetPaper_rawLink, targetPaper_rawInfo,
                                 targetpaper_publicationYear, targetPaper_nbCitation, targetPaper_isseen,
                                 paper_paper_id)

    def addSQL(self, targetPaper_scholarTitle, targetPaper_scholarInfo, targetPaper_citationURL, targetPaper_pdfURL, targetPaper_rawLink, targetPaper_rawInfo, targetpaper_publicationYear, targetPaper_nbCitation, targetPaper_isseen, paper_paper_id):
        # 无论解析阶段是否出现异常都写入数据库
        if self.sql_cnt != 0:
            self.sql += ","
        self.sql += "('%s', '%s', '%s', '%s', '%s', '%s', '%d', '%d', '%d', '%d')" % (
                         targetPaper_scholarTitle.replace('\'', '\\\'').strip(), targetPaper_scholarInfo.replace('\'', '\\\'').strip(),
                         targetPaper_citationURL.replace('\'', '\\\'').strip(),
                         targetPaper_pdfURL.replace('\'', '\\\'').strip(),
                         targetPaper_rawLink.replace('\'', '\\\'').strip(),
                         targetPaper_rawInfo.replace('\'', '\\\'').strip(), targetpaper_publicationYear,
                         targetPaper_nbCitation, targetPaper_isseen, paper_paper_id)
        self.sql_cnt += 1
        if self.sql_cnt >= 100:
            self.executeDataBase()

    def executeDataBase(self):
        # 统一写入数据库
        if self.sql_cnt == 0:
            return
        sql_update = "INSERT INTO targetpaper(targetPaper_scholarTitle, targetPaper_scholarInfo, " \
                     "targetPaper_citationURL, targetPaper_pdfURL, targetPaper_rawLink, targetPaper_rawInfo, " \
                     "targetpaper_publicationYear, targetPaper_nbCitation, targetPaper_isseen, paper_paper_id) VALUES"

        sql_update += self.sql
        cursor.execute(sql_update)
        self.sql = ""
        self.sql_cnt = 0
        try:
            db.commit()
        except:
            warnInfo("Execute DataBase FAILED!" )
            raise Exception

    def crawlWeb(self):
        # print "crawlWeb"
        while self.is_end != 1:
            try:
                response = self._requestWeb()
            except:
                warnInfo("Connection FAILED! The url is: " + self.url + " At Page" + self.start)
                raise Exception
            # 获取网页成功
            self._parseGoogle(response)
            if self.start + 20 >= self.nbCitation:
                break
            self.start += 20
        self.executeDataBase()


reload(sys)
sys.setdefaultencoding("utf-8")

for row_tuple in res_set:
    paper_id = int(row_tuple[0])
    paper_title = row_tuple[1]
    paper_url = row_tuple[2]

    urlWhole = url + paper_url

    cur_extract = extractCitation(urlWhole, headers, paper_title, paper_id)

    cur_extract.crawlWeb()

    sql_update = "UPDATE test_paper_shi SET has_get_citation=1 WHERE paper_id= '%d' " % (paper_id)

    cursor.execute(sql_update)
    print "----------------------%d SUSCESSED!  ----------------------" % index
    # fp.write("----------------------%d SUSCESSED!  ----------------------\n" %index)
    index += 1

# fp.close()
