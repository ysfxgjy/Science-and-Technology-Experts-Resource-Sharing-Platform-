import scrapy
from scrapy.spider import Spider
from bs4 import BeautifulSoup
import pymongo
from scrapy.http.cookies import CookieJar
from scrapy.http import Request
import urllib.parse
import re

global conn
conn = pymongo.MongoReplicaSetClient('mongodb://%s:%s@%s/SpiderResult' % ("usrname", "pwd", "ip:port"))
global db
global counter
counter=0
global article_counter
article_counter=0
db = conn.SpiderResult

class ScholarSpider(Spider):
    name = "scholar"
    global school
    f=open(r"F:\\Code\\ScholarSpider\\SpiderData\\SpiderData\\spiders\\school.txt","r")
    line = f.readline()
    school_list=list()
    while line:
        line = line[:-1]
        school_list.append(line)
        line=f.readline()
    print(school_list)
    school="北京航空航天大学"
    allowed_domains = ["cnki.net"]
    start_urls = [];

    for j in range(0,2):
        for i in range(1,70):
            url_string = "http://papers.cnki.net/View/DataCenter/Scholar.ashx?nmt=nm&sm=1&nmv=&id=SC&db=0&cp="+str(i)+"&ck=d6adaba7-1623-41bb-8c1e-18ece5b29ee1&p=&uid=-1&ut="+school_list[j]
            start_urls.append(url_string)
            print(url_string)
    
    def parse(self, response):
        #from scrapy.shell import inspect_response
        #inspect_response(response, self)
        
        #print(response.body.decode('utf-8'))        
        soup = BeautifulSoup(response.body)
        result_list= soup.find_all(class_="listBox wauto clearfix")
        if len(result_list) != 0:
            for item in result_list:            
                #获取学者姓名
                info = item.find('span',{'class','f14'})
                scholar_name = info.find('a').text
                #print(scholar_name)

                #获取学者username
                scholar_username=info.text.split('\r\n')[1].split('\u3000')[1]
                #print(scholar_username)

                #获取学者vid
                info = item.find('span',{'class','f14'})
                scholar_vid = info.find('a')['href'][21:]
                #print(scholar_vid)
            
                #获取学者主页链接
                prefix_href = "papers.cnki.net\\"
                scholar_page=prefix_href+scholar_username
                #print(scholar_page)

                #获取学者研究领域
                info = item.find('div',{'class','xuezheDW'})
                scholar_field = info.text
                scholar_field = scholar_field[5:]
                #print(scholar_field)

                db.Experts.update({'vid':scholar_vid},{'$set':{"vid":scholar_vid, "name":scholar_name, "username":scholar_username, "page":scholar_page, "field":scholar_field}}, upsert = True)
                print()

                #学者info页面
                info_url = "http://papers.cnki.net/View/DataCenter/GetHTML.ashx?ac=sinfo&vt=1&pid="+scholar_vid+"&r=0"
                request = scrapy.Request(info_url,callback=self.parse_info)
                yield request
                request.meta['vid']=scholar_vid

                #学者data            
                info = item.find_all('span',{'class','numList'})
                db.Experts_data.update({'vid':scholar_vid},{'$set':{"achievements_count":info[0].text[:-5]}},upsert = True)
                db.Experts_data.update({'vid':scholar_vid},{'$set':{"hIndex":info[1].text}},upsert = True)
                db.Experts_data.update({'vid':scholar_vid},{'$set':{"refered":info[2].text.split('/')[1]}},upsert = True)

                #Article-year
                data_url = "http://papers.cnki.net/View/Analysis/Show.aspx"
                querystring = {"ac":"pc","vid":scholar_vid,"t":"9","_":"1527774101863"}
                r = Request(data_url+'?'+urllib.parse.urlencode(querystring), callback=self.parse_year)
                yield r
                r.meta['vid']=scholar_vid

                #Article-type
                type_url = "http://papers.cnki.net/View/Analysis/Show.aspx?ac=dt&vid="+scholar_vid+"&t=10&_=1527763220464"
                request = scrapy.Request(type_url,callback=self.parse_type)
                yield request
                request.meta['vid']=scholar_vid

                #Aritcle-detail
                for page_number in range(1,101):            
                    article_url = "http://papers.cnki.net/View/Analysis/Show.aspx?ac=qd&mt=f&pi="+str(page_number)+"&vid="+str(scholar_vid)+"&tl=45&ot=QuotedCount&od=DESC&t=19"
                    request = scrapy.Request(article_url,callback=self.parse_article)
                    yield request
                    request.meta['vid']=scholar_vid
                
                global counter
                counter+=1
                print("Finish "+str(counter)+" scholars")

    def parse_info(self,response):
        #from scrapy.shell import inspect_response
        #inspect_response(response, self)
        vid = response.meta['vid']
        soup = BeautifulSoup(response.body)
        result_list = soup.find_all('div',{'class','f-ct'})
        prize = list()
        if len(result_list[6].find_all('input')) != 0:
            for item in result_list[6].find_all('input'):
                prize.append(item.attrs['value'])
        fund = list()
        if len(result_list[8].find_all('input')) != 0:
            for item in result_list[8].find_all('input'):
                fund.append(item.attrs['value'])
        history_school = list()
        if len(result_list[2].find_all('input')) != 0:
            for item in result_list[2].find_all('input'):
                history_school.append(item.attrs['value'])
        db.Experts.update({'vid':vid},{'$set':{"school":result_list[1].text, "history_school":history_school, "target":result_list[3].text, "postcode":result_list[5].text, "prize":prize, "homepage":result_list[7].text, "fund":fund, "career_title":result_list[9].text}}, upsert = True)

    def parse_year(self,response):
        soup = BeautifulSoup(response.body)
        vid = response.meta['vid']
        year_dict=dict()
        if(soup.text):
            evaled = eval(soup.text)
            for list_item in evaled:
                evaled_item = eval(str(list_item))
                year_dict[str(evaled_item[0])]=str(evaled_item[1])
        db.Experts_data.update({'vid':vid},{'$set':{'year':year_dict}}, upsert = True)

    def parse_type(self,response):
        soup = BeautifulSoup(response.body)        
        vid = response.meta['vid']        
        item = soup.find_all('tr')
        type_dict=dict()

        if len(item)!=0:
            for each_item in item:
                cell = each_item.find_all('td')
                if len(cell)!=0:
                    if(len(cell)==3):
                        if ('总计' in cell[1].text) == False:
                            type_dict[cell[1].text] = str(cell[2].text)
                    else:
                        if ('总计' in cell[0].text) == False:
                            type_dict[cell[0].text] = str(cell[1].text)

        db.Experts_data.update({'vid':vid},{'$set':{'type':type_dict}}, upsert = True)

    def parse_article(self,response):
        soup = BeautifulSoup(response.body)
        vid = response.meta['vid']        
        cols = soup.find_all('tr')

        for each_col in cols:
            item = each_col.find_all('td')
            if len(item) != 0:
                link_prefix = 'http://papers.cnki.net'
                link = link_prefix+item[1].find('a').attrs['href']
                db.Experts_articles.update({'vid':vid, 'title':item[1].text},{'$set':{'vid':vid, 'title':item[1].text, 'link':link}}, upsert = True)

                author_list = re.split('[,;]',item[2].text)
                author_dict = dict()
                for i in range(0,len(author_list)):
                    if author_list[i] != '' and author_list[i] != ' ':
                        author_dict[str(i+1)]=str(author_list[i])
                db.Experts_articles.update({'vid':vid, 'title':item[1].text},{'$set':{'author':author_dict}}, upsert = True)
                db.Experts_articles.update({'vid':vid, 'title':item[1].text},{'$set':{'source':item[3].text}}, upsert = True)
                db.Experts_articles.update({'vid':vid, 'title':item[1].text},{'$set':{'time':item[4].text}}, upsert = True)
                db.Experts_articles.update({'vid':vid, 'title':item[1].text},{'$set':{'type':item[5].text}}, upsert = True)
                db.Experts_articles.update({'vid':vid, 'title':item[1].text},{'$set':{'refered_count':item[6].text}}, upsert = True)
                global article_counter
                article_counter+=1
                print("Crawed "+str(article_counter)+" articles")
