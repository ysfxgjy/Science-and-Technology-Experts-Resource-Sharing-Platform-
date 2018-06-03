import scrapy

class ScholarSpider(scrapy.spider):
    name = "scholar"
    allowed_domains = ["cnki.net"]
    start_urls = ["http://papers.cnki.net/Search/Search.aspx?ac=result&sm=0&sv=%E5%8C%97%E4%BA%AC%E8%88%AA%E7%A9%BA%E8%88%AA%E5%A4%A9%E5%A4%A7%E5%AD%A6",]

    def parse(self, response):
        url_list = response.xpath('//*[@id="ajax_Sc"]/div[1]/div/div[2]/div[1]/span/a').extract()
        print(url_list)
