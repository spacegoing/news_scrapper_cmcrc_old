# -*- coding: utf-8 -*-
import scrapy
import pandas as pd

class LoginSpider(scrapy.Spider):
    name = 'news'
    website_url = 'https://www.stockopedia.com/'
    news_page_url_tmp = 'https://www.stockopedia.com/share-prices/bhp-billiton-ASX:BHP/news/?page=%d'

    def start_requests(self):
        yield scrapy.Request('https://www.stockopedia.com/auth/login/',callback=self.login)

    def login(self,response):
        yield scrapy.FormRequest.from_response(
            response,
            formdata={'username': 'spacebnbk',
                      'password': 'qwertyuio',
                      'remember': 'on',
                      'redirect': 'auth%2Flogout'
            },
            callback=self.after_login
        )

    def after_login(self, response):
        # check login succeed before going on
        if b"incorrect" in response.body:
            self.logger.error("Login failed")
            return
        name_mkt_url_mat = pd.read_csv('total_secs.csv', header=None, index_col=None).as_matrix()

        # debug
        for i in name_mkt_url_mat:
            yield scrapy.Request(i[2]+'news/', callback=self.parse_first_page,
                                    meta={'name': i[0],
                                        'mkt': i[1],
                                        'url': i[2]
                                    }, dont_filter=True)

    def parse_first_page(self, response):
        """
        Do some initializtions before parsing:
        1. set up end_page_idx
        """
        end_pg_idx = -2 # by inspection of webpage. -1 is "Next"
        # get pagination list
        pg_li_list = response.xpath('//div[@class="pagination"][1]/li')
        if pg_li_list:
            end_pg_li = pg_li_list[end_pg_idx]
            end_page_idx = int(end_pg_li.xpath('a/text()').extract()[0].strip())
            # visit next pages
            # from scrapy.shell import inspect_response
            # inspect_response(response, self)
            news_pages = [response.url+'?page=%d' % i for i in range(1, end_page_idx+1)]

            # debug
            for url in news_pages:
                yield scrapy.Request(url, callback=self.parse, dont_filter=True, meta=response.meta)
        else:
            yield self.parse_return(response)

    def parse(self, response):
        yield self.parse_return(response)

    def parse_return(self, response):
        tr_list = response.xpath('//table[@class="noborder"]/tr')

        news_list = []
        for i in tr_list:
            td_list = i.xpath('td')
            date = td_list[0].xpath('text()').extract()[0].strip()
            title = td_list[1].xpath('a/text()').extract()[0].strip()
            url = td_list[1].xpath('a/@href').extract()[0].strip()
            news_list.append([date, title, url])

        return {'news_list': news_list, 'meta': response.meta}
