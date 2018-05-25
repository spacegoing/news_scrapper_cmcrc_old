# -*- coding: utf-8 -*-
import scrapy
import pandas as pd

class LoginSpider(scrapy.Spider):
    name = 'stock_url'
    website_url = 'https://www.stockopedia.com/'
    share_prices_page_url = 'https://www.stockopedia.com/share-prices/?page=%d&region=au'
    start_page_idx = 1
    end_page_idx = start_page_idx

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
        init_url = self.share_prices_page_url % self.start_page_idx
        yield scrapy.Request(init_url, callback=self.parse_first_page)

    def parse_first_page(self, response):
        """
        Do some initializtions before parsing:
        1. set up end_page_idx
        """
        end_pg_idx = -2 # by inspection of webpage. -1 is "Next"

        # get pagination list
        pg_li_list = response.xpath('//div[@class="pagination"][1]/li')
        end_pg_li = pg_li_list[end_pg_idx]
        self.end_page_idx = int(end_pg_li.xpath('a/text()').extract()[0])

        # visit next pages
        shares_pages = [self.share_prices_page_url % i for i in range(self.start_page_idx, self.end_page_idx+1)]

        for i,url in enumerate(shares_pages):
            yield scrapy.Request(url, callback=self.parse_page, meta={'page_idx': i+1}, dont_filter=True)

    def parse_page(self, response):
        tr_list = response.xpath('//table/tbody/tr')
        name_mkt_href_list = []
        for i in tr_list:
            name = i.xpath('td[2]/a/text()').extract()[0].strip()
            mkt = i.xpath('td[2]/span/text()').extract()[0].strip().strip('(').strip(')')
            href = self.website_url + i.xpath('td[2]/a/@href').extract()[0].strip()
            name_mkt_href_list.append([name, mkt, href])

        pd.DataFrame(name_mkt_href_list).to_csv('./name_mkt_url_data/%d.csv' % response.meta['page_idx']
                                                , header=False, index=False)

