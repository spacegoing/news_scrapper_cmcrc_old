# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient
import json

class NewsPipeline(object):

    # def open_spider(self, spider):
    #     self.client = MongoClient('mongodb://localhost:27017/')
    #     self.db = self.client['stockopedia']
    #     self.title_col = self.db['title']

    def process_item(self, item, spider):
        import ipdb; ipdb.set_trace()
        return item

