import json
import pymongo
import requests
from datetime import datetime
import hashlib


class Infoq_seed():

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
            'Referer': 'https://www.infoq.cn/',
            'Content-Type': 'application/json',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
        }
        self.url = 'https://www.infoq.cn/public/v1/my/recommond'
        self.detail_url = 'https://www.infoq.cn/article/'
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.client = pymongo.MongoClient()
        self.collection = self.client['spider_date']['info_seed']
        self.field_list = ['uuid', 'url', 'author', 'translator', 'topic', 'title', 'cover', 'summary', 'publish_time', 'md5name', 'create_time']



    def get_res(self, date):
        res = self.session.post(url=self.url, data=date, timeout=10)
        if res.status_code in [200, 201]:
            return json.loads(res.text)

    def save_data(self, res):
        tasks = []
        for data in res['data']:
            try:
                dic = dict()
                uuid = data.get('uuid')
                url = f'{self.url}{uuid}'
                title = data.get('article_title').strip()
                summary = data.get('article_summary')
                ctime = data.get('ctime')
                publish_time = datetime.utcfromtimestamp(int(ctime)/1000).strftime("%Y-%m-%d %H:%M:%S")
                cover = data.get('article_cover')
                author = data.get('author')
                if author:
                    author = ','.join([i.get('nickname') for i in author])
                else:
                    author = 'no_author'
                topic = ','.join([topic.get('name') for topic in data.get('topic')])
                translator = data.get('translator')
                if translator:
                    translator = ','.join([i.get('nickname') for i in translator])
                else:
                    translator = 'no_translator'
                md5name = hashlib.md5(title.encode("utf-8")).hexdigest()
                create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for field in self.field_list:
                    dic[field] = eval(field)
                print(dic)
                tasks.append(dic)
            except IndexError as e:
                print('解析出错', e)
        for task in tasks:
            self.collection.update_one({'uuid':task['uuid']}, {'$set':task}, upsert=True)


    def run(self):
        data = {'size':12}
        for i in range(10):
            res = self.get_res(data)
            score = res['data'][-1]['score']
            data.update({'score':score})
            self.save_data(res)


if __name__ == '__main__':
    spider = Infoq_seed()
    spider.run()
