import time
import aiohttp
import asyncio
from datetime import datetime
from aiostream import stream
import aiofiles
from motor.motor_asyncio import AsyncIOMotorClient
import os
from async_retrying import retry


class Infoq_detail():

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
        self.client = AsyncIOMotorClient(host='127.0.0.1', port=27017)
        self.db = self.client['spider_date']
        self.collection1 = self.db['info_seed']
        self.detail_url = 'https://www.infoq.cn/public/v1/article/getDetail'

    async def get_buff(self, item, session):
        """下载图片"""
        url = item.get('cover')
        async with session.get(url, headers=self.headers) as res:
            if res.status == 200:
                buff = await res.read()
                if len(buff):
                    print(f"NOW_IMAGE_URL:, {url}")
                    await self.get_img(item, buff)

    async def get_img(self, item, buff):
        file_path = item.get('file_path')
        img_path = item.get('img_path')
        if not os.path.exists(file_path):
            os.makedirs(file_path)

        if not os.path.exists(img_path):
            print(f"SAVE_PATH:{img_path}")
            async with aiofiles.open(img_path, 'wb') as f:
                await f.write(buff)

    @retry(attempts=5)
    async def get_date(self, item, session,):
        """获取数据"""
        refer = item.get('url')
        uuid = item.get('uuid')
        date = {'uuid': uuid}
        self.headers['Referer'] = refer
        update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await self.collection1.update_one({'uuid': uuid}, {'$set': {'status': 1}})
        async with session.post(url=self.detail_url, headers=self.headers, json=date, timeout=10) as res:
            if res.status == 200:
                res_json = await res.json()
                if res_json:
                    content = res_json.get('data').get('content').strip()
                    await self.collection1.update_one({'uuid': uuid}, {'$set': ({'content': content, 'update_time': update_time})})
        await self.collection1.update_one({'uuid': uuid}, {'$set': {'status': 2}})

    async def bound_featch(self, item, session):
        """创建路径，分发下载"""
        self.field_list = ['uuid', 'url', 'author', 'translator', 'topic', 'title', 'cover', 'summary', 'publish_time', 'md5name', 'create_time', 'status']
        md5name = item.get('md5name')
        file_path = os.path.join(os.getcwd(), 'infoq_cover')
        img_path = os.path.join(file_path, f'{md5name}.jpg')
        item['md5name'] = md5name
        item['img_path'] = img_path
        item['file_path'] = file_path
        await self.get_date(item, session)
        await self.get_buff(item, session)

    async def branch(self, coros, limit=10):
        """异步切片，限制并发"""
        index = 0
        while True:
            xs = stream.preserve(coros)
            ys = xs[index:index + limit]
            t = await stream.list(ys)
            if not t:
                break
            await asyncio.create_task(asyncio.wait(t))
            index = limit + 1


    async def run(self):
        """入口函数，创建总Session"""
        data = self.collection1.find()
        async_gen = (item async for item in data)
        async with aiohttp.TCPConnector(limit=100, force_close=True, enable_cleanup_closed=True) as tc:
            async with aiohttp.ClientSession(connector=tc) as session:
                coros = (asyncio.create_task(self.bound_featch(item, session)) async for item in async_gen)
                await self.branch(coros)


if __name__ == '__main__':
    now = time.time()
    spider = Infoq_detail()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(spider.run())
    loop.close()
    times = time.time()-now
    print(times)
