import sys
sys.path.append(r'/home/FuJianTech/LagouJobInfo/')

import json
import re
import time
import requests
import multiprocessing
from lagou_spider.handle_insert_data import lagou_mysql

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class HandleLaGou(object):
    def __init__(self):
        # 使用session保存cookies信息
        self.lagou_session = requests.session()
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'
        }
        self.city_list = ""

    # 获取全国城市列表的方法
    def handle_city(self):
        city_search = re.compile(r'www\.lagou\.com\/.*\/">(.*?)</a>')
        city_url = "https://www.lagou.com/jobs/allCity.html"
        city_result = self.handle_request(method="GET", url=city_url)
        # 使用正则表达式获取城市列表
        self.city_list = set(city_search.findall(city_result))
        self.lagou_session.cookies.clear()

    def handle_city_job(self, city):
        first_request_url = "https://www.lagou.com/jobs/list_python?city=%s&cl=false&fromSearch=true&labelWords=&suginput=" % city
        first_response = self.handle_request(
            method="GET", url=first_request_url)
        total_page_search = re.compile(r'class="span\stotalNum">(\d+)</span>')
        try:
            total_page = total_page_search.search(first_response).group(1)
        # 由于没有岗位信息造成的exception
        except BaseException:
            return
        else:
            from job_info import jobs
            import random
            # jobs = ['广告设计师', '多媒体设计师', '原画师', '游戏特效', '游戏界面设计师', '游戏场景', ]
            for j in jobs:
                for i in range(1, int(total_page) + 1):
                    data = {
                        "pn": i,
                        "kd": random.choice(jobs)
                    }
                    page_url = "https://www.lagou.com/jobs/positionAjax.json?city=%s&needAddtionalResult=false" % city
                    referer_url = "https://www.lagou.com/jobs/list_python/p-city_%s&cl=false&fromSearch=true&labelWords=&suginput=" % city

                    # referer的url需要进行encode
                    self.header['Referer'] = referer_url.encode()
                    time.sleep(5)
                    response = self.handle_request(
                        method="POST", url=page_url, data=data, info=city)
                    lagou_data = json.loads(response)
                    job_list = lagou_data['content']['positionResult']['result']
                    for job in job_list:
                        lagou_mysql.insert_item(job)

    def handle_request(self, method, url, data=None, info=None):
        while True:
            if method == "GET":
                response = self.lagou_session.get(
                    url=url, headers=self.header, verify=False)
            elif method == "POST":
                response = self.lagou_session.post(
                    url=url, headers=self.header, data=data)
            response.encoding = 'utf-8'
            if '频繁' in response.text:
                print('频繁')
                # 需要先清除cookies信息
                self.lagou_session.cookies.clear()
                # 重新获取cookies信息
                first_request_url = "https://www.lagou.com/jobs/list_python?city=%s&cl=false&fromSearch=true&labelWords=&suginput=" % info
                time.sleep(60)

                self.handle_request(method="GET", url=first_request_url)
                continue
            return response.text


if __name__ == '__main__':
    lagou = HandleLaGou()
    # 获取所有城市的方法
    lagou.handle_city()
    print(lagou.city_list)
    # 引入多进程加速抓取
    pool = multiprocessing.Pool(5)
    import random
    city_list = list(lagou.city_list)

    for city in city_list:
        pool.apply_async(
            lagou.handle_city_job, args=(
                random.choice(city_list),))
    pool.close()
    pool.join()
