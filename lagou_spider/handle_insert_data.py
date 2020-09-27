import os,sys
if os.name=="nt":
    sys.path.append(r'E:\FuJianTech\LagouJobInfo\\')
if os.name=="posix":
    sys.path.append(r'/home/FuJianTech/LagouJobInfo/')

from collections import Counter
from sqlalchemy import func
# from LagouJobInfo.lagou_spider.create_lagou_tables import Lagoutables
from create_lagou_tables import Lagoutables
# from LagouJobInfo.lagou_spider.create_lagou_tables import Session
from create_lagou_tables import Session

import time


class HandleLagouData(object):
    def __init__(self):
        # 实例化session信息
        self.mysql_session = Session()
        self.date = time.strftime("%Y-%m-%d",time.localtime())

    def insert_item(self, item):
        collect_time = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())
        collect_data = time.strftime("%Y/%m/%d ", time.localtime())
        #存储的数据结构
        data = Lagoutables(
            #岗位ID
            positionId=item['positionId'],
            # 经度
            longitude=item['longitude'],
            # 纬度
            latitude=item['latitude'],
            # 岗位名称
            positionName=item['positionName'],
            # 工作年限
            workYear=item['workYear'],
            # 学历
            education=item['education'],
            # 岗位性质
            jobNature=item['jobNature'],
            # 公司类型
            financeStage=item['financeStage'],
            # 公司规模
            companySize=item['companySize'],
            # 业务方向
            industryField=item['industryField'],
            # 所在城市
            city=item['city'],
            # 岗位标签
            positionAdvantage=item['positionAdvantage'],
            # 公司简称
            companyShortName=item['companyShortName'],
            # 公司全称
            companyFullName=item['companyFullName'],
            # 公司所在区
            district=item['district'],
            # 公司福利标签
            companyLabelList=','.join(item['companyLabelList']),
            #工资
            salary=item['salary'],
            # 抓取时间
            crawl_date=collect_time,
            # 创建时间
            createtime=item['createTime'],
            # 抓取日期
            collect_data = collect_data
        )
        # 在存储数据之前，先查询表中是否有这条岗位信息
        query_result = self.mysql_session.query(Lagoutables).filter(Lagoutables.crawl_date == collect_data,
                                                                         Lagoutables.positionId == item['positionId']).first()
        if query_result:
            pass
            print('该岗位信息已存在%s:%s:%s' %(item['positionId'], item['city'], item['positionName']))
        else:
            try:
                print(72,data)
                self.mysql_session.add(data)
                self.mysql_session.commit()
                print('新增岗位:%s' %item['positionName'],"-----", '所在城市:%s' %item['city'])
                self.mysql_session.close()
            except Exception as e:
                self.mysql_session.rollback()
            finally:
                self.session.close()

    #行业信息
    def query_industryfield_result(self):
        info = {}
        # 查询今日抓取到的行业信息数据
        result = self.mysql_session.query(Lagoutables.industryField).filter(
            Lagoutables.crawl_date==self.date
        ).all()
        self.mysql_session.close()
        print(73,result)
        # 修改None 的spilt()问题
        result_list1= []
        for x in result:
            data_str = x[0]
            if data_str is   None:
                data_str = "空"
            data_str = data_str.split(',')
            result_list1.append(data_str[0])
        # result_list1 = [x[0].split(',')[0] for x in result]
        result_list2 = [x for x in Counter(result_list1).items() if x[1]>15]
        data = [{"name":x[0],"value":x[1]} for x in result_list2]
        name_list = [name['name'] for name in data]
        info['x_name'] = name_list
        info['data'] = data
        if  info is None:
            info = "空"
        return info

    # 查询薪资情况
    def query_salary_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.salary).filter(Lagoutables.crawl_date == self.date).all()
        self.mysql_session.close()

        # 处理原始数据
        result_list1 = [x[0] for x in result]
        # 计数,并返回
        result_list2 = [x for x in Counter(result_list1).items() if x[1]>9]
        result = [{"name": x[0], "value": x[1]} for x in result_list2]
        name_list = [name['name'] for name in result]
        info['x_name'] = name_list
        info['data'] = result
        return info

    # 查询工作年限情况
    def query_workyear_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.workYear).filter(Lagoutables.crawl_date==self.date).all()
        self.mysql_session.close()

        # 处理原始数据
        result_list1 = [x[0] for x in result]
        # 计数,并返回
        result_list2 = [x for x in Counter(result_list1).items()]
        result = [{"name": x[0], "value": x[1]} for x in result_list2 if x[1]>15]
        name_list = [name['name'] for name in result]
        info['x_name'] = name_list
        info['data'] = result
        return info

    # 查询学历信息
    def query_education_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.education).filter(Lagoutables.crawl_date==self.date).all()
        self.mysql_session.close()

        # 处理原始数据
        result_list1 = [x[0] for x in result]
        # 计数,并返回
        result_list2 = [x for x in Counter(result_list1).items()]
        result = [{"name": x[0], "value": x[1]} for x in result_list2]
        name_list = [name['name'] for name in result]
        info['x_name'] = name_list
        info['data'] = result
        return info

    # 岗位发布数量,折线图
    def query_job_result(self):
        info = {}
        result = self.mysql_session.query(Lagoutables.crawl_date,func.count('*').label('c')).group_by(Lagoutables.crawl_date).all()
        self.mysql_session.close()

        result1 = [{"name": x[0], "value": x[1]} for x in result]
        name_list = [name['name'] for name in result1]
        info['x_name'] = name_list
        info['data'] = result1
        return info

    # 根据城市计数
    def query_city_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.city,func.count('*').label('c')).filter(Lagoutables.crawl_date==self.date).group_by(Lagoutables.city).all()
        self.mysql_session.close()

        result1 = [{"name": x[0], "value": x[1]} for x in result]
        name_list = [name['name'] for name in result1]
        info['x_name'] = name_list
        info['data'] = result1
        return info

    #融资情况
    def query_financestage_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.financeStage).filter(Lagoutables.crawl_date == self.date).all()
        self.mysql_session.close()

        # 处理原始数据
        result_list1 = [x[0] for x in result]
        # 计数,并返回
        result_list2 = [x for x in Counter(result_list1).items()]
        result = [{"name": x[0], "value": x[1]} for x in result_list2]
        name_list = [name['name'] for name in result]
        info['x_name'] = name_list
        info['data'] = result
        return info

    # 公司规模
    def query_companysize_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.companySize).filter(Lagoutables.crawl_date == self.date).all()
        self.mysql_session.close()

        # 处理原始数据
        result_list1 = [x[0] for x in result]
        # 计数,并返回
        result_list2 = [x for x in Counter(result_list1).items()]
        result = [{"name": x[0], "value": x[1]} for x in result_list2]
        name_list = [name['name'] for name in result]
        info['x_name'] = name_list
        info['data'] = result
        return info


    # 任职情况
    def query_jobNature_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(Lagoutables.jobNature).filter(Lagoutables.crawl_date == self.date).all()
        self.mysql_session.close()

        # 处理原始数据
        result_list1 = [x[0] for x in result]
        # 计数,并返回
        result_list2 = [x for x in Counter(result_list1).items()]
        result = [{"name": x[0], "value": x[1]} for x in result_list2]
        name_list = [name['name'] for name in result]
        info['x_name'] = name_list
        info['data'] = result
        return info

    # 抓取数量
    def count_result(self):
        info = {}
        try:
            info['all_count'] = self.mysql_session.query(Lagoutables).count()
            info['today_count'] = self.mysql_session.query(Lagoutables).filter(Lagoutables.crawl_date==self.date).count()
            self.mysql_session.close()

            return info
        except :
            pass
lagou_mysql = HandleLagouData()
