#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     gift_analyze
   Description :
   Author :       zhanyuanpeng
   date：          2020/4/9
-------------------------------------------------
   Change Activity:
                   2020/4/9:
-------------------------------------------------
"""

import psycopg2
import sys
import configparser
import datetime

class DB:
    def __init__(self,host,port,target_db,user,password):
        self.host=host
        self.port=port
        self.target_db=target_db
        self.user=user
        self.password=password

        try:
            conn_str = "host='{}' port='{}' dbname='{}' user='{}' password='{}'".format(self.host, self.port,
                                                                                        self.target_db, self.user,
                                                                                        self.password)
            self.conn = psycopg2.connect(conn_str)
            self.conn.autocommit = True
        except Exception as e:
            self.errmsg="Connect to {} failed".format(self.target_db)
            sys.exit(self.errmsg)

    def execute_select_sql(self,sql):
        """execute sql and return"""
        cur = self.conn.cursor()
        error_code = 0
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        except Exception as e:
            err_msg = str(e)
            error_code = 1
            #msg = "select failed: {}".format(traceback.format_exc())
            msg = "select failed: " + err_msg
            self.logger.error(msg)

        if error_code:
            return error_code, msg
        else:
            return error_code, rows



class analyze_gift():
    def __init__(self):
        cf = configparser.ConfigParser()
        cf.read('db_conn.ini')

        self.host = cf.get("db connection","host")
        self.port = cf.get("db connection","port")
        self.target_db = cf.get("db connection","target_db")
        self.user = cf.get("db connection","user")
        self.password = cf.get("db connection","password")
        self.db = DB(self.host,self.port,self.target_db,self.user,self.password)

    def data_analyze(self):
        ## 当前统计的音浪最多的前五名
        sound_wave_top5="select fan_name,sum(sound_wave) from gift_list group by fan_name order by 2 desc limit 5;"

    def totalPeople(self):
        ## 被录入的刷礼物的总粉丝数（每天最多取前五名）
        totalPeople = "select count(distinct fan_name) from gift_list;"

        error_code, results = self.db.execute_select_sql(totalPeople)
        output = {}
        if not error_code:
            output['total_people'] = int(results[0][0])
            return error_code, output['total_people']
        else:
            return error_code, results

    def yesterdayPeople(self):
        ## 昨天录入的刷礼物的粉丝数
        yesterdayPeople = "select count(distinct fan_name) from gift_list where gift_time = current_date - interval '1 days';"

        error_code, results = self.db.execute_select_sql(yesterdayPeople)
        output = {}
        if not error_code:
            output['yesterdayPeople'] = int(results[0][0])
            return error_code,output['yesterdayPeople']
        else:
            return error_code, results

    def yesterdayPeoplelist(self):
        ## 昨天被录入的粉丝名单
        yesterdayPeoplelist = "select distinct fan_name from gift_list where gift_time = current_date - interval '1 days' order by 1;"

        error_code, results = self.db.execute_select_sql(yesterdayPeoplelist)

        output = {}
        if not error_code:
            list_str = ''
            for i in range(len(results)):
                if i < len(results)-1:
                    list_str=list_str + results[i][0] + ','
                else:
                    list_str = list_str + results[i][0]
            output['yesterdayPeople'] = list_str

            return error_code,output['yesterdayPeople']
        else:
            return error_code, results

    def sound_wave_1ws_count(self):
        ## 统计送出音浪累计超过1w的粉丝数
        SoundWave1Ws="select count(1) from (select fan_name,sum(sound_wave) as sum_sound_wave from gift_list group by fan_name order by 2 desc) t1 where sum_sound_wave > 10000;"
        error_code, results = self.db.execute_select_sql(SoundWave1Ws)
        output = {}
        if not error_code and results:
            output['sound_wave_1ws_count'] = int(results[0][0])
            return error_code,output['sound_wave_1ws_count']
        else:
            return error_code, results

    def sound_wave_1ws_count_list(self):
        ## 统计送出音浪超过1w的人员列表
        SoundWave1Wslist="select fan_name,sum_sound_wave  from (select fan_name,sum(sound_wave) as sum_sound_wave from gift_list group by fan_name order by 2 desc) t1 where sum_sound_wave > 10000;"
        error_code, results = self.db.execute_select_sql(SoundWave1Wslist)

        error_code, sound_wave_1ws_count = runsql.sound_wave_1ws_count()

        output={}
        if not error_code and sound_wave_1ws_count>0:
            list_str = ''
            for i in range(len(results)):
                if i < len(results)-1:
                    list_str=list_str + str(results[i][0]) + ' 贡献音浪 ' + str(results[i][1]) + ',\n\t'
                else:
                    list_str=list_str + str(results[i][0]) + ' 贡献音浪: ' + str(results[i][1]) + '\n'
            list_str='当前送出音浪超过1w的粉丝有{}人.'.format(sound_wave_1ws_count) + '送出音浪详情：\n\t' + list_str + '\t感谢支持小兔！\n'
            output['sound_wave_1ws_count_list'] = list_str
            return error_code,output['sound_wave_1ws_count_list']

        elif not error_code and sound_wave_1ws_count==0:
            list_str='当前贡献音浪累计超过1w的粉丝数为0,真是个伤心的数据，大家加把劲儿！'
            output['sound_wave_1ws_count_list'] = list_str
            return error_code,output['sound_wave_1ws_count_list']
        else:
            return error_code, results

    def sound_wave_top5(self):
        ## 当前统计的音浪最多的前五名
        sound_wave_top5="select fan_name,sum(sound_wave) from gift_list group by fan_name order by 2 desc limit 5;"
        error_code, results = self.db.execute_select_sql(sound_wave_top5)

        output = {}
        if not error_code:
            list_str = ''
            for i in range(len(results)):
                if i < len(results)-1:
                    list_str=list_str +  results[i][0] + ' 贡献音浪 '+ str(results[i][1]) + ',\n\t'
                else:
                    list_str = list_str + results[i][0] + ' 贡献音浪 ' + str(results[i][1]) + '\n'
            output['sound_wave_top5'] = list_str
            return error_code,output['sound_wave_top5']
        else:
            return error_code, results


if __name__ == '__main__':
    today=datetime.date.today()

    yesterday= (today + datetime.timedelta(days=-1))

    runsql=analyze_gift()

    # 获取刷礼物被录入的总粉丝数
    error_code, total_people = runsql.totalPeople()

    # 获取昨天刷礼物被录入的粉丝数
    error_code, yesterday_people = runsql.totalPeople()

    # 获取昨天被录入的的粉丝名单
    error_code, yesterday_people_list = runsql.yesterdayPeoplelist()

    # 当前统计的音浪最多的前五名
    error_code, sound_wave_top5 = runsql.sound_wave_top5()

    # 统计累计音浪超过1w的人数
    error_code,sound_wave_1ws_count = runsql.sound_wave_1ws_count()

    # 统计累计音浪超过1w的人数列表
    error_code,sound_wave_1ws_count_list = runsql.sound_wave_1ws_count_list()

    # 编辑文本内容
    print('''
喵小兔漫画-(画师七七)家粉丝音浪统计({})：
    {}
    
    -- more --
    当前在库记录的粉丝共{}人，
    昨天({})新增{}名粉丝的记录。
    昨天新增记录的粉丝列表：{}.    
    
    统计中的累计送音浪最多的前5列表如下：
    {}
万分感谢大家对喵小兔的喜爱和支持!!!

备注：音浪统计开始时间为2020-04-08，每日累计。每天统计送音浪最多的5个人的记录。送出音浪累计超过1W可以获得七七送出的惊喜！ 如有疑问，请咨询粉丝团管理。
    '''.format(today,sound_wave_1ws_count_list, total_people,yesterday, yesterday_people,yesterday_people_list,sound_wave_top5) )