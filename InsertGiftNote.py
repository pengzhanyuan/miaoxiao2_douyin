#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     InsertGiftNote
   Description :
   Author :       zhanyuanpeng
   date：          2020/4/9
-------------------------------------------------
   Change Activity:
                   2020/4/9:
-------------------------------------------------
"""

from gift_analyze import DB
import datetime
import configparser





class Insert_gift:
    def __init__(self):
        cf=configparser.ConfigParser()
        cf.read('db_conn.ini')
        self.host = cf.get("db connection","host")
        self.port = cf.get("db connection","port")
        self.target_db = cf.get("db connection","target_db")
        self.user = cf.get("db connection","user")
        self.password = cf.get("db connection","password")
        self.db = DB(self.host,self.port,self.target_db,self.user,self.password)

    def insert_gift(self,fan_name,sound_wave,idol='喵小兔漫画-(画师七七)',gift_time=datetime.date.today()+datetime.timedelta(days=-1),reporter='包包(漠南)'):

        self.fan_name=fan_name
        self.sound_wave=sound_wave
        self.idol=idol
        self.gift_time=gift_time
        self.reporter=reporter

        insert_gift="INSERT INTO gift_list(fan_name,sound_wave,idol,gift_time,reporter) VALUES ('{}','{}','{}','{}','{}') returning id;".format(self.fan_name,self.sound_wave,self.idol,self.gift_time,self.reporter)
        error_code,results = self.db.execute_insert_sql(insert_gift)
        id = int(results[0][0])
        select_gift = "select * from gift_list where id={};".format(id)
        error_code,results=self.db.execute_select_sql(select_gift)

        return error_code,results




if __name__ == '__main__':
    today = datetime.date.today()
    yesterday = today + datetime.timedelta(days=-1)
    fan_name=input('粉丝姓名: ')
    sound_wave=input('音浪数:')
    idol=input('偶像(default:喵小兔漫画-(画师七七)):')
    gift_time=input('送礼日期(default:{}):'.format(yesterday))
    reporter=input('登记人(default:包包(漠南)):')

    runsql=Insert_gift()
    error_code,id = runsql.insert_gift(fan_name,sound_wave)
    print(id)