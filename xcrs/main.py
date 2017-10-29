# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 10:32:56 2017

@author: lisc1

1.原始数据库中清洗数据，并存储到分析服务器的数据库中
2.完成命令行接口部分
"""
import os
import logging
import sys
import time

import pandas as pd
import sqlalchemy
import numpy as np
import click

from model.company_portrait import Company
from model.person_portrait import Person



def etl(company_id,time=None):
    if time is None:
        pass
  
    
def get_all_companys(engine):
    """
    获取相城区所有企业
    """
    sql = "select distinct company_id from companyandworker"
    df_companys = pd.read_sql_query(sql,engine)
    companys = df_companys['company_id'].values.tolist()
    return companys

def get_all_workers(engine):
    """
    获取所有员工
    """
    sql = "select distinct person_id from companyandworker" 
    df_workers = pd.read_sql_query(sql,engine)
    workers = df_workers['person_id'].values.tolist()
    return workers
  
def run_person_portrait(engine_szrs,engine_analysis_db,start):
    """
    运行人物画像相关功能
    """
    workers = get_all_workers(engine_analysis_db)
    table_name1 = 'person_portrait'
    table_name2 = 'fraud_info'
    table_name3 = 'test_person_portrait_time'
    file_name = 'example_person.log'
    file_mode = 'a'
    
    logging.basicConfig(level=logging.DEBUG,
            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
            datefmt='%a, %d %b %Y %H:%M:%S',
            filename=file_name,
            filemode=file_mode)
    for i,worker in enumerate(workers[start:]):
        logging.info('i=%d',i+start)
        logging.info('worker=%s'%worker)
        try:
            person = Person(worker,engine_szrs,engine_analysis_db)
            person.person_base_info.to_sql(table_name1,engine_analysis_db,\
                                    if_exists='append',index=False)
            person_all_time_info = person.get_all_time_info()
            person.convert2lb(person_all_time_info).to_sql(table_name3,\
                            engine_analysis_db,if_exists='append',index=False)
            df_judge_medical = person.judge_medical_fraud()
            df_judge_preg = person.judge_preg_fraud()
            if not df_judge_medical.empty:
                df_judge_medical.to_sql(table_name2,engine_analysis_db,\
                                        if_exists='append',index=False)
            if not df_judge_preg.empty:
                df_judge_preg.to_sql(table_name2,engine_analysis_db,\
                                     if_exists='append',index=False)
        except Exception as e:
            logging.exception(e) 
       
            
def run_company_portrait(mode,runmode,start,date,engine_szrs,engine_analysis_db):
    """
    运行企业画像相关功能
    """
    companys = get_all_companys(engine_analysis_db)
    if mode=='test':
        table_name = 'test_company_portrait_time'
        file_name = 'test.log'
    else:
        #table_name = 'company_portrait_time'
        table_name = 'test_company_portrait_time'
        file_name = 'example.log'
    file_mode='a'

    logging.basicConfig(level=logging.DEBUG,
            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
            datefmt='%a, %d %b %Y %H:%M:%S',
            filename=file_name,
            filemode=file_mode)
    
    if runmode=='a':
        START_TIME = start
        END_TIME = int(time.strftime('%Y%m',time.localtime(time.time())))
    elif runmode=='b':
        START_TIME = END_TIME = int(time.strftime('%Y%m',time.localtime(time.time())))
    elif runmode=='c':
        START_TIME = END_TIME = date
    for i,company in enumerate(companys):
        logging.info('i=%d',i)
        logging.info('company=%s'%company)
        try:
            comp = Company(company,engine_szrs=engine_szrs,engine_analysis_db=engine_analysis_db,start_time=START_TIME,end_time=END_TIME)
            df = comp.company_portrait_time()
            df_convert = comp.convert2lb(df)
            if not df_convert.empty:
                df_convert.to_sql(table_name,engine_analysis_db,if_exists='append',index=False)
        except Exception as e:
            logging.exception(e)       

            
@click.command()
@click.option('--mode', default='test', help='choose programe run mode')
@click.option('--runmode', default='c', help='choose programe run mode')
@click.option('--date', default=201709, help='generate time data')
@click.option('--start', default=201601, help='generate time data')
@click.option('--s', default=0, help='generate time data')
def run(mode,runmode,date,start,s):
    """
    命令行参数解析
    """
    platform = sys.platform
    if platform=='linux':
        engine_szrs = sqlalchemy.create_engine("oracle://szythrd:szythrd912@10.39.43.72:1521/szsbbak?charset=utf-8")
    elif platform=='win32':
        engine_szrs = sqlalchemy.create_engine("oracle://szythrd:szythrd912@192.168.90.14:1521/szsbbak?charset=utf-8")
    engine_analysis_db = sqlalchemy.create_engine("postgresql://liyi:123456@172.16.102.24:5432/db_szrs")
    
    run_person_portrait(engine_szrs,engine_analysis_db,s)
    


if __name__=='__main__':
    run()
 
