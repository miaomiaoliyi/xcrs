# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 13:31:38 2017

@author: lisc1
"""
import os
import sys
import math
import time
import datetime

import pandas as pd
import sqlalchemy
import numpy as np
import plotly
import plotly.graph_objs as go
import pylab as pl
import matplotlib.pyplot as plt

#import person_portrait
#import utils.api


class Company(object):
    
    def __init__(self,company_id,engine_szrs,engine_analysis_db,engine_lb,
                 start_time,end_time):
        """
        
        Parameters
        ----------
        companu_id:str
            单位ID
        engine_szrs:SQLAlchemy connectable
            苏州数据服务器连接
        engine_analysis_db:SQLAlchemy connectable
            分析服务器数据连接
            
        """
        self.engine_szrs = engine_szrs
        self.engine_analysis_db = engine_analysis_db
        self.engine_lb = engine_lb
        self.company_id = company_id
        self.start_time = start_time
        self.end_time = end_time
        #企业的基本情况
        self.company_base_info = self.base_info()
        #企业的医保缴费情况
        self.company_medical_fund_payment_info = self.medical_fund_payment_info()
        #企业员工的医疗支出情况
        self.company_fund_spending_info = self.health_care_spending_info()
        #企业员工的工伤情况
        self.company_injury_info = self.injury_info()
        #企业员工的失业情况
        self.company_unemployment_info = self.unemployment_info()
        #企业医保收入，支出汇总
        #self.company_all_time_data = self.company_time_period_data()
        
        
    def base_info(self):
        """
        抽取AAB001=company_id的单位的基本信息(AB01)
        """
        sql = "select * from szyth.AB01 where AAB001=%d"%int(self.company_id)
        df = pd.read_sql_query(sql,self.engine_szrs)
        return df
        
 
    def medical_fund_payment_info(self):
        """
        单位历史所有员工的医保缴费记录
        """
        sql = "select to_char(AAB001) 单位ID,to_char(AAC001) 人员ID,AAE002 费款所属期,AAE003 对应费款所属期,AAE201 缴费月数,AAE180 人员缴费基数 from szyth.KC43 \
        where AAB001=%d and AAE140='310' and AAE003>=%d and AAE003<=%d order by 对应费款所属期,人员ID"%(int(self.company_id),self.start_time-100,self.end_time)
        df = pd.read_sql_query(sql,self.engine_szrs)
        return df
     
        
    def mature_fund_payment_info(self):
        """
        单位历史所有员工的生育缴费记录
        """
        sql = "select to_char(AAB001) 单位ID,to_char(AAC001) 人员ID,AAE002 费款所属期,AAE003 对应费款所属期,AAE201 缴费月数,AAE180 人员缴费基数 from szyth.MC43 \
        where AAB001=%d and AAE003>=%d and AAE003<=%d order by 对应费款所属期,人员ID"%(int(self.company_id),self.start_time-100,self.end_time)
        df = pd.read_sql_query(sql,self.engine_szrs)
        return df
    
    def company_zhenjiao_info(self):
        """
        单位征缴记录
        """
        sql = "select to_char(AAB001) 单位ID,AAE003,AAE140 险种代码,AAB119 应缴人数,AAE020 应缴金额,\
        AAE080 实缴金额,AAE078 足额到账标志 from szyth.AB07 where AAB001=%d order by aae003"%int(self.company_id)
        df = pd.read_sql_query(sql,self.engine_szrs)
        df_money = df.groupby('aae003')['应缴金额','实缴金额'].sum()
        df_renshu = df.groupby('aae003')['应缴人数'].mean().to_frame()
        df_renshu['应缴人数'] = df_renshu['应缴人数'].apply(lambda i:math.ceil(i))
        df_money['应缴人数'] = df_renshu
        df_money.reset_index(inplace=True)
        return df_money
        
    def health_care_spending_info(self):
        """
        单位员工的历史医保支出信息（包含在本单位期间及不在本单位期间）
        """
        df_fund_spend = pd.DataFrame()
        workers = self.get_all_workers()
        for worker in workers:
            sql = "select to_char(AAC001) 人员ID,CKE363 险种分类,AKC264 费用总额,\
            AKB068 统筹基金支付,AAE149 年月,BKE032 交易时间 from szyth.KC12 where \
            AAC001=%d and AAE149>=%d and AAE149<=%d order by AAE149"\
            %(int(worker),self.start_time-100,self.end_time)
            df_temp = pd.read_sql_query(sql,self.engine_szrs)
            df_fund_spend = pd.concat([df_fund_spend,df_temp])
        if not df_fund_spend.empty:
            df_fund_spend['年月'] = df_fund_spend['年月'].astype('int')
        else:
            df_fund_spend = pd.DataFrame({'人员ID':[],
                                          '险种分类':[],
                                          '统筹基金支付':[],
                                          '年月':[]})
        return df_fund_spend
    
        
    def injury_info(self):
        """
        单位员工的工伤信息
        """
        sql = "select to_char(AAC001) person_id,to_char(AAB001) company_id,ALC031 time \
        from szyth.LC31 where AAB001=%d and (ALA015='1' or ALA015='2')"%int(self.company_id)
        df_injury_info = pd.read_sql_query(sql,self.engine_szrs)
        if not df_injury_info.empty:
            df_injury_info['time'] = df_injury_info['time'].apply(lambda x:str(x)[:6])
            workers = df_injury_info['person_id'].values.tolist()
            df = pd.DataFrame()
            for worker in workers:
                if worker is None:continue
                sql2 = "select to_char(AAC001) person_id,ALC021 injury_level \
                from szyth.LC04 where AAC001=%d"%int(worker)
                df_injury_grading = pd.read_sql_query(sql2,self.engine_szrs).dropna()
                df = pd.concat([df,df_injury_grading])
        if df_injury_info.empty or df.empty:
            df_injury_info['events'] = 'L'
            df_injury_info['total_money'] = np.nan
            df_injury_info['level'] = np.nan
            df_injury_info['flag'] = True
            return df_injury_info

        df_injury_info = pd.merge(df_injury_info,df,
                                  left_on=['person_id'],right_on=['person_id'],
                                  how='left')
        df_injury_info['events'] = 'L'
        df_injury_info['total_money'] = np.nan
        df_injury_info['flag'] = True
        df_injury_info = df_injury_info[['person_id','company_id','time',
                                         'events','total_money','level','flag']]        
        return df_injury_info
        
    
    def unemployment_info(self):
        """
        单位的失业信息（特指领取了失业基金的失业员工）
        """
        sql = "select to_char(AAC001) person_id,to_char(AAB001) company_id,\
        AAE210 time from szyth.JCC4 where AAE210>201001 and (CJB003='30' \
        or CJB003='99') and AAB001=%d"%int(self.company_id)
        df_unemployment_info = pd.read_sql_query(sql,self.engine_szrs)
        if not df_unemployment_info.empty:
            df_unemployment_info['events'] = 'J'
            df_unemployment_info['total_money'] = np.nan
            df_unemployment_info['level'] = np.nan
            df_unemployment_info['flag'] = np.nan
            df_unemployment_info = df_unemployment_info[['person_id',
                                                         'company_id',
                                                         'time',
                                                         'events',
                                                         'total_money',
                                                         'level',
                                                         'flag']]  
        return df_unemployment_info
   
        
    def mat_benefit(self):
        """
        单位领取生育津贴信息
        """
        sql = "select to_char(AAC001) 人员ID,CME020 天数,AMC030 金额 from \
        szyth.MBA7 where AAB001=%d and CME020>0"%int(self.company_id)
        df = pd.read_sql_query(sql,self.engine_szrs).drop_duplicates()
        return df
        
    def arbitration_case(self):
        """
        仲裁案件（LB）
        """
        sql = "select * from xcsp.fa01 where AAB001=%d"%int(self.company_id)
        df = pd.read_sql_query(sql,self.engine_lb)
        return df
    
    def introduction_talents(self):
        """
        人才引进(LB)
        """
        pass
    
    def talent_declare(self):
        """
        人才申报(LB)
        """
        pass
    
    
#==============================================================================
        
    def get_preg_fraud_info(self):
        """
        从分析服务器中获取生育欺诈的数据
        """
        sql = "select * from fraud_info where company_id='%s' and events='M'"%self.company_id
        df_preg_fraud_info = pd.read_sql_query(sql,self.engine_analysis_db)
        return df_preg_fraud_info
        
     
    def get_medical_fraud_info(self):
        """
        从分析服务器中获取带病参保数据
        """
        sql = "select * from fraud_info where company_id='%s' and events='K'"%self.company_id
        df_medical_fraud_info = pd.read_sql_query(sql,self.engine_analysis_db)
        return df_medical_fraud_info

#==============================================================================
     
    def get_region(self):
        """
        单位的所属街道
        """
        return self.company_base_info['aab073']

    def get_all_workers(self):
        """
        单位的所有员工列表
        """
        return self.company_medical_fund_payment_info['人员ID'].unique().tolist()
        
        
    def get_establish_time(self):
        """
        获取企业的存续年限
        """      
        return self.company_base_info['aae047'].values[0]
        
  
    def get_all_times(self):
        """
        获取所有有记录的社保缴费月份
        """
        return self.company_medical_fund_payment_info['对应费款所属期'].unique().tolist()
        
        
    def worker_flow_cycle(self):
        """
        获取企业员工的流动周期
        """
        pass
    
    def company_worker_info(self,workers,in_time):
        """
        计算员工的年龄分布，性别分布，户籍分布,学历分布
        """
        if len(workers)>1:
            workers = tuple(list(map(lambda i:int(i),workers)))
            sql = 'select to_char(AAC001) 人员ID,AAC003 姓名,AAC147 证件号码,\
            AAC004 性别,AAC006 出生日期,AAC011 学历 from szyth.AC01 \
            where AAC001 in %s'%str(workers)
        elif len(workers)==1:
            workers = int(workers[0])
            sql = 'select to_char(AAC001) 人员ID,AAC003 姓名,AAC147 证件号码,\
            AAC004 性别,AAC006 出生日期,AAC011 学历 from szyth.AC01 \
            where AAC001=%d'%workers

        df = pd.read_sql_query(sql,self.engine_szrs)
        df['人数']=1
        #性别
        gender = pd.DataFrame(np.array([0]*2).reshape(2,1),
                              columns=['num'],
                              index=['1','2'])
        gender_group = df.groupby(['性别']).人数.count()
        gender_group.index.name=None
        gender['人数'] = gender_group
        gender.drop(['num'],axis=1,inplace=True)
        gender['人数'] = gender['人数'].fillna(0)
        gender = gender.T
        
        #年龄
        def slice_age(i):
            if i<25:
                return '100'
            elif i>=25 and i<35:
                return '200'
            elif i>=35 and i<45:
                return '300'
            elif i>=45 and i<55:
                return '400'
            else:
                return '500'
                
        df['年龄'] = df['出生日期'].apply(lambda i:int(str(in_time)[:4])-int(str(i)[:4]))
        df['年龄分段'] = df['年龄'].apply(slice_age)
        age_slice_group = df.groupby(['年龄分段']).人数.count()
        age_slice_group.index.name=None
        age_slice = pd.DataFrame(np.array([0]*4).reshape(4,1),
                              columns=['num'],
                              index=['100','200','300','400'])
        age_slice['人数'] = age_slice_group
        age_slice.drop(['num'],axis=1,inplace=True)
        age_slice['人数'] = age_slice['人数'].fillna(0)
        age_slice = age_slice.T
        
        #户籍        
        df['户籍'] = df['证件号码'].apply(lambda i:i[:4])
        huji_group = df.groupby(['户籍'],as_index=False).人数.count()
    
        #员工的学历统计
        xueli = pd.DataFrame(np.array([0]*13).reshape(13,1),
                             columns=['num'],
                             index=['11','14','21','31','41','44','47','60',\
                             '61','71','81','90','00'])
        df['学历'] = df['学历'].fillna('00')
        xueli_group = df.groupby(['学历']).人数.count()
        xueli_group.index.name=None
        xueli['人数'] = xueli_group
        xueli.drop(['num'],axis=1,inplace=True)
        xueli['人数'] = xueli['人数'].fillna(0)
        xueli = xueli.T

        df_return = pd.concat([gender,age_slice,xueli],axis=1)
        return df_return,huji_group
    
        
    def company_workers_health_cds(self):
        """
        企业员工的健康状况(年人均平均医疗消费金额，年人均医疗次数，大额支出员工占比)
        """
        df_merge = pd.merge(self.company_fund_spending_info,\
                            self.company_medical_fund_payment_info,\
                            left_on=['人员ID','年月'],\
                            right_on=['人员ID','对应费款所属期'],\
                            how='right')\
                            .sort_values(by=['对应费款所属期'])
        df_merge= df_merge[['对应费款所属期','人员ID','险种分类','费用总额','统筹基金支付']]
        df_merge['SYEAR'] = df_merge['对应费款所属期'].apply(lambda i:str(i)[:4])
        df_merge['SMONTH'] = df_merge['对应费款所属期'].apply(lambda i:str(i)[4:6])  
        df_merge.drop(['对应费款所属期'],axis=1,inplace=True)
        df_merge.fillna(0,inplace=True)
        df_merge = df_merge[df_merge['险种分类']!='2']
        
        #年人均医疗花费
        df_cost = df_merge.groupby(['SYEAR','人员ID'],as_index=False).费用总额.sum()
        df_yl_ave = (df_cost.groupby('SYEAR').费用总额.sum()/
                  df_cost.groupby('SYEAR').人员ID.count()).to_frame()
        df_yl_ave.columns = ['ave_cost']
        df_yl_ave['ave_cost'] = df_yl_ave['ave_cost'].astype('int')
        df_yl_ave.fillna(0,inplace=True)
        #年人均医疗次数
        df_merge_temp = df_merge[df_merge['费用总额']>10]
        df_cishu = df_merge_temp.groupby(['SYEAR']).险种分类.count()
        df_cishu_ave = (df_cishu/df_cost.groupby('SYEAR').人员ID.count()).to_frame()
        df_cishu_ave.columns = ['ave_cishu']
        df_cishu_ave.fillna(0,inplace=True)
        #大额支出员工占比
        df_merge_temp2 = df_merge[df_merge['费用总额']>5000]
        df_shengbing = df_merge_temp2.groupby(['SYEAR','人员ID'],as_index=False).险种分类.count()
        df_zb = (df_shengbing.groupby(['SYEAR']).人员ID.count()/
                     df_cost.groupby('SYEAR').人员ID.count()).to_frame()
        df_zb.columns = ['sb_zb']
        df_zb.fillna(0,inplace=True)
        
        df_return = pd.concat([df_yl_ave,df_cishu_ave,df_zb],axis=1)
        df_return.reset_index(inplace=True)
        return df_return
    
        
    def company_time_period_data(self):
        """
        单位的社保缴费，新增员工，员工离职的时序数据
        """
        df_all_time = pd.DataFrame()
        time_series = self.company_medical_fund_payment_info['对应费款所属期']\
                        .unique()\
                        .tolist()
        
        df_merge = pd.merge(self.company_fund_spending_info,\
                            self.company_medical_fund_payment_info,\
                            left_on=['人员ID','年月'],\
                            right_on=['人员ID','对应费款所属期'],how='right')\
                            .sort_values(by=['对应费款所属期'])\
                            .fillna(0)
        df_month_fund_cost = df_merge.groupby(['对应费款所属期']).统筹基金支付\
                            .sum().to_frame()
        df_month_fund_cost.index.name = '年月'
        df_month_fund_cost.reset_index(inplace=True) 
                   
        old_workers = []
        for i in time_series:

            df = self.company_medical_fund_payment_info[self.company_medical_fund_payment_info['对应费款所属期']==i]
            workers = df['人员ID'].unique().tolist()
            df_workers_info = self.company_worker_info(workers,i)
            
            num_of_pay = len(df)
            ave_insurance_base = int(df['人员缴费基数'].mean())

            num_of_worker_add = len(list(set(workers).difference(set(old_workers))))
            num_of_worker_rm = len(list(set(old_workers).difference(set(workers))))
            
            old_workers = workers

            df_one_time = pd.DataFrame({'单位ID':[self.company_id],
                                        '年月':[i],
                                        '缴费人数':[num_of_pay],
                                        '人均缴费基数':[ave_insurance_base],
                                        '新增员工':[num_of_worker_add],
                                        '离职员工':[num_of_worker_rm]})
            df_one_time = pd.concat([df_one_time,df_workers_info],axis=1)
            
            df_all_time = pd.concat([df_all_time,df_one_time])
        if df_all_time.empty:
            df_all_time = pd.DataFrame({'单位ID':[],
                                        '年月':[],
                                        '缴费人数':[],
                                        '人均缴费基数':[],
                                        '新增员工':[],
                                        '离职员工':[]})

        df_all_time = pd.merge(df_month_fund_cost,df_all_time,
                                left_on=['年月'],right_on=['年月'],how='inner')
        
        df_all_time = df_all_time[['单位ID','年月','缴费人数','新增员工','离职员工',
        '人均缴费基数','统筹基金支付']]  
        #,'man_num','woman_num','ageslice1','ageslice2',
        #'ageslice3','ageslice4','ageslice5','bendi','jiangsu','waidi'
        
        df_all_time.columns = ['company_id','time','num_of_contrib',
        'new_add_employee','rm_employee','ave_pay_fund','month_total_fund_pay']  
        

        return df_all_time

        

#==============================================================================
    #企业基本属性：所属行业，单位类型，存续年限       
    def count_comp_type_score(self):
        """
        根据单位类型，计算评分
        """
        if self.company_base_info['aab019'].empty:
            return 0.2
        else:
            comp_type = self.company_base_info['aab019'].values[0]
        
        if comp_type in ['210','220','230','300']:
            comp_type_score = 0.8
        elif comp_type in ['100']:
            comp_type_score = 0.6
        elif comp_type in ['500']:
            comp_type_score = 0.5
        elif comp_type in ['400','600']:
            comp_type_score = 0.4
        elif comp_type in ['000','800','700']:
            comp_type_score = 0.2
        else:
            comp_type_score = 0.2
        
        return comp_type_score
        
        
    def count_sector_score(self):
        """
        根据所属行业，计算评分
        按优先发展来给分
        """
        if self.company_base_info['aab022'].empty:
            return 0.2
        else:
            comp_sector = self.company_base_info['aab022'].values[0]

        if comp_sector in ['0700','1300','1600']:
            comp_sector_score = 0.8
        elif comp_sector in ['0100','0200','0800','1100','1500']:
            comp_sector_score = 0.3
        else:
            comp_sector_score = 0.5
            
        return comp_sector_score
      
        
    def count_duration_score(self):
        """
        根据企业的存续年限，计算评分
        """
        def count_month(date):
            """
            根据传入日期计算距今月数
            """  
            from datetime import datetime
            if date is None or len(str(date))<8:
                return -1    
            year = int(str(date)[:4])
            month = int(str(date)[4:6])
            day = int(str(date)[6:8])
            now = datetime.now()
            delta = datetime(now.year,now.month,now.day)-datetime(year,month,day)
            return math.ceil(delta.days/30)  
        def sigmoid(x):
            return 1/(1+math.exp(-0.5*(x/duration_median-1)))
            
        date = self.get_establish_time()
        duration_median = 50        
        duration = count_month(date)
        #存续年限字段为空或者字段设置不正确时评分
        if duration == -1:
            comp_drs_score = 0.5
            return comp_drs_score
        #存取年限字段可能记录错误，异常值用中位数代替
        elif duration >1000:
            duration = duration_median      
        comp_drs_score  = sigmoid(duration)   
        
        return comp_drs_score    
      
        
    #企业人社信用评分（仲裁案件发生数，失信人员）    
    def count_judge_score(self,in_time):
        """
        根据最近12个月内仲裁案件数，计算评分
        """
        def sigmoid(x):
            return 1/(1+math.exp(-0.5*(x)))
        df_arbitration_case = self.arbitration_case()
        if df_arbitration_case.empty:return 1
        
        
        
    
    def count_fraud_score(self,in_time):
        """
        基于单位最近12个月的失信人员，计算评分
        """
        def sigmoid(x):
            return 1/(1+math.exp(-0.5*(x)))
        df_medical_fraud = self.get_medical_fraud_info()
        if df_medical_fraud.empty:
            return 1
        else:
            num_medical_fraud = len(df_medical_fraud)
            return 1-sigmoid(num_medical_fraud)
            

            
    #企业社保缴纳情况评分（缴纳人员数，缴纳基数，缴纳延时率）
    def count_insurance_contrib_score(self,in_time):
        """
        根据每月社保缴纳人数，平均缴费基数计算评分
        """
        peopel_ave = 15
        money_ave = 3200
        def sigmoid(x):
            return 1/(1+math.exp(-0.5*(x/peopel_ave-1)))
            
        def sigmoid2(x):
            return 1/(1+math.exp(-3*(x/money_ave-1)))
            
        temp = self.company_medical_fund_payment_info[self.company_medical_fund_payment_info['对应费款所属期']==in_time]

        num_of_insurance_people = len(temp)
        base_contrib_ave = int(temp['人员缴费基数'].mean())
        
        insurance_people_score = sigmoid(num_of_insurance_people)
        base_contrib_score = sigmoid2(base_contrib_ave)
        
        return round(insurance_people_score,2),round(base_contrib_score,2)
        
    def count_insurance_delay_score(self,in_time):
        """
        根据企业最近12个月的社保缴纳延时情况，计算评分（应缴，实缴）
        """
        pass
            
    #企业发展评分(员工增长率，员工离职率，人才引进数，人才申报数)
    def count_worker_grow_rate_score(self,in_time):
        """
        根据每月企业新增员工数和离职员工数，计算评分
        员工增长率，离职率
        """
        from pandas.tseries.offsets import DateOffset
        from datetime import datetime
        from sklearn import linear_model
        
        end = datetime.strptime(str(in_time),'%Y%m')
        start = end + DateOffset(months=-11)
        df_all_time = self.company_all_time_data.copy()
        df_all_time['time'] = df_all_time['time'].apply(lambda i:datetime.strptime(str(i),'%Y%m'))
        df_all_time.set_index('time',inplace=True)
        start_all_time = df_all_time.index[0]
        if start >= start_all_time:
            num_list_data = df_all_time[start:end]['num_of_contrib'].values.tolist()
            if len(num_list_data)<12:
                num_list_data = num_list_data+[num_list_data[-1]]*(12-len(num_list_data))
        else:
            num_list_data = df_all_time[start_all_time:end]['num_of_contrib'].values.tolist()
            num_list_data = [num_list_data[0]]*(12-len(num_list_data))+num_list_data
        y_train = np.asarray(num_list_data).reshape(len(num_list_data),1)-num_list_data[0]
        x_train = np.arange(12).reshape(12,1)
        regr = linear_model.LinearRegression()
        regr.fit(x_train,y_train)
        coef = regr.coef_.ravel()[0]

        #new_add_worker = df_all_time[df_all_time['time']==in_time]['new_add_employee'].values[0]
        #leave_worker = df_all_time[df_all_time['time']==in_time]['rm_employee'].values[0]
        #num_contrib = df_all_time[df_all_time['time']==in_time]['num_of_contrib'].values[0]
        #大中小微企业划分标准
        #x<10 微型    10<=x<100 小型   100<=x<300 中型  x>=300 大型
        if np.mean(num_list_data)<10:
            coef = 0.1*coef
        elif np.mean(num_list_data)>=10 and np.mean(num_list_data)<100:
            coef = 0.3*coef
        elif np.mean(num_list_data)>=100 and np.mean(num_list_data)<300:
            coef = 0.7*coef
        else:
            coef = coef
   
        def sigmoid(x):
            return 1/(1+math.exp(-50*x))
        comp_condition_score = sigmoid(coef)
        if comp_condition_score<0:
            comp_condition_score = 0

        return round(comp_condition_score,2)
    
    def count_worker_dimission_rate_score(self,in_time):
        """
        根据企业员工的离职率，计算评分
        """
        pass
    
    def count_brain_gain_socre(self,in_time):
        """
        根据企业最近12个月的人才引进数，计算评分
        """
        pass
    
    def count_talent_declare_score(self,in_time):
        """
        根据企业最近12个月的人才申报数，计算评分
        """
        pass
    
    #企业运营评分    
    def count_unemployment_score(self,in_time):
        """
        根据失业人口，计算评分
        """
        def mytanh(x):
            return math.tanh(-0.2*x)+1
        unemployment_info = self.company_unemployment_info
        num_of_unemployment = len(unemployment_info[unemployment_info['time']==in_time])
        unemployment_score = mytanh(num_of_unemployment)
        
        return round(unemployment_score,2)
        
    def count_injury_score(self,in_time):
        """
        根据企业最近12个月工伤情况，计算评分
        """
        def mytanh(x):
            return math.tanh(-0.5*x)+1
        injury_info = self.company_injury_info
        injury_level_list = injury_info[injury_info['time']==in_time]['injury_level'].values.tolist()
        if len(injury_level_list)>0:
            injury_level_list = [4-x for x in injury_level_list]
            level_all = sum(injury_level_list)
        else:
            level_all = 0
        injury_score = mytanh(level_all)        
        return round(injury_score,2)
    
    def count_worker_train_score(self):
        """
        根据企业的员工培训情况，计算评分
        """
        pass
    
    #企业员工合理性评分（年龄结构，学历分布）
    def count_age_structure_score(self):
        """
        根据企业员工年龄合理性评分
        """
        pass
    
    
    def count_degree_score(self):
        """
        根据企业员工的学历分布，计算评分
        """
        pass
    
    
    #企业员工健康情况评分（年人均医疗消费金额,年人均医疗次数，大额医疗支出员工占比）
    def count_ave_medical_spend_score(self,in_time):
        """
        根据企业员工年人均医疗消费金额评分
        """
        def sigmoid(x):
            return 1/(1+math.exp(-3*(x/ave_cost-1)))
        in_year = str(in_time)[:4]
        in_month= int(str(in_time)[4:])
        ave_cost = 100*in_month#假设平均每人月医疗支出100元
        df_medical = self.company_workers_health_cds()
        cost_in_year = df_medical[df_medical['SYEAR']==in_year]\
                                      ['ave_cost'].values[0]
        score = 1-sigmoid(cost_in_year)
        return score
                            
    def count_ave_medical_nums_score(self,in_time):
        """
        根据企业员工年人均医疗次数评分
        """
        def sigmoid(x):
            return 1/(1+math.exp(-3*(x/ave_num-1)))
        in_year = str(in_time)[:4]
        in_month= int(str(in_time)[4:])
        ave_num = 6#假设平均每人月医疗支出100元
        df_medical = self.company_workers_health_cds()
        cost_in_year = df_medical[df_medical['SYEAR']==in_year]\
                                      ['ave_cishu'].values[0]
        score = 1-sigmoid(cost_in_year)
        return score
    
    def count_large_amount_score(self):
        """
        根据企业大额医疗支出员工占比评分
        """
        pass

        
############################################################################################################  

    def count_base_cond_score(self,type_score,sector_score,duration_score,
                              type_weight=0.34,sector_weight=0.33,duration_weight=0.33):
        """
        根据单位性质评分和所属行业平分，计算基本情况得分
        """
        return round(type_weight*type_score+sector_weight*sector_score+\
                     duration_weight*duration_score,2)*100


    def count_credit_cond_score(self,judge_score,c_score,judge_weight=0.5,c_weight=0.5):
        """
        根据仲裁案件数评分和失信人员评分，计算信用得分
        """
        return round(judge_weight*judge_score+c_weight*c_score,2)*100
    

    def count_fund_cond_score(self,num_score,base_score,delay_score,
                              num_weight=0.34,base_weight=0.33,delay_weight=0.33):
        """
        根据社保基金缴费人数评分，缴费基数评分，缴纳延时率评分，计算社保缴费情况
        """
        return round(num_weight*num_score+base_weight*base_score\
                     +delay_weight*delay_score,2)*100
        
    
    def count_development_cond_score(self,rise_score,leave_score,t_in_score,t_dcl_score,
                                rise_w=0.25,leave_w=0.25,t_in_w=0.25,t_dcl_w=0.25):
        """
        根据员工增长率评分，员工离职率评分，人才引进数评分，人才申报数评分，计算企业发展评分
        """
        return round(rise_w*rise_score+leave_w*leave_score+t_in_w*t_in_score\
                     +t_dcl_w*t_dcl_score,2)*100
        
                     
    def count_manage_cond_score(self,l_score,i_socre,train_score,
                                l_w=0.34,i_w=0.33,train_w=0.33):
        """
        根据单位的失业员工数评分，工伤员工数评分，员工培训情况评分，计算单位的经营情况评分
        """
        return round(l_w*l_score+i_w*i_socre+train_w*train_score,2)*100  

    
    def count_structure_cond_socre(self,age_score,degree_score,age_w=0.5,degree_w=0.5):
        """
        根据员工年龄结构，学历分布，计算单位的员工结构合理度评分
        """
        return round(age_w*age_score+degree_w*degree_score)*100
        
    def count_health_cds_cond_score(self,ave_cost_score,ave_num_score,large_amount_score,
                               ave_cost_w=0.34,ave_num_w=0.33,large_amount_w=0.33):
        """
        根据员工健康情况，计算员工健康情况评分
        """
        return round(ave_cost_w*ave_cost_score+ave_num_w*ave_num_score+\
                     large_amount_w*large_amount_score,2)*100
    
                     
    def count_comp_total_score(self,base_score,credit_score,fund_score,
                               development_score,manage_score,structure_socre,
                               health_cds_score,base_w=0.14,credit_w=0.14,
                               fund_w=0.14,development_w=0.14,manage_w=0.14,
                               structure_w=0.14,health_cds_w=0.14):
        """
        根据企业的基本情况，信用状况，社保缴费情况，发展情况，运营状况，人员结构合理度，人员
        健康情况，计算企业的综合评分
        """
        return math.floor(round(base_w*base_score+credit_w*credit_score\
                                +fund_w*fund_score+development_w*development_score\
                                +manage_w*manage_score+structure_w*structure_socre\
                                +health_cds_w*health_cds_score,2))
        
    
    def sumary_all_score(self,in_time):
        """
        汇总企业所有的评分情况
        
        Returns
        -------
        list
        """
        a1 = self.count_comp_type_score()
        a2 = self.count_sector_score()
        a3 = self.count_duration_score()
        a4 = self.count_judge_score(in_time)
        a5 = self.count_fraud_score(in_time)
        a6,a7 = self.count_insurance_contrib_score(in_time)
        a8 = self.count_insurance_delay_score(in_time)
        a9 = self.count_worker_grow_rate_score(in_time)
        a10 = self.count_worker_dimission_rate_score(in_time)
        a11 = self.count_brain_gain_socre(in_time)
        a12 = self.count_talent_declare_score(in_time)
        a13 = self.count_unemployment_score(in_time)
        a14 = self.count_injury_score(in_time)
        a15 = self.count_worker_train_score(in_time)
        a16 = self.count_age_structure_score(in_time)
        a17 = self.count_degree_score(in_time)
        a18 = self.count_ave_medical_spend_score(in_time)
        a19 = self.count_ave_medical_nums_score(in_time)
        a20 = self.count_large_amount_score(in_time)
        
       
        b1 = self.count_base_cond_score(a1,a2,a3)
        b2 = self.count_credit_cond_score(a4,a5)
        b3 = self.count_fund_cond_score(a6,a7,a8)
        b4 = self.count_development_cond_score(a9,a10,a11,a12)
        b5 = self.count_manage_cond_score(a13,a14,a15)
        b6 = self.count_structure_cond_socre(a16,a17)
        b7 = self.count_health_cds_cond_score(a18,a19,a20)
       
        score_list = [b1,b2,b3,b4,b5,b6,b7]
        score_list = list(map(lambda x:5*x,score_list))
        c1 = self.count_comp_total_score(b1,b2,b3,b4,b5,b6,b7)
        return [[a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20],
                [b1,b2,b3,b4,b5,b6,b7],
                [c1]]
        

    def score_time_periods(self):
        """
        计算各时间段的评分
        """


        if self.start_time == self.end_time:
            times = [self.start_time]
        else:
            times = self.get_all_times()
            
        df_all_time = pd.DataFrame()

        for i in times:
            scores = self.sumary_all_score(i)
            df_one_time = pd.DataFrame({'time':[i],
                                        'credit_score':[scores[1][0]],
                                        'base_cond_score':[scores[1][1]],
                                        'fund_cond_score':[scores[1][2]],
                                        'safe_stab_cond_score':[scores[1][3]],
                                        'manage_cond_score':[scores[1][4]],
                                        'total_score':[scores[2][0]]})
            df_all_time = pd.concat([df_all_time,df_one_time])
   
        if df_all_time.empty:
            return pd.DataFrame()
        else:
            df_all_time['company_id'] = self.company_id
            df_all_time = df_all_time[['company_id','time','credit_score','base_cond_score',\
                                       'fund_cond_score','safe_stab_cond_score','manage_cond_score','total_score']]
            return df_all_time
        
    
    def company_portrait_time(self):
        """
        汇总时间数据
        """
        df_all_time = self.company_all_time_data
        if df_all_time.empty:
            return pd.DataFrame()
        else:
            all_times = df_all_time['time'].values.tolist()

        if self.start_time==self.end_time:
            if self.start_time not in all_times:
                return pd.DataFrame()
        df_score_time = self.score_time_periods()
        if df_score_time.empty:
            return pd.DataFrame()
            
        df = pd.merge(df_all_time,df_score_time,
                      left_on=['company_id','time'],right_on = ['company_id','time'],how='inner')
        df = df[df['time']>=self.start_time+100]
        return df
       
            
    def convert2lb(self,df_origin):
        """
        将数据转换为LB需要的格式
        """
        if df_origin.empty or df_origin is None:
            return pd.DataFrame()
        df = df_origin.copy()
        df['SYEAR'] = df['time'].apply(lambda i:str(i)[:4])
        df['SQUARTER'] = df['time'].apply(lambda i:str(math.ceil(int(str(i)[4:6])/3)))
        df['SMONTH'] = df['time'].apply(lambda i:str(i)[4:6])  
        df['XVAL'] = None
        df['NOTE'] = None
        df.drop('time',axis=1,inplace=True)
        df.rename(columns={'company_id':'ITEMID',
                   'num_of_contrib':'AAB001',
                   'new_add_employee':'AAB002',
                   'rm_employee':'AAB003',
                   'ave_pay_fund':'AAB004',
                   'month_total_fund_pay':'AAB005',
                   'man_num':'AAB012',
                   'woman_num':'AAB013',
                   'ageslice1':'AAB014',
                   'ageslice2':'AAB015',
                   'ageslice3':'AAB016',
                   'ageslice4':'AAB017',
                   'ageslice5':'AAB018',
                   'bendi':'AAB019',
                   'jiangsu':'AAB020',
                   'waidi':'AAB021',
                   'credit_score':'AAB006',
                   'base_cond_score':'AAB007',
                   'fund_cond_score':'AAB008',
                   'safe_stab_cond_score':'AAB009',
                   'manage_cond_score':'AAB010',
                   'total_score':'AAB011'},inplace=True)
        cols = df.columns.tolist()
        cols = cols[0:1]+cols[-5:]+cols[1:-5]
        df = df[cols]
        return df
        
        
#==============================================================================
    class Radar(object):

        def __init__(self, fig, titles, labels, rect=None):
            if rect is None:
                rect = [0.05, 0.05, 0.95, 0.95]
    
            self.n = len(titles)
            self.angles = np.arange(90, 90+360, 360.0/self.n)
            self.angles = [a % 360 for a in self.angles]
            self.axes = [fig.add_axes(rect, projection="polar", label="axes%d" % i) 
                             for i in range(self.n)]
    
            self.ax = self.axes[0]
            self.ax.set_thetagrids(self.angles, labels=titles, fontsize=14)
    
            for ax in self.axes[1:]:
                ax.patch.set_visible(False)
                ax.grid("off")
                ax.xaxis.set_visible(False)
    
            for ax, angle, label in zip(self.axes, self.angles, labels):
                ax.set_rgrids(range(1, 6), angle=angle, labels=label)
                ax.spines["polar"].set_visible(False)
                ax.set_ylim(0, 5)
    
        def plot(self, values, *args, **kw):
            angle = np.deg2rad(np.r_[self.angles, self.angles[0]])
            values = np.r_[values, values[0]]
            self.ax.plot(angle, values, *args, **kw)    
            
            
    def plot(self,score_list):
        """
        根据5个方面的评分，作雷达图
        """
        score_list = list(map(lambda x:5*x,score_list))
        fig = pl.figure(figsize=(4, 4))

        titles = ['credit_cond','base_cond','fund_cond','safestab_cond','manage_cond']
        
        labels = [
            [1,2,3,4,5],
            [1,2,3,4,5],
            [1,2,3,4,5],
            [1,2,3,4,5],
            [1,2,3,4,5]
        ]
        radar = self.Radar(fig, titles, labels)
        radar.plot(score_list,  "-", lw=2, color="b", alpha=0.4, label="score")
        radar.ax.legend()
        plt.show()
        
        
        
#==============================================================================================
    
if __name__=='__main__':
    platform = sys.platform
    if platform=='linux':
        engine_szrs = sqlalchemy.create_engine("oracle://szythrd:szythrd912@10.39.43.72:1521/szsbbak?charset=utf-8")
    elif platform=='win32':
        engine_szrs = sqlalchemy.create_engine("oracle://szythrd:szythrd912@192.168.90.14:1521/szsbbak?charset=utf-8")
    engine_analysis_db = sqlalchemy.create_engine("postgresql://liyi:123456@172.16.102.24:5432/db_szrs")
    XCSP = "(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.90.5)\
    (PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = xcsb)))"
    engine_lb = sqlalchemy.create_engine("oracle+cx_oracle://LBCHART:Rjjzase!p02@%s"%XCSP)
    
    mode='a'
    INPUT_TIME=int(201508)
    company_id = '403279192'
    if mode=='a':
        START_TIME = 201301
        END_TIME = int(time.strftime('%Y%m',time.localtime(time.time())))       
    elif mode=='b':
        START_TIME = END_TIME = int(time.strftime('%Y%m',time.localtime(time.time())))
    elif mode=='c':
        START_TIME = END_TIME = INPUT_TIME
    comp = Company(company_id,
                       engine_szrs=engine_szrs,
                       engine_analysis_db=engine_analysis_db,
                       engine_lb=engine_lb,
                       start_time=START_TIME,
                       end_time=END_TIME)    
    
    #test3 = comp.unemployment_info()
    #comp.count_comp_condition(201708) 
   
#==============================================================================
#    test1 = comp.company_time_period_data()
#    test2 = comp.get_all_times()
#    test3 = comp.company_time_period_data()
#    score = comp.sumary_all_score(201709)
#    comp.plot(score[1])
#==============================================================================
#==============================================================================
#     a = comp.company_portrait_time()
#     b = comp.convert2lb(a.copy())
#     c = comp.company_all_time_data
#     
#==============================================================================
#==============================================================================
#     a = comp.medical_fund_payment_info()
#     b = comp.mature_fund_payment_info()
#     c = comp.health_care_spending_info()
#     d = comp.company_base_info
#     e = comp.injury_info()
#     f = comp.unemployment_info()
#==============================================================================
   # g = comp.company_time_period_data()
    
    #h = comp.score_time_periods()
    #ii = comp.company_portrait_time()
    #j = comp.convert2lb(ii)
    #test3 = comp.get_all_workers()
    #test1,test2 = comp.company_worker_info(comp.get_all_workers(),201709)
    #test2 = comp.company_fund_spending_info
    #test3 = comp.company_medical_fund_payment_info
    test5 = comp.company_workers_health_cds()
    test1 = comp.count_ave_medical_spend_score(201409)
    test2 = comp.count_ave_medical_nums_score(201709)
    
#%%
XCSP = "(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.90.5)\
    (PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = xcsb)))"
engine_lb2 = sqlalchemy.create_engine("oracle+cx_oracle://XCSP:Zgwlcdj!p02@%s"%XCSP)
sql = "select a.*,c.aab001,c.aab004 from fa01 a,ab01 b,szyth.ab01@sztbk c  where a.aab001=b.aab001 and b.aab004=c.aab004"        
df = pd.read_sql_query(sql,engine_lb2)
    