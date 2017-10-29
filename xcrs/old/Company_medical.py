# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 11:28:27 2017

@author: lisc1
"""

import pandas as pd
import sqlalchemy
import numpy as np
import plotly
#import plotly.plotly as py
#import cufflinks as cf
import plotly.graph_objs as go
#import os
import datetime
#plotly.offline.init_notebook_mode(connected=False)
#cf.set_config_file(world_readable=True,offline=True)
#os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
#plotly.tools.set_credentials_file(username='lisc1991', api_key='xy1wAWlY1gA4Gx6Myp3a')

def plot_pay_heatmap(company_id,engine):
    """
    公司缴费热度图
    
    para:
        company_id:单位ID
        engine:数据库连接
    
    return:
        作图对象
    """
    sql = "select to_char(AAB001) 单位ID,AAE002 费款所属期,AAE003 对应费款所属期,AAB119 应缴人数 from AB07 where AAB001=%s and AAE140='310'"%company_id
    
    df_pay = pd.read_sql_query(sql,engine)
    d1 = df_pay[['费款所属期','应缴人数']].groupby(['费款所属期']).应缴人数.sum().to_frame()
    d2 = df_pay[['对应费款所属期','应缴人数']].groupby(['对应费款所属期']).应缴人数.sum().to_frame()
    x = [datetime.datetime.strptime(str(x),'%Y%m') for x in d1.index.values.tolist()]
    y = ['费款所属期','对应费款所属期']
    z = [d1['应缴人数'].values.tolist(),d2['应缴人数'].values.tolist()]
    data = [
        go.Heatmap(
            z=z,
            x=x,
            y=y,
            ygap=10,
            colorbar=dict(thickness=30),
            colorscale='Viridis',
            )
    ]

    layout = go.Layout(
        title='医保缴费对应人数',
        xaxis = dict(ticks='', nticks=36),
        yaxis = dict(ticks='' )
    )

    fig = go.Figure(data=data, layout=layout)
    return(plotly.offline.plot(fig))

    

companyAndworker = pd.read_csv('../企业用户画像/companyAndworker.csv')
companyAndworker['单位ID'] = companyAndworker['单位ID'].astype('str')
companyAndworker['员工ID'] = companyAndworker['员工ID'].astype('str')            
    
def merge_in_out(company,engine,workers=None):
    """
    生成单位ID对应的人员社保缴费以及医保支出信息.
    
    para:
        company:str
        engine:数据库连接
        workers:人员ID
    return:
        df_merge: DataFrame 人员ID', '年月', '统筹基金支付', '单位ID', '费款所属期', '对应费款所属期', '缴费月数', '人员缴费基数'
    """
    if workers is None:
        workers = companyAndworker[companyAndworker['单位ID']==str(company)]['员工ID'].values.tolist()
    else:
        workers = workers
  
    df_jiesuan = pd.DataFrame()
    for worker in workers:

        sql = "select to_char(KC12.AAC001) 人员ID,KC12.CKE363 险种分类,KC12.AKC264 费用总额,KC12.AKB068 \
               统筹基金支付,KC12.AKC200 本年度住院次数,KC12.CKC299 住院累计,KC12.CKE011 交易日期,KC12.AAE149 年月 from KC12 where KC12.AAC001=%d"%int(worker)

        df_temp = pd.read_sql_query(sql,engine)
        df_jiesuan = pd.concat([df_jiesuan,df_temp])
  
    df_temp = df_jiesuan[df_jiesuan['统筹基金支付']>0].sort_values(by=['统筹基金支付'],ascending=False)
    df_temp = df_temp.groupby(['人员ID','年月']).统筹基金支付.sum().to_frame().reset_index()
    
    df_jiao_all = pd.DataFrame()
    for worker in workers:
        sql_jiaofei = "select to_char(AAB001) 单位ID,to_char(AAC001) 人员ID,AAE002 费款所属期,AAE003 对应费款所属期,AAE201 缴费月数,AAE180 人员缴费基数 \
        from MC43 where AAB001=%d and AAC001=%d order by 对应费款所属期"%(int(company),int(worker))
        df_jiao = pd.read_sql_query(sql_jiaofei,engine)
        df_jiao_all = pd.concat([df_jiao_all,df_jiao],axis=0)
    
    df_merge = pd.merge(df_temp,df_jiao_all,left_on=['人员ID','年月'],right_on=['人员ID','对应费款所属期'],how='right').sort_values(by=['人员ID','对应费款所属期']).fillna(0)
    
    return df_merge
    
    
def find_fund_pay_worker(company,min_fund_pay,engine):
    """
    找出符合条件的员工id
    
    para:
        company:str 单位ID
        min_fund_pay:int 最低统筹基金支付
        engine:数据库连接
    
    return:    
        workers:list 符合条件的员工ID
    """
    df_merge = merge_in_out(company,engine)
    df = df_merge.groupby(['人员ID']).统筹基金支付.sum().to_frame()
    workers = df[df['统筹基金支付']>min_fund_pay].index.values.tolist()
    return workers

    
def generate_data(workers,company,engine):
    """
    生成热度图所需数据  
    
    para:
        workers:嫌疑人员ID，type:list,str
        company:单位ID type:str
        engine:数据库连接
        
    return:
        x,y,z 热度图所需数据
    """
    df_merge = merge_in_out(company,engine,workers)
    
    if len(workers)==0 or df_merge.empty:
        return None,None,None
         
    min_date = df_merge[df_merge['人员ID']==workers[0]]['对应费款所属期'].min()
    max_date = df_merge[df_merge['人员ID']==workers[0]]['对应费款所属期'].max()
    for worker in workers:
        temp_min_date = df_merge[df_merge['人员ID']==worker]['对应费款所属期'].min()
        temp_max_date = df_merge[df_merge['人员ID']==worker]['对应费款所属期'].max()
        if temp_min_date<min_date:
            min_date = temp_min_date
        if temp_max_date>max_date:
            max_date = temp_max_date
#     print('min_date=',min_date,',','max_date=',max_date)
    x = pd.date_range(start=str(min_date)+'01',end=str(max_date)+'01',freq='MS')
    y = ['ID:'+worker for worker in workers]
    z = []
    def count_month_intevel(x,y):
        return abs(12*(int(str(x)[:4])-int(str(y)[:4]))+(int(str(x)[4:6])-int(str(y)[4:6])))
    for worker in workers:
        temp_min_date = df_merge[df_merge['人员ID']==worker]['对应费款所属期'].min()
        temp_max_date = df_merge[df_merge['人员ID']==worker]['对应费款所属期'].max()
#         print('temp_min_date=',temp_min_date,',',' temp_max_date=',temp_max_date)
        low_range = count_month_intevel(temp_min_date,min_date)
        high_range = count_month_intevel(temp_max_date,max_date)
#         print(low_range,',',high_range)
        z_temp = [None]*low_range+df_merge[df_merge['人员ID']==worker]['统筹基金支付'].values.tolist()+[None]*high_range
#         print(z_temp)
        z.append(z_temp)
    return x,y,z
 
    
def plot_worker_medical(x,y,z):
    """
    缴费支出时序热度图
    
    para:
        x:x轴数据
        y:y轴数据
        z:z轴数据
    
    return:作图对象
        
    """
    if x is None or y is None or z is None:
        print('nobody~~')
        return None
    data = [
        go.Heatmap(
            z=z,
            x=x,
            y=y,
            ygap=8,
            colorscale='Viridis',
            )
    ]

    layout = go.Layout(
        title='缴费支出时序图',
        xaxis = dict(ticks='', nticks=len(x)),
        yaxis = dict(ticks='' ),
    )
    
    fig = go.Figure(data=data, layout=layout)
    return(plotly.offline.plot(fig))



def find_person_of_interest(company,enine,min_fund_value=5000,interval_month=4,max_times=2):
    """
    疑似带病参保嫌疑人检测
    嫌疑人规则1：首次缴费3个月内发生大额基金支付行为（支付大于5000）,医保基金支付大于5000的次数大于等于2次，总额大于x元
    嫌疑人规则2：
    
    para:
        company:str 单位ID
        min_fund_value:int 认定为有嫌疑的最低基金支付金额
        interval_month:int 认定的从社保缴费到社保支出的时间间隔
        max_times:int 认定为有嫌疑的大于最低基金支付金额的次数
        engine:数据库连接
        
    return:
        suspect_list:list 嫌疑人名单
    """
    df_merge = merge_in_out(company,engine)
    if df_merge.empty:
        return []
    

    suspect_list = []  
    workers = df_merge[df_merge['统筹基金支付']>=min_fund_value]['人员ID'].drop_duplicates().values.tolist()
    
    def count_month_intevel(x,y):
        return 12*(int(str(x)[:4])-int(str(y)[:4]))+(int(str(x)[4:6])-int(str(y)[4:6]))
        
    for worker in workers:
        fund_date_list = df_merge[((df_merge['人员ID']==worker)&(df_merge['统筹基金支付']>min_fund_value))]['对应费款所属期'].values.tolist()
        if len(fund_date_list)>=max_times:
            first_fund_date = fund_date_list[0] 
            first_in_date = df_merge[df_merge['人员ID']==worker]['对应费款所属期'].values.tolist()[0]
            if count_month_intevel(first_fund_date,first_in_date)<=interval_month:
                suspect_list.append(worker)
    return suspect_list

    
def generate_all_company_suspect_list(companys,engine,min_fund_value=5000,interval_month=4,max_times=2):
    """
    生成相城区所有符合带病参保行为的名单
    
    para:
        companys:list 相城区所有企业ID
        engine:数据库连接
        
    return:
        company_suspect_list:dict
        
    """
    company_suspect_dict = {}
    for company in companys:
        suspect_list = find_person_of_interest(company,engine,min_fund_value,interval_month,max_times)
        if len(suspect_list)>0:
            company_suspect_dict[company] = suspect_list
    return company_suspect_dict

    
if __name__=='__main__':
    engine=sqlalchemy.create_engine("oracle://szyth:szyth11@192.168.90.60:1521/szxcsbbk?charset=utf-8")
    #plot_pay_heatmap('3003538156',engine)
    company = '2001100110'
    min_fund_value = 5000
    interval_month = 4
    max_times = 2
    #df_merge = merge_in_out(company,engine)

    test4 = merge_in_out(company,engine)
    
#==============================================================================
#     workers = find_person_of_interest(company,engine)
# 
#     x,y,z = generate_data(workers,company,engine)
#     plot_worker_medical(x,y,z)
#==============================================================================
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    