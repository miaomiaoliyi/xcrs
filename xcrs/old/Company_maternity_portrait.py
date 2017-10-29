# -*- coding: utf-8 -*-
"""
Created on Tue Sep  5 17:23:54 2017

@author: lisc1
"""
import numpy as np
import pandas as pd
import sqlalchemy
import os

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def generate_attr_final(engine,company_id):
    
    """
    生成单位的属性
    统计特征包括：员工人数，流动周期 ，男员工人数 ，女员工人数 ，怀孕后参保人数，成立至今，本地户籍，外地户籍，男_年龄，女_年龄
                领取生育津贴人数，领取津贴额 ，缴费期数中位数
                
    参数
    -----------------
    engine:数据库连接对象
    
    返回值
    -------------------
    df_final:DataFrame
    
    """
    
    sql = "select to_char(AB.AAB001) 单位ID,AB.AAE047 成立日期,to_char(AC01.AAC001) 人员ID,\
    AC01.AAC147 证件号码,AC01.AAC004 性别,AC01.AAC006 出生日期,MC43.AAE002 缴费所属期,MC43.AAE003 对应缴费所属期 \
    from AC01,MC43,(select * from AB01 where AAB001=%d) AB \
    where AB.AAB001=MC43.AAB001 and MC43.AAC001=AC01.AAC001"%int(company_id)
    df_fundmental = pd.read_sql_query(sql,engine)
    
    #缴费期数，员工人数
    df_fundmental['缴费期数']=0
    df1 = df_fundmental.groupby(['单位ID','人员ID'],as_index=False)[['缴费期数']].count()
    df2 = df1.groupby('单位ID')[['人员ID']].count()
    df2.rename(columns={'人员ID':'员工人数'},inplace=True)
    
    df_final = df1.groupby('单位ID')[['缴费期数']].median()
    df_final.rename(columns={'缴费期数':'缴费期数中位数'},inplace=True)
    df_final['员工人数'] = df2['员工人数']
    
    # 计算各商户在MC43中有缴费记录的男女员工人数
    a = df_fundmental[['单位ID','人员ID','性别']].drop_duplicates()
    b = a[a['性别']=='1'].groupby('单位ID')['性别'].count().to_frame()
    c = a[a['性别']=='2'].groupby('单位ID')['性别'].count().to_frame()
    b['女员工人数'] = c
    b.rename(columns={'性别':'男员工人数'},inplace=True)
    b[b['女员工人数'].isnull()]=0
    b['男员工人数'] = a[a['性别']=='1'].groupby('单位ID')['性别'].count().to_frame()
    b['女员工人数'] = b['女员工人数']
    df_final[['男员工人数','女员工人数']]=b
    
    #成日至今(月)
    df_time = df_fundmental[['单位ID','成立日期']].drop_duplicates()
    df_time.set_index('单位ID',inplace=True)
    
    def count_month(day):
        from datetime import datetime
        import math
        if day is None:
            return np.nan
        if np.isnan(day) or len(str(int(day)))<8:
            return np.nan    
        year = int(str(day)[:4])
        month = int(str(day)[4:6])
        day1 = int(str(day)[6:8])
        now = datetime.now()
        delta = datetime(now.year,now.month,now.day)-datetime(year,month,day1)
        return math.ceil(delta.days/30)
        
    df_time['成立至今'] = df_time['成立日期'].apply(count_month)
    
    df_final['成立至今'] = df_time['成立至今']
    
    
    company_id = list(set(df_fundmental['单位ID']))
    result={}
    for company in company_id:
        workers = df1[df1['单位ID']==company]['人员ID']
        preg_num=0
        age_man = []
        age_woman = []
        liudong_fre = []
        get_allowance = []
        huji_bendi = 0
        huji_waidi = 0
        num_of_get_allowance = 0
        for worker in workers:
    
                
            #人员流动分析（人员流动周期）,人员年龄分布（男员工，女员工）,人员户籍属性(本地，外地)    
            temp = df_fundmental[((df_fundmental['单位ID']==company)&(df_fundmental['人员ID']==worker))].sort_values(by=['对应缴费所属期'],ascending=False)
            #年龄(这里采用的年龄计算方法是：最近的对应缴费期数-出生日期的前4位)
            age = int(str(temp['对应缴费所属期'].iloc[0])[:4])-int(str(temp['出生日期'].iloc[0])[:4])
            if temp['性别'].iloc[0]=='1':
                age_man.append(age)
            else:
                age_woman.append(age)
                
            #户籍
            if temp.iloc[0,3][:4] =='3205':
                huji_bendi +=1
            else:
                huji_waidi +=1
            #流动周期 
#             liudong = 12*(int(str(temp['对应缴费所属期'].iloc[0])[:4])-int(str(temp['对应缴费所属期'].iloc[-1])[:4]))+int(str(temp['对应缴费所属期'].iloc[0])[4:6])-int(str(temp['对应缴费所属期'].iloc[-1])[4:6])
            liudong = len(temp)
            liudong_fre.append(liudong)
            
            if temp['性别'].iloc[0]=='2':
                temp2 = pd.read_sql_query('select AMC030 生育津贴金额 from MBA7 where AAB001=%d and AAC001=%d'%(int(company),int(worker)),engine)
                if not temp2.empty:
                    num_of_get_allowance+=1
                    get_allowance.append(temp2['生育津贴金额'].values[0])
                    

        man_ave_age = round(np.array(age_man).mean(),1)
        woman_ave_age = round(np.array(age_woman).mean(),1)
        staff_Flow_cycle = np.median(np.array(liudong))
        allowance_sum = np.array(get_allowance).sum()
        result[company]={'流动周期中位数':staff_Flow_cycle,'本地户籍':huji_bendi,'外地户籍':huji_waidi,'男职工年龄均值':man_ave_age,'女职工年龄均值':woman_ave_age,
                        '怀孕后参保人数':preg_num,'领取生育津贴人数':num_of_get_allowance,'领取津贴总额':allowance_sum}
    df_result = pd.DataFrame(result).T
    df_result.index.name = '单位ID'
    
    if df_result.empty or df_final.empty:
        return None

    df_final[['本地户籍','外地户籍','男职工年龄均值','女职工年龄均值','流动周期中位数','怀孕后参保人数','领取生育津贴人数','领取津贴总额']] = df_result[['本地户籍','外地户籍','男职工年龄均值','女职工年龄均值','流动周期中位数','怀孕后参保人数','领取生育津贴人数','领取津贴总额']]
    return df_final

    
def generate_dict_industry_large(df_ind_large):
    """
    构建行业大类字典，并将行业大类str->num
    
    参数
    ---------------------------
    df_ind_large:Series 行业大类
    
    返回
    --------------------------
    df_ind_large:Series 行业大类
    
    """
    dict_hydl = {}
    idc_hydl = {}
    list_hydl = list(set(df_ind_large))
    
    for i in range(len(list_hydl)):
        dict_hydl[i] = list_hydl[i]
        idc_hydl[list_hydl[i]] = i
        
    def str_to_num(categ):
        return idc_hydl[categ]
    
    df_ind_large = df_ind_large.apply(str_to_num).to_frame()
    return df_ind_large
  
    
def generate_dict_industry_small(df_ind_small):
    """
    构建行业小类字典，并将行业小类str->num
    
    参数
    ---------------------------
    df_ind_small:Series 行业大类
    
    返回
    --------------------------
    df_ind_small:Series 行业大类
    
    """

    dict_hyxl = {}
    idc_hyxl = {}
    list_hyxl = list(set(df_ind_small))
    
    for i in range(len(list_hyxl)):
        dict_hyxl[i] = list_hyxl[i]
        idc_hyxl[list_hyxl[i]] = i
        
    def str_to_num2(categ):
        return idc_hyxl[categ]

    df_ind_small = df_ind_small.apply(str_to_num2).to_frame()
    return df_ind_small

def generate_all_attr2():
    """
    全体特征集的汇总
    
    返回
    -----------------
    df_temp:DataFrame
    
    """
    engine=sqlalchemy.create_engine("oracle://szyth:szyth11@192.168.90.60:1521/szxcsbbk?charset=utf-8")
    company_id = generate_company_id(engine)
    
    df_final = pd.DataFrame()
    for company in company_id[:100]:
        df = generate_attr_final(engine,company)
        if df is not None:
            if df_final.empty:
                df_final = df
            else:
                df_final = df_final.append(df)
    
    _ = df_final.reset_index(inplace=True)
    df_final[['男员工人数','女员工人数']] = df_final[['男员工人数','女员工人数']].fillna(-1).apply(np.int64).replace(-1,np.nan)
    df_final = df_final[['单位ID','缴费期数中位数','员工人数','男员工人数','女员工人数','成立至今','本地户籍','外地户籍','男职工年龄均值','女职工年龄均值','流动周期中位数','怀孕后参保人数','领取生育津贴人数','领取津贴总额']]
    return df_final

 
def generate_company_id(engine):
    sql = "select to_char(AAB001) 单位ID,CAB009 经营状态 from AB01 where AAB019 not in ('100','600') and AAA027='320507'"
    df_company = pd.read_sql_query(sql,engine)  
    company_id = df_company['单位ID'].values.tolist()
    
    return company_id
    
    
def data2db(df,table_name):
    """
    将抽取的企业画像特征，写入数据库的对应表中
    
    参数
    -------------------------
    df:DataFrame 待写入的特征集
    table_name:str 待写入的表名
    #engine:str 待写入的数据库的连接

    """
    engine = sqlalchemy.create_engine("postgresql://liyi:123456@172.16.102.24:5432/db_szrs")
    conn = engine.connect()
    df.to_sql(table_name,conn,if_exists='append',index=False)


    
if __name__=='__main__':
    #df_final = generate_all_attr2() 
    table_info = {['company_portrait'}
    data2db(df_final,'company_portrait')



