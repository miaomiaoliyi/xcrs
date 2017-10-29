# -*- coding: utf-8 -*-
"""
Created on Mon Sep 11 13:52:29 2017

@author: lisc1
"""
import sqlalchemy
import numpy as np
import pandas as pd
import math
import Company_maternity_portrait

def clean_data(df_final):
    """
    清洗数据
    """
    # 填补缺失值，男职工年龄均值缺失是由于男员工人数为0
    # 女员工人数为总人数减去男员工人数
    # 设置男员工人数为0的男职工年龄均值为0
    df_final.loc[(np.isnan(df_final['男职工年龄均值'])),'男员工人数'] = 0
    df_final.loc[(np.isnan(df_final['男职工年龄均值'])),'女员工人数'] = df_final.loc[(np.isnan(df_final['男职工年龄均值'])),'员工人数'] - df_final.loc[(np.isnan(df_final['男职工年龄均值'])),'男员工人数']
    df_final.loc[(np.isnan(df_final['男职工年龄均值'])),'男职工年龄均值'] = df_final['男职工年龄均值'].mean()
    
    # 填补缺失值，女职工年龄均值缺失是由于男员工人数为0
    df_final.loc[np.isnan(df_final['女职工年龄均值']),'女职工年龄均值'] = df_final['女职工年龄均值'].mean()
    
    
    #mean_est = (df_final[df_final['成立至今']!=1433]['成立至今']).mean()
    med_est = (df_final[df_final['成立至今']!=1433]['成立至今']).median()
    df_final.loc[df_final['成立至今']==1433,'成立至今'] = med_est
    
    # 成立至今缺失值处理
    df_final.loc[np.isnan(df_final['成立至今']),'成立至今'] = df_final['成立至今'].median()
    
    # 将怀孕后参保人数，领取生育津贴人数变为整形数据
    df_final[['怀孕后参保人数','领取生育津贴人数']] = df_final[['怀孕后参保人数','领取生育津贴人数']].astype('int')
    
    # 年龄均值的处理
    df_final[['男职工年龄均值','女职工年龄均值']] = df_final[['男职工年龄均值','女职工年龄均值']].apply(np.int64)
    
    df_final.drop(['员工人数','领取津贴总额'],axis=1,inplace=True)
    
    #怀孕后参保人数/领取津贴人数
    df_preg_fund_rate = df_final.apply(count_preg_fund_rate,axis=1)
    df_final['怀孕/领取津贴'] = df_preg_fund_rate
 
    ##领取津贴人数/女员工人数
    df_fund_woman_rate = df_final.apply(count_fund_woman_rate,axis=1)
    df_final['领取津贴/女员工'] = df_fund_woman_rate
    
    #怀孕后参保人数/外地户籍人数
    df_preg_foreign_rate = df_final.apply(count_preg_foreign_rate,axis=1)
    df_final['怀孕/外地'] = df_preg_foreign_rate
       
    return df_final
    

#怀孕后参保人数/领取津贴人数
def count_preg_fund_rate(df):
    num_preg,num_get_fund = df['怀孕后参保人数'],df['领取生育津贴人数']
    # 怀孕后参保人数/领取津贴人数
    if num_get_fund!=0 and num_preg!=0:
        preg_fund_rate = num_preg/num_get_fund
    elif num_get_fund==0 and num_preg!=0:
        preg_fund_rate = num_preg
    else:
        preg_fund_rate = 0               
    return round(preg_fund_rate,2)
    
#领取津贴人数/女员工人数
def count_fund_woman_rate(df):
    num_woman,num_get_fund = df['女员工人数'],df['领取生育津贴人数']
    #领取津贴人数/女员工人数
    if num_woman!=0 and num_get_fund!=0:
        fund_woman_rate = num_get_fund/num_woman
    else:
        fund_woman_rate = 0
    return round(fund_woman_rate,4)

#怀孕后参保人数/外地户籍人数 
def count_preg_foreign_rate(df):
    num_foreign,num_preg = df['外地户籍'],df['怀孕后参保人数']
    #怀孕后参保人数/外地户籍人数
    if num_preg!=0 and num_foreign!=0:
        preg_foreign_rate = num_preg/num_foreign
    elif num_preg!=0 and num_foreign==0:
        preg_foreign_rate = num_preg
    else:
        preg_foreign_rate = 0
    return round(preg_foreign_rate,4)
    

    
def score_system(df_final):
    """
    评分函数
    """
    # 混合模型
    # 机器模型：
    # 分类回归树评分模型
    # 专家模型：
    # 1.极大可能从事生育诈骗的公司（80~100）：怀孕/领取津贴率高，怀孕参保人数高
    # 2.发生过怀孕后参保且领取生育津贴行为的公司（60~80）：怀孕/领取津贴率相对较低，怀孕参保人数高 或者怀孕/领取津贴率相对较高，但怀孕参保人数少
    # 3.存在风险的公司（40~60）：存在怀孕后参保人数，但是无领取生育津贴的行为 或者无怀孕后参保,但是领取生育津贴相对员工人数比值偏高的公司
    # 4.暂无风险的公司（0~40）：怀孕后参保人数为0，领取津贴人数为0
    max_preg = df_final['怀孕后参保人数'].max()
    min_preg = df_final['怀孕后参保人数'].min()
    max_preg_fund_rate = df_final['怀孕/领取津贴'].max()
    min_preg_fund_rate = df_final['怀孕/领取津贴'].min()
    max_num_get_fund = df_final['领取生育津贴人数'].max()
    min_num_get_fund = df_final['领取生育津贴人数'].min()
    max_fund_woman_rate = df_final['领取津贴/女员工'].max()
    min_fund_woman_rate = df_final['领取津贴/女员工'].min()
    
    max_num_move_fre = df_final['流动周期中位数'].max()
    min_num_move_fre = df_final['流动周期中位数'].min()
    max_num_foreign = df_final['外地户籍'].max()
    min_num_foreign = df_final['外地户籍'].min()
    
    
    def min_max_scale(x,x_max,x_min):
        return abs((x-x_min)/(x_max-x_min))
    
    def count_fraud_score(ser):
        final_score = 0
        num_preg,num_get_fund,preg_fund_rate,fund_woman_rate= ser['怀孕后参保人数'],ser['领取生育津贴人数'],ser['怀孕/领取津贴'],ser['领取津贴/女员工']
        num_move_fre,num_foreign = ser['流动周期中位数'],ser['外地户籍']
           
        #以有骗保行为，则直接计算骗保行为的严重程度
        if num_preg>0 and num_get_fund>0:
            final_score += 60
            if num_preg>=3 and 0.5<preg_fund_rate<=1:
                final_score +=20
                final_score += 20*math.tanh(3*min_max_scale(preg_fund_rate,max_preg_fund_rate,min_preg_fund_rate)+min_max_scale(num_preg,max_preg,min_preg))
            elif num_preg<3 or preg_fund_rate<0.6:
                final_score += 20*math.tanh(2*min_max_scale(preg_fund_rate,max_preg_fund_rate,min_preg_fund_rate)+min_max_scale(num_preg,max_preg,min_preg))
        #有嫌疑行为，则根据
        elif (num_preg>0 and num_get_fund==0) or (num_preg==0 and fund_woman_rate>0.1):
            final_score = 40+20*math.tanh(min_max_scale(num_preg,max_preg,min_preg)+min_max_scale(num_get_fund,max_num_get_fund,min_num_get_fund)
                                        +min_max_scale(fund_woman_rate,max_fund_woman_rate,min_fund_woman_rate))
        #暂时无嫌疑则根据人员流动周期和外籍人口评分
        else:
            final_score = 40*math.tanh(0.5*(1-min_max_scale(num_move_fre,max_num_move_fre,min_num_move_fre))+0.5*(1-(min_max_scale(num_foreign,max_num_foreign,min_num_foreign))))
        return round(final_score,1)
        
    df_final['评分'] = df_final.apply(count_fraud_score,axis=1)
    df_final.sort_values(by=['评分'],ascending=False,inplace=True)
    return df_final
  
    
    
if __name__=='__main__':
    engine = sqlalchemy.create_engine("postgresql://postgres:123456@localhost:5432/szrs")
    df_final = pd.read_sql_query("SELECT * FROM company_portrait",engine)
    df_final = clean_data(df_final)
    df_final = score_system(df_final)
    Company_maternity_portrait.data2db(df_final,'company')
    
