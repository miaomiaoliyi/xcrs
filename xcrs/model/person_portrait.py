# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 11:08:26 2017

@author: lisc1
"""
import logging
import datetime
import sys
import math

import pandas as pd
import sqlalchemy
import numpy as np
import plotly
import plotly.graph_objs as go


class Person(object):
    
   
    def __init__(self,person_id,engine_szrs,engine_analysis_db):
        """
        初始化信息
        """
        self.engine_szrs = engine_szrs
        self.engine__analysis_db = engine_analysis_db
        self.person_id = person_id
        
        self.person_base_info = self.base_info()
        self.person_social_contrib_info = self.social_contrib_info()
        self.person_health_care_spending_info = self.health_care_spending_info()
        #self.porson_mat_fund_info = self.mat_fund_info()
        
    def base_info(self):
        """
        人员基本信息
        """
        sql = "select to_char(AAC001) person_id,aac003 name,aac058 cert_type,\
        aac147 cert_num,aac004 gender,aac006 brithday,cac220 country,aac005 \
        nation,cac219 address,aae005 phone,aac008,aac084,aac114,aac060 from \
        szyth.AC01 where AAC001=%d"%int(self.person_id)
        df = pd.read_sql_query(sql,self.engine_szrs)
        return df
    
    def social_contrib_info(self):
        """
        医保缴费信息
        """
        sql = "select to_char(AAB001) 单位ID,to_char(AAC001) 人员ID,AAE002 \
        费款所属期,AAE003 对应费款所属期,AAE201 缴费月数,AAE180 人员缴费基数 from \
        szyth.KC43 where AAC001=%d and AAE140='310' \
        order by 对应费款所属期"%int(self.person_id)
        df = pd.read_sql_query(sql,self.engine_szrs)
        return df
        
    def mat_contrib_info(self):
        """
        生育缴费信息
        """
        sql = "select to_char(AAB001) 单位ID,to_char(AAC001) 人员ID,AAE002 \
        费款所属期,AAE003 对应费款所属期,AAE201 缴费月数,AAE180 人员缴费基数 from \
        szyth.MC43 where AAC001=%d order by 对应费款所属期"%int(self.person_id)
        df = pd.read_sql_query(sql,self.engine_szrs)
        return df  
        
    def mat_fund_info(self):
        """
        生育津贴领取信息
        """
        sql = "select to_char(AAC001) 人员ID,to_char(AAB001) 单位ID,\
        CMC114 怀孕日期,AMC020 生育日期,AMC030 生育津贴金额 \
        from szyth.MBA7 where AAC001=%d"%int(self.person_id)
        df = pd.read_sql_query(sql,self.engine_szrs)
        return df
    
    def get_injury_info(self):
        """
        获取工伤的信息
        """
        sql = "select * from fraud_info where person_id=%s and events='L'"%self.person_id
        df = pd.read_sql_query(sql,engine_analysis_db)
        return df
        
    def get_unemployment_info(self):
        """
        获取失业的信息
        """
        sql = "select * from fraud_info where person_id=%s and events='J'"%self.person_id
        df = pd.read_sql_query(sql,engine_analysis_db)
        return df
        
    def health_care_spending_info(self):
        """
        医疗支出结算信息
        """
        sql = "select to_char(AAC001) 人员ID,CKE011 交易日期,AAE149 年月,\
        AAZ107 医疗机构ID,CKE363 险种分类,AKA078 就诊方式,AKC264 费用总额,AKB068 统筹基金支付,\
        AKB067 现金支付,AKC200 本年度住院次数 from szyth.KC12 where AAC001=%d \
        and CKC005='00'"%int(self.person_id)
        df = pd.read_sql_query(sql,self.engine_szrs)
        return df
    
    def get_brithday(self):
        """
        出生日期
        """
        return self.person_base_info['brithday'].values.tolist()[0]
        
    def get_gender(self):
        """
        性别
        """
        return self.person_base_info['gender'].values.tolist()[0]
    
    def get_first_join_social_contrib_info(self):
        """
        首次参加苏州市医保日期
        """
        return self.person_social_contrib_info['对应费款所属期'].iloc[0]
    
    def get_brithplace(self):
        """
        获取籍贯信息
        """
        if self.person_base_info['cert_type'].values[0]=='1':
            identity_card_num = self.person_base_info['cert_num'].values[0]
            return identity_card_num[:4]
        else:
            return None
    
    def get_all_medical_cost(self):
        """
        医疗总花费
        """
        return self.person_health_care_spending_info['费用总额'].sum()
    
    def get_all_fund_pay(self):
        """
        社保基金支付总额
        """
        return self.person_health_care_spending_info['统筹基金支付'].sum()
    
    def get_all_inhostital_times(self):
        """
        住院次数
        """
        return self.person_health_care_spending_info['本年度住院次数'].sum()
    
    def injury_info(self):
        """
        工伤相关信息
        """
        pass
    
    def unemployment_info(self):
        """
        失业相关信息
        """
        pass
    
    def medical_bhv_als_hsp(self):
        """
        住院医疗行为分析（KCQ8 住院明细）
        住院行为分析：常用药和各项费用情况
        """
        sql = "select CKE020 时间,AAZ107 机构ID,AKA078 就诊方式,CKE021 明细分类,\
        AAZ231 项目编码,AKE002 明细名称,AKC226 明细数量,CKE103 大项分类 from \
        szyth.KCQ8,szyth.KC12 where KC12.AAC001=%d and KC12.CKA009=KCQ8.CKA009"\
        %int(self.person_id)
        
        df = pd.read_sql_query(sql,engine_szrs)
        if df.empty:
            return None   
        #个人住院行为：常用药分析
        df_yongyao = df[(df['明细分类']=='1')]['项目编码']\
                            .value_counts().to_frame()
        df_yongyao['项目占比'] = df_yongyao/df[(df['明细分类']=='1')]['明细数量'].sum()
        #df_yongyao.columns = ['数量']
        df_yongyao.reset_index(inplace=True)
        df_yongyao['人员ID'] = self.person_id
        df_yongyao.rename(columns={'index':'药品编码','项目编码':'数量'},inplace=True)
        df_yongyao = df_yongyao[:10]
        

        #个人医疗行为：住院金额分析
        df_temp = self.person_health_care_spending_info.copy()
        df_temp['SYEAR'] = df_temp['交易日期'].apply(lambda i:int(str(i)[:4]))
        df_temp['SMONTH'] = df_temp['交易日期'].apply(lambda i:int(str(i)[4:6]))
        df_temp['SDAY'] = df_temp['交易日期'].apply(lambda i:int(str(i)[6:]))
        cost_year = df_temp[df_temp['本年度住院次数']==1].groupby('SYEAR')\
                    ['费用总额','现金支付'].sum()
        cost_year['percent'] = cost_year['现金支付']/cost_year['费用总额']
        cishu_year = df_temp[df_temp['本年度住院次数']==1].groupby('SYEAR')\
                    ['本年度住院次数'].count()
        cost_year['年住院次数'] = cishu_year
        cost_year['人员ID'] = self.person_id
        cost_year.reset_index(inplace=True)
        
        
        #个人医疗行为：住院事件
        df_events = df_temp[df_temp['本年度住院次数']==1][['人员ID','交易日期',\
                    '医疗机构ID','费用总额','现金支付']]
        #个人医疗行为：诊疗地点分析
        df_ads_cishu = df_events.groupby('医疗机构ID').交易日期.count().to_frame()
        df_ads_jine = df_events.groupby('医疗机构ID').费用总额.sum().to_frame()
        df_ads = pd.concat([df_ads_cishu,df_ads_jine],axis=1)
        df_ads.reset_index(inplace=True)
        df_ads.rename(columns={'交易日期':'住院次数'},inplace=True)
        df_ads['人员ID'] = self.person_id
#==============================================================================
#         df_ads = df[(df['明细分类']=='1')].groupby('机构ID').明细数量.count().to_frame()
#         df_ads.index = df_ads.index.astype('int')
#         df_ads['人员ID'] = self.person_id
#         df_ads.reset_index(inplace=True)            
#==============================================================================
        return df_yongyao,df_ads,cost_year,df_events
        
    def medical_bhv_als_clnc(self):
        """
        非住院医疗行为分析(KC17 门诊明细)
        非住院行为分析：时间特征和常用药
        """
        #sql = "select KC17.CKE020 时间,KC12.AAZ107 机构ID,KBH3.AKB021 机构名称,KC17.CKE021 明细分类,KC17.AAZ231 项目编码,KC17.AKE002 明细名称,KC17.AKC226 明细数量,KC17.CKE103 大项分类 from szyth.KC17,szyth.KC12,szyth.KBH3 where KC12.AAC001=%d and KC12.CKA009=KC17.CKA009 and KC12.AAZ107=KBH3.AAZ107)"%int(self.person_id)
        sql = "select CKE020 时间,AAZ107 机构ID,AKA078 就诊方式,CKE021 明细分类,AAZ231 项目编码,\
        AKE002 明细名称,AKC226 明细数量,AKC225 项目单价,CKE103 大项分类 from szyth.KC17,szyth.KC12 \
        where KC12.AAC001=%d and KC12.CKA009=KC17.CKA009"%int(self.person_id)
        df = pd.read_sql_query(sql,self.engine_szrs)
        if df.empty:
            return None,None,None
        df['项目花费'] = df['明细数量']*df['项目单价']    
        df['hour'] = df['时间'].apply(lambda i:i.hour)   
        #个人医疗行为时间分析
        df_mdc_time = pd.DataFrame(np.array([0]*24).reshape(24,1),
                                   index=list(range(24)),
                                   columns=['数量'])
        df_time = df[(df['明细分类']=='1')].groupby('hour').明细数量.sum().to_frame()
        df_mdc_time['数量'] = df_time
        df_mdc_time.fillna(0,inplace=True)
        df_mdc_time.reset_index(inplace=True)
        df_mdc_time.rename(columns={'index':'时间'},inplace=True)
        #df_mdc_time = df_mdc_time
        
        #个人医疗行为：常用药分析
        df_yongyao = df[(df['明细分类']=='1')]['项目编码']\
                            .value_counts().to_frame()
        df_yongyao['项目占比'] = df_yongyao/df[(df['明细分类']=='1')]['明细数量'].sum()
        df_yongyao.reset_index(inplace=True)
        df_yongyao['人员ID'] = self.person_id
        df_yongyao.rename(columns={'index':'药品编码','项目编码':'数量'},inplace=True)
        df_yongyao = df_yongyao[:10]

        #df_yongyao
        #个人医疗行为：诊疗地点分析
        df_ads_count = df[(df['明细分类']=='1')].groupby('机构ID',as_index=False).明细数量.count()
        df_ads_sum = df[(df['明细分类']=='1')].groupby('机构ID',as_index=False).项目花费.sum()
        df_ads_count['项目花费'] = df_ads_sum['项目花费'].astype('int')
        df_ads_count['就诊方式'] = '15'
        df_ads_count['人员ID'] = self.person_id
        
        return df_mdc_time,df_yongyao,df_ads_count
          
    def career_life(self):
        """
        职业生涯简况
        """
        
        df_career = self.person_social_contrib_info[['单位ID','对应费款所属期',
                                                     '人员缴费基数']]      
        if df_career.empty:
            return []
        now_time = datetime.datetime.now()
        now = int(str(now_time.year)+str(now_time.month))
        df_career_periods=pd.DataFrame()
        companys = df_career['单位ID'].unique().tolist()
        for company in companys:
            periods = df_career[df_career['单位ID']==company]['对应费款所属期']\
                      .values.tolist()
            min_time = min(periods)
            max_time = max(periods) 
            if max_time==now:
                max_time=None
            ave_pay = int(df_career[df_career['单位ID']==company]['人员缴费基数'].mean())
            df_item = pd.DataFrame({'单位ID':[company],
                                    '入职':[min_time],
                                    '离职':[max_time],
                                    '平均缴费基数':[ave_pay]})
            df_career_periods = pd.concat([df_career_periods,df_item])
        df_career_periods['人员ID'] = self.person_id
        df_career_periods = df_career_periods[['人员ID','单位ID','入职','离职','平均缴费基数']]
        return df_career_periods
    

         
    def get_all_time_info(self):
        """
        生平医保缴费，医保消费全记录
        """
        medical_spend = self.person_health_care_spending_info.groupby(['年月'],as_index=False)\
                        [['费用总额','统筹基金支付']].sum()
        medical_spend['人员ID'] = self.person_id
        df_merge = pd.merge(medical_spend,self.person_social_contrib_info,
                            left_on=['人员ID','年月'],right_on=['人员ID','对应费款所属期'],how='outer')\
                            .sort_values(by=['对应费款所属期'])\
                            #.fillna(0)
                            
        df_merge.loc[df_merge['年月'].isnull(),'年月']= df_merge[df_merge['年月'].isnull()]['对应费款所属期']
        df_merge.loc[df_merge['费用总额'].isnull(),'费用总额'] = 0
        df_merge.loc[df_merge['统筹基金支付'].isnull(),'统筹基金支付'] = 0
        df_merge = df_merge[['人员ID', '单位ID','年月','费用总额',
                             '统筹基金支付','费款所属期', '对应费款所属期',
                             '缴费月数','人员缴费基数']]
        df_merge.columns = ['person_id','company_id','time',
                            'month_self_total_pay','month_fund_pay','aae002',
                            'aae003','num_of_nonth','base_pay']
        df_merge['time'] = df_merge['time'].astype('int')
        return df_merge
       
        
    def judge_medical_fraud(self,min_fund_value=5000,interval_month=4,\
                            max_times=1,upper_limit=10000,time_left_last_comp=4):
        """
        带病参保标志检测
        """
        def count_month_intevel(x,y):
            return 12*(int(str(x)[:4])-int(str(y)[:4]))+(int(str(x)[4:6])-int(str(y)[4:6]))
        
        periods = self.career_life()
        
        if len(periods)==0:
            return pd.DataFrame()
            
        left_time = periods[0][2]
        df_return = pd.DataFrame()
        
        for i,period in enumerate(periods):
            comp = period[0]
            in_time = period[1]
            
            social_contrib_in_comp = self.person_social_contrib_info[self.\
                                    person_social_contrib_info['单位ID']==comp]
            
            df_merge = pd.merge(self.person_health_care_spending_info,
                                social_contrib_in_comp,
                                left_on=['人员ID','年月'],
                                right_on=['人员ID','对应费款所属期'],
                                how='right')\
                                .sort_values(by=['对应费款所属期'])\
                                .fillna(0)
        
            fund_date_list = df_merge[(df_merge['统筹基金支付']>min_fund_value)]\
                                      ['对应费款所属期'].values.tolist()
        
            if len(fund_date_list)>=max_times:
                first_fund_date = fund_date_list[0] 
                #print('first_fund_date=',first_fund_date)
                first_in_date = in_time
                #print('first_in_date',first_in_date)
                total_money_in_comp = df_merge['统筹基金支付'].sum()
                #print('total_money_in_comp=',total_money_in_comp)
                               
                if count_month_intevel(first_fund_date,first_in_date)<=interval_month\
                     and total_money_in_comp>=upper_limit:
                    #print('interval_month=',count_month_intevel(first_fund_date,first_in_date))
                    
                    if (i==0) or (i>0 and count_month_intevel(in_time,left_time)>=time_left_last_comp):
                        all_money = self.get_all_fund_pay()
                        if all_money<5e4:
                            level = 1
                        elif all_money>=5e4 and all_money<1e5:
                            level = 2
                        elif all_money>=1e5 and all_money<1.5e5:
                            level = 3
                        elif all_money>=1.5e5 and all_money<2e5:
                            level = 4
                        elif all_money>=2e5:
                            level = 5                           
                        df_flag = pd.DataFrame({'person_id':[self.person_id],
                                                'company_id':[comp],
                                                'time':[first_fund_date],
                                                'events':['K'],
                                                'total_money':[total_money_in_comp],
                                                'level':[level],
                                                'flag':[True]}) 
                        df_flag = df_flag[['person_id','company_id','time',\
                                           'events','total_money','level','flag']]
                        df_return = pd.concat([df_return,df_flag])
                    
                left_time = period[2]

        return df_return
        
      
    def judge_preg_fraud(self):
        """
        怀孕后参保检测
        """
        #排除男性
        if self.get_gender()=='1':
            return pd.DataFrame()
            
        def count_date(date):
            from pandas.tseries.offsets import Day
            before_date = pd.to_datetime(str(date),format='%Y%m%d')-294*Day()
            return int(before_date.strftime('%Y%m%d'))
            
        def count_month_intevel(x,y):
            return 12*(int(str(x)[:4])-int(str(y)[:4]))+(int(str(x)[4:6])-int(str(y)[4:6]))
        #怀孕日期空值排除
        mat_fund_info = self.mat_fund_info()
        
        mat_fund_info = mat_fund_info.dropna().drop_duplicates()
        
        if mat_fund_info.empty:
            return pd.DataFrame()

        date_of_preg = int(str(mat_fund_info['怀孕日期'].values[0])[:6])
        #print('date_of_preg=',date_of_preg)

        df_pay = self.mat_contrib_info()
        all_pay_month = df_pay['对应费款所属期'].values.tolist()
        #print(all_pay_month)
        all_pay_month_except_local = df_pay.loc[df_pay['单位ID']!=mat_fund_info['单位ID']\
                                                .values[0],'对应费款所属期'].values.tolist()
        #print(all_pay_month_except_local)

        
        pay_first_month = df_pay[df_pay['单位ID']==mat_fund_info['单位ID'].values[0]]['费款所属期'].iloc[0]
        #print('pay_first_month=',pay_first_month)
        #计算领取津贴对应公司的首次缴费日期与怀孕日期的时间间隔
        time_intervel = count_month_intevel(pay_first_month,date_of_preg)
        #print('time_intervel=',time_intervel)
        
        #怀孕后参保判断规则：从怀孕日期到费款所属期的间隔在（0，10）之间
        #怀孕对应的日期没有在其他单位的参保记录，并且怀孕日期前没有参保或者间隔大于6个月
        if 0<=time_intervel<=10:
            list_temp = [i for i in all_pay_month_except_local if i<date_of_preg]
            #print(list_temp)
            
            last_pay = list_temp[-1] if len(list_temp)>0 else None
            #print(last_pay)
            
            if date_of_preg not in all_pay_month_except_local and\
            (last_pay is None or count_month_intevel(date_of_preg,last_pay)>=5):
                total_money = mat_fund_info['生育津贴金额'].values[0]
                if total_money<1e4:
                    level = 3
                elif total_money>=1e4 and total_money<1.5e4:
                    level = 4
                else:
                    level = 5
                    
                df_flag = pd.DataFrame({'person_id':[self.person_id],
                                    'company_id':[mat_fund_info['单位ID'].values[0]],
                                    'time':[pay_first_month],
                                    'events':['M'],
                                    'total_money':[total_money],
                                    'level':[level],
                                    'flag':[True]})
                return df_flag
        return pd.DataFrame()
    

    def judge_work_relate_injury_fraud(self):
        pass
    
    def data_to_db(self):
        """
        将数据汇总写入数据库
        """
        pass
    
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
        df.rename(columns={'person_id':'ITEMID1',
                           'company_id':'ITEMID2',
                           'month_self_total_pay':'AAB022',
                           'month_fund_pay':'AAB023',
                           'aae002':'AAB024',
                           'aae003':'AAB025',
                           'num_of_nonth':'AAB026',
                           'base_pay':'AAB027'},inplace=True)
        cols = df.columns.tolist()
        cols = cols[0:2]+cols[-5:]+cols[2:-5]
        df = df[cols]
        return df
        
    def plot_heatmap(x,y,z):
        """
        缴费支出时序热度图
        
        para:
            x:x轴数据
            y:y轴数据
            z:z轴数据
        
        return:作图对象           
        """
        if x is None or y is None or z is None:
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

      
if __name__=='__main__':
   
   platform = sys.platform
   if platform=='linux':
       engine_szrs = sqlalchemy.create_engine("oracle://szythrd:szythrd912@10.39.43.72:1521/szsbbak?charset=utf-8")
   elif platform=='win32':
       engine_szrs = sqlalchemy.create_engine("oracle://szythrd:szythrd912@192.168.90.14:1521/szsbbak?charset=utf-8")
   engine_analysis_db = sqlalchemy.create_engine("postgresql://liyi:123456@172.16.102.24:5432/db_szrs")
   
   zhangsan = Person(person_id='300046178',engine_szrs=engine_szrs,
                     engine_analysis_db=engine_analysis_db)
   #test3 = zhangsan.judge_preg_fraud()
   test1 = zhangsan.career_life()
   test2 = zhangsan.health_care_spending_info()
   #test_4,test_5 = zhangsan.medical_bhv_als_clnc()
   test3,test4,test5,test6 = zhangsan.medical_bhv_als_hsp()
   test7,test8,test9 = zhangsan.medical_bhv_als_clnc()
#==============================================================================
#    test4 = zhangsan.career_life()
#    test5 = zhangsan.get_all_time_info()
#    test_5 = zhangsan.judge_preg_fraud()
#    test6 = zhangsan.convert2lb(test5)
#==============================================================================
   #print(zhangsan.get_brithplace())         
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        