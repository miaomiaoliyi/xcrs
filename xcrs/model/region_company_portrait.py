# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 15:56:32 2017

@author: lisc1

相城区的街道画像
"""
import sys

import pandas as pd
import sqlalchemy
import numpy as np

class RegionCompany(object):
    def __init__(self,region_code,engine_szrs,engine_analysis_db):
        self.region_code = region_code
        self.engine_szrs = engine_szrs
        self.engine__analysis_db = engine_analysis_db
        self.region_companys = self.region_company_info()
        
    def region_company_info(self):
        """
        抽取AAB073=region_code的所有企业
        """
        sql = "select * from szyth.AB01 where aaa027=320507 and aab073='%s'"\
                %self.region_code
        df = pd.read_sql_query(sql,self.engine_szrs)
        return df
        
    def region_comps_status(self):
        """
        相城区街道企业经营状态统计及产业类型,单位类型，所属行业统计
        """
        #经营状态统计
        df_status = self.region_companys['cab009'].value_counts().to_frame().T
        df_status.rename(columns={'1':'zc','2':'jh'},inplace=True)
        df_status.index = [self.region_code]
        #产业类别统计
        df_type = self.region_companys['aab035'].value_counts().to_frame().T
        df_type.rename(columns={'1':'1th',
                                  '2':'2th',
                                  '3':'3th',
                                  '0':'0th'},inplace=True)
        df_type.index = [self.region_code]
        #所属行业统计
        df_ids = self.region_companys['aab022'].value_counts().to_frame().T
        df_ids.index = [self.region_code]
        #单位类型统计
        df_dtype = self.region_companys['aab019'].value_counts().to_frame().T
        df_dtype.index = [self.region_code]
        df = pd.concat([df_status,df_type,df_ids,df_dtype],axis=1)    
        df.reset_index(inplace=True)
        df.rename(columns={'index':'region'},inplace=True)
        return df
        
    def region_comps_etb_time(self):
        """
        相城区街道每月新成立单位数
        """
        self.region_companys['aae047'] = self.region_companys['aae047']\
                                             .fillna(19000101)\
                                             .astype('int')
        self.region_companys['est_time'] = self.region_companys['aae047']\
                                           .apply(lambda i:int(str(i)[:6]))
        df = self.region_companys[self.region_companys['est_time']>=201201]\
                .groupby('est_time',as_index=False).aab001.count()
        df['region'] = self.region_code
        df.rename(columns={'aab001':'nums'},inplace=True)
        df = df[['region','est_time','nums']]
        return df

    def region_workers_info(self):
        """
        相城区街道员工的年龄构成，性别构成，户籍构成，学历构成
        """
        pass
     
    def region_fund_contrib(self):
        """
        相城区各街道的时序社保贡献
        """
        pass     

    def region_comps_size(self):
        """
        相城区各街道的企业规模
        """
        pass
              
def get_region(engine):
    """
    获取相城区管辖内的街道及街道代码
    """
    sql = "select AAA102,AAA103 from szyth.AA10 where AAA100='AAB073' and \
    CZZ002=320507 and AAA102!='0700' and AAA102!='9903'"
    df = pd.read_sql_query(sql,engine)
    return df


        
if __name__=='__main__':
    platform = sys.platform
    if platform=='linux':
        engine_szrs = sqlalchemy.create_engine("oracle://szythrd:szythrd912@\
                                              10.39.43.72:1521/szsbbak?charset=utf-8")
    elif platform=='win32':
        engine_szrs = sqlalchemy.create_engine("oracle://szythrd:szythrd912@\
                                              192.168.90.14:1521/szsbbak?charset=utf-8")
    engine_analysis_db = sqlalchemy.create_engine("postgresql://liyi:123456@\
                                                 172.16.102.24:5432/db_szrs")
   
    regions = get_region(engine_szrs)
    print(regions)
    regions_code = regions['aaa102'].values.tolist()
    test3 = pd.DataFrame()
    test4 = pd.DataFrame()
    for region in regions_code:
        region = RegionCompany(region,engine_szrs,engine_analysis_db)
        test1 = region.region_comps_etb_time()
        test2 = region.region_comps_status()
        test3 = pd.concat([test3,test2])
        test4 = pd.concat([test4,test1])
        

#%%
print(1)  
