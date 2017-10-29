# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 14:24:16 2017

@author: lisc1
"""
from datetime import datetime
import math
    
import numpy as np

def count_month(date):
    """
    根据传入日期计算距今月数
    """    
    if np.isnan(date) or len(str(date))<10:
        return -1    
    year = int(str(date)[:4])
    month = int(str(date)[4:6])
    day = int(str(date)[6:8])
    now = datetime.now()
    delta = datetime(now.year,now.month,now.day)-datetime(year,month,day)
    return math.ceil(delta.days/30)











