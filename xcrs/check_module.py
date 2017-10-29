# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 18:04:32 2017

@author: lisc1

python运行环境及模块检查
"""

import sys
import pip

modules = ['pandas','numpy','matplotlib.pyplot','sqlalchemy','plotly']

def install(package):
    pip.main(['install',package])
    

def check():
    STATE = True
    for module in modules:
        try:
            __import__(module)
        except Exception as e:
            STATE = False
            print('%s is not installed, installing it now!'%module)
        
    if STATE is True:
        print('All module is installed')   
        

if __name__=='__main__':
    check()
    
    
    
    
    
    
    
    
    