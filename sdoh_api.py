# -*- coding: utf-8 -*-
"""
Created on Wed Aug  3 10:45:11 2022

@author: nbadam
"""



import numpy as np
import pandas as pd
from base_schema import *
from db_connection import *

from flask import Flask
from flask import request

from sqlalchemy import select
import pandas.io.sql as sqlio
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)



##Final api

app = Flask(__name__)

@app.route('/sdohdata/',methods=['GET'])

def request_page():
    zip1  = request.args.get('zip',type=str , default='')
    cat  = request.args.get('category',type=str , default='')
    
    subcat  = request.args.get('label', type=int, default='')
    
    
    if subcat=='':
        subcat  = request.args.get('label', type=str, default='')
        
    
    
    query_sdoh='select * from insdata'
    
    
    
    df_temp= sqlio.read_sql_query(query_sdoh,engine)
    

    #print(df_temp)
    
    df_temp=df_temp[df_temp.zip==zip1].copy()
        
    min_max=df_temp[(df_temp.category.isin(['age','hh income']))&(df_temp.label!='19 to 64 years')][['category','label']].drop_duplicates()
    
    
    
    min_max1= min_max['label'].str.split('-', expand=True).astype(int)
    min_max = min_max.assign(maxval=min_max1.max(axis=1),minval=min_max1.min(axis=1))
    
    del min_max1
    
    def det_range(cat,subcat):
        
        min_max2=min_max[min_max.category==cat].copy()
        
       

        for i in range(len(min_max2)):
                        
            if (min_max2.iloc[i,min_max2.columns.get_loc('minval')]<=subcat)&(min_max2.iloc[i,min_max2.columns.get_loc('maxval')]>=subcat):
                
            
                #print("label",min_max2.iloc[i,min_max2.columns.get_loc('label')])
                return min_max2.iloc[i,min_max2.columns.get_loc('label')]
 
    
    def final_data(zip1,cat,subcat):
        
        
    
        
        if cat in ('age','hh income') :
            

            cd1=((df_temp.category==cat)&(df_temp.label==det_range(cat,subcat)))
 
        

            
        else:
            
            
            cd1=((df_temp.category==cat)&(df_temp.label==subcat))
            


            
            
        temp=df_temp[(cd1)&(df_temp.zip==zip1)].groupby(['label','zip', 'outputparametername','outputparametertype', 'category'])['outputvalue'].sum().reset_index()
          
        
        return temp
            
    
        
        
    df_final=final_data(zip1,cat,subcat)
        
    
    data=df_final.to_json(orient='records')
    
     
    return data

if __name__ == '__main__':
     app.run(port=7777)



