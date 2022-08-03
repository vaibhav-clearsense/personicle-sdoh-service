# -*- coding: utf-8 -*-
"""
Created on Tue Aug  2 16:36:51 2022

@author: nbadam
"""

from pyforest import *

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




#zip1  = request.args.get('zip',type=str , default='')
#cat  = request.args.get('category',type=str , default='')

#subcat  = request.args.get('label',type=str , default='')
# te  = request.args.get('endtime',type=str , default='')
# freq  = request.args.get('freq',type=str , default='')

df=pd.read_csv('healthinsurance_sdoh.csv',encoding="utf8")

df.rename(columns={'Label (Grouping)':'label'},inplace=True)
df.columns = [col.replace('\xa0', '') for col in df.columns]
df1=df.melt(id_vars='label')
df1 =df1.replace(u'\xa0', u'', regex=True).sort_values(by='label')

dct_cat={ '19 to 25 years':'age',
       '19 to 64 years':'age', '26 to 34 years':'age', '35 to 44 years':'age',
       '45 to 54 years':'age', '55 to 64 years':'age', '6 to 18 years':'age',
       '65 to 74 years':'age', '65 years and older':'age', '75 years and older':'age',
        'Male':'gender','Female':'gender',
        'White alone':'race',
         'Black or African American alone':'race',
        'American Indian and Alaska Native alone':'race',
         'Asian alone':'race',
         'Native Hawaiian and Other Pacific Islander alone':'race',
         'Some other race alone':'race',
         'Two or more races':'race',
         'Hispanic or Latino (of any race)':'race',
         'not Hispanic or Latino':'race',
         'With a disability':'disability','No disability':'disability',
         'Employed':'employment_status',
         'Unemployed':'employment_status',
         'Under $25,000':'hh income','$25,000 to $49,999':'hh income',
         '$50,000 to $74,999':'hh income','$75,000 to $99,999':'hh income','$100,000 and over':'hh income'    
        }

df2=pd.concat([df1[['label','value']], df1['variable'].str.split('!!', expand=True)], axis=1)
df2.rename(columns={0:'zip',1:'insurance',2:'parameters'},inplace=True)

df2['category']=df2.label.map(dct_cat)
df2["zip"]=df2["zip"].str.replace("ZCTA5 ","")

df_temp=df2[(df2.category.notnull())&((df2.insurance=='Insured')|(df2.insurance=='Uninsured'))&(df2.insurance!='Total')]

df_temp["value"]=df_temp["value"].astype(str)
df_temp["value"]=df_temp["value"].str.replace(",","").str.replace("±"," ")


#Replacing age strings
df_temp["label"]=df_temp["label"].str.replace("6 to 18 years","6-18").str.replace("19 to 25 years","19-25").str.replace("26 to 34 years","26-34").str.replace("35 to 44 years","35-44").str.replace("45 to 54 years","45-54").str.replace("55 to 64 years","55-64").str.replace("65 to 74 years","65-100").str.replace("65 years and older","65-100").str.replace("75 years and older","65-100")

#Replacing race strings
df_temp["label"]=df_temp["label"].str.replace("American Indian and Alaska Native alone","American/Alaskan").str.replace("Asian alone","Asian").str.replace("Black or African American alone","African American").str.replace("Hispanic or Latino (of any race)","Hispanic").str.replace("Native Hawaiian and Other Pacific Islander alone","Hawaiian").str.replace("Some other race alone","others").str.replace("Two or more races","others").str.replace("White alone","white")

#Disability
df_temp["label"]=df_temp["label"].str.replace("With a disability","disabled").str.replace("No disability","non-disabled")

#hh Income
df_temp.replace('$100,000 and over', '100000-100000000', inplace=True)
df_temp.replace('Under $25,000', '0-25000', inplace=True)
df_temp.replace('$25,000 to $49,999', '25000-49999', inplace=True)
df_temp.replace('$50,000 to $74,999', '50000-74999', inplace=True)
df_temp.replace('$75,000 to $99,999', '75000-99999', inplace=True)

df_temp['value']=df_temp['value'].astype(float)

#Renaming columns
df_temp.rename(columns={'insurance':'outputparametername','parameters':'outputparametertype','value':'outputvalue'},inplace=True)



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
        
        #print("dtypes:::",min_max2,min_max2.dtypes)
        
        #print('subcat dtypes:::::',cat,type(cat),subcat,type(subcat))
        
       # print('min_max2::::',min_max2)

        for i in range(len(min_max2)):
                        
            if (min_max2.iloc[i,min_max2.columns.get_loc('minval')]<=subcat)&(min_max2.iloc[i,min_max2.columns.get_loc('maxval')]>=subcat):
                
            
                #print("label",min_max2.iloc[i,min_max2.columns.get_loc('label')])
                return min_max2.iloc[i,min_max2.columns.get_loc('label')]
 
    
    def final_data(zip1,cat,subcat):
        
        
        #try:
    
        
        if cat in ('age','hh income') :
            
            #print('loop entry')
            print('subcat is:::', subcat)

            cd1=((df_temp.category==cat)&(df_temp.label==det_range(cat,subcat)))
 
        
#        except TypeError:
            #print(e)
            
        else:
            
            #print("This variable is not numeric hence adding categorical matching", subcat)
            
            cd1=((df_temp.category==cat)&(df_temp.label==subcat))
            
            #print('cd exception head::::::',cd1.head())


            
            
        temp=df_temp[(cd1)&(df_temp.zip==zip1)].groupby(['label','zip', 'outputparametername','outputparametertype', 'category'])['outputvalue'].sum().reset_index()
          
        
        return temp
            
    
        
#    cd1=((df_temp.category==cat)&(df_temp.label==subcat))
        
    df_final=final_data(zip1,cat,subcat)
        
    
    data=df_final.to_json(orient='records')
    
     
    return data

if __name__ == '__main__':
     app.run(port=7777)



