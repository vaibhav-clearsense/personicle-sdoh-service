# -*- coding: utf-8 -*-
"""
Created on Mon Aug 15 20:23:57 2022

@author: nbadam
"""

import numpy as np
import pandas as pd

from base_schema import *
from db_connection import *

from sqlalchemy import select
import pandas.io.sql as sqlio
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


df=pd.read_csv('healthinsurance_sdoh1.csv',encoding="utf8")

df.columns = df.iloc[0]
df = df[1:].copy()



df.drop(columns=['id'],inplace=True)
df.rename(columns={'Geographic Area Name':'zip'},inplace=True)
df["zip"]=df["zip"].str.replace("ZCTA5 ","")


collist1=df.filter(like='19 to 64 years').columns.tolist()
collist2=df.filter(like='poverty').columns.tolist()
collist3=df.filter(like='CITIZENSHIP STATUS').columns.tolist()
collist4=df.filter(like='poverty threshold').columns.tolist()

collist=collist1+collist2+collist3+collist4




df.drop(columns=collist,inplace=True)

#df=df.head(50)

df1=df.melt(id_vars=['zip'], var_name='variable', value_name='value')


df1 =df1.replace(u'\xa0', u'', regex=True)#.sort_values(by='label')


df2=pd.concat([df1[['zip','value']], df1['variable'].str.split('!!', expand=True)], axis=1)

df2=df2[(df2[4].notnull())|(df2[5].isin(['Under $25,000', '$25,000 to $49,999',
       '$50,000 to $74,999', '$75,000 to $99,999', '$100,000 and over']))].drop(columns=[2,3,6])

df2['label']=df2[4].astype(str)+df2[5].astype(str)

df2["label"]=df2["label"].str.replace("None","").str.replace("Civilian noninstitutionalized population","").str.replace("Total household population","")
df2.drop(columns=[4,5],inplace=True)
df2.rename(columns={1:'insurance',0:'parameters'},inplace=True)

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

df2['category']=df2.label.map(dct_cat)
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

#Renaming columns
df_temp.rename(columns={'insurance':'outputparametername','parameters':'outputparametertype','value':'outputvalue'},inplace=True)

#Cleaning zipcodes
df_temp["zip"]=df_temp["zip"].str.replace("ZCTA5 ","")

#print(df_temp.dtypes)

df_temp['outputvalue']=df_temp['outputvalue'].astype(float)



df_temp.to_sql(name='insdata', con=engine, if_exists='replace', index=False)
