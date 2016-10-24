# -*- coding: utf-8 -*-
"""
Created on Wed Aug 17 10:52:29 2016

@author: newchama
"""
import pandas as pd
from sqlalchemy import create_engine
#拿到数据
team=pd.read_csv('/Users/newchama/Documents/My_data/DB/data_to_db/output_company_team.csv')

#参数mysql+pymysql:表示数据库名称，用户名：密码@主机ip；数据库名?charset=utf8 表示数据库名称？转义成字符
engine = create_engine('mysql+pymysql://xxx:xxx@xxx/xxx?charset=utf8')#用sqlalchemy创建引擎  
team.to_sql("company_team", engine, if_exists='append',index=False)


#需要传入的参数：要导入的数据，数据库中表的名称，判断是否重复的字段，engine
#参数形式：df或series, str,str
#注意：传入的字段只能为一个
def My_to_sql(data,table,dup_field,engine):
    query='SELECT DISTINCT  '+dup_field+'  FROM  '+table
    field_value=pd.read_sql(query,engine)
    data_to_db=data[~data[dup_field].isin(field_value)]
    data_to_db.to_sql(table, engine, if_exists="append",index=False)  #检查主键ID是否重复，若重复引发错误时，进行pass







    
