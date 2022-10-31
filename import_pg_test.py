# -*- coding: utf-8 -*-
"""
Created on Wed Aug 31 21:46:23 2022

@author: Sam

Надо потестить как очень быстро выбирать строчку из датафрейма
"""


import os
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 
import pickle
import hashlib
#import datetime
from dateutil.parser import parse
import csv


filename = 'site - clearvoicesurvey.com_2015_17.5M.csv'

class Df_name():
    df_name = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.h_name;"
        self.conn = conn
        #self.df_name = sqlio.read_sql_query(sql, conn)
        self.df_name = sqlio.read_sql_query(sql, conn, index_col =['name'])
        #print('df_name:')
        #print(self.df_name)

    def ins(self, text):
        text = text.lower()
        #print('inserting "' + text + '"')
        list_ind = self.df_name[(self.df_name['name'] == text)]['id'].tolist();    #.index.tolist();
        #print(list_ind)
        if list_ind ==[]:
            sql = """insert into ppg_data_vault.h_name(name) 
                values('{}');""".format(text.replace("'", "''"))
            #print(sql)
            cur.execute(sql)
            conn.commit()
            #print("insert ", text)
            
            sql = """select id 
                from ppg_data_vault.h_name where name = '{}';""" \
                .format(text.replace("'", "''"))
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("text num: ", src_num)
            #self.df_name = self.df_name.append({'id': src_num 
            #                , 'name': text
            #                }, ignore_index=True);
            self.df_name = pd.concat([self.df_name, \
                    pd.DataFrame.from_records([{ 'id': src_num, 'name': text }])], \
                        ignore_index=True)
            
            return int(src_num)
        else:
            return int(list_ind[0])




# Получить 

try:
    conn = psycopg2.connect("dbname='demo' user='skvo' host='192.168.56.101' password='sghc200'")
    #conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, dbname, username, pwd))
    
    cur = conn.cursor()
    print("connected")
    
    sql = "select id from ppg_data_vault.sources where source_desc = '{}';".format(filename)
    cur.execute(sql)
    src_num = cur.fetchone()[0]
    print("src_num: ", src_num)


    
    
    
    #DATA_DIR = 'D:/data/dbs/fox'
    #DATA_DIR = 'D:\\data\\dbs\\fox'
    #DATA_DIR = 'D:\data\dbs\fox'
    #DATA_DIR = 'D:/data/dbs/fox/site - clearvoicesurvey.com_2015_17.5M'
    #filename = 'ALA_20151217.txt'
    
    dfname = Df_name(conn)

    cur.close()
    conn.close()

except Exception as e:
    print(e.__class__)
    print(e)
    print(step)
finally:
    pass