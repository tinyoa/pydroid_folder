# -*- coding: utf-8 -*-

#Created on Tue Feb  1 22:14:51 2022
#
#@author: Sam
#
#Проблема: pandas append method is deprecated
#df = pd.concat([df, pd.DataFrame.from_records([{ 'a': 1, 'b': 2 }])], ignore_index=True)



import os
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 
import pickle
import hashlib
#import datetime
from dateutil.parser import parse
import csv


print("start")

filename = 'site - clearvoicesurvey.com_2015_17.5M.csv'
filepath = 'D:\\data\\dbs\\fox\\site - clearvoicesurvey.com_2015_17.5M.csv'
DATA_DIR = 'D:/data/dbs/fox/site - clearvoicesurvey.com_2015_17.5M'

#print(f"select count(*) from ppg_data_vault.sources where source_desc = '{filename}';")
CHKPNT_FILE = 'chkpntfil.chp'


colname = 'last_name';
colnum = 0;
separator =',';
goon = 1;	# 0 - начать файл с начала.
# словарь, который должен описать, какие домены интересуют, в каких столбцах их искать, в каких файлах хранить.
dict_cols = {
	'surname': {
		'colname': ['last_name', 'surname']
		, 'filename': 'secondnames.csv'
		, 'colnum' : 0
		}
	, 'name': {
		'colname': ['name', 'first_name']
		, 'filename': 'names.csv'
		, 'colnum' : 0
		}
	, 'middlename': {
		'colname': ['middlename', 'second_name']
		, 'filename': 'names.csv'
		, 'colnum' : 0
		}
	, 'phone': {
		'colname': ['phone']
		, 'filename': 'phones.csv'
		, 'colnum' : 0
		}
	, 'birthdate': {
		'colname': ['birthdate', 'dob']
		, 'filename': 'username.csv'
		, 'colnum' : 0
		}
	, 'username': {
		'colname': ['username']
		, 'filename': 'username.csv'
		, 'colnum' : 0
		}
		, 'address': {
		'colname': ['address']
		, 'filename': 'address.csv'
		, 'colnum' : 0
		}
		, 'email': {
		'colname': ['email', 'email_address', 'user_email']
		, 'filename': 'email.csv'
		, 'colnum' : 0
		}
		, 'nickname': {
		'colname': ['nickname']
		, 'filename': 'nickname.csv'
		, 'colnum' : 0
		}
		, 'city': {
		'colname': ['city']
		, 'filename': 'city.csv'
		, 'colnum' : 0
		}
};


def prepare_date(date_text):
    try:
        result = parse(date_text)
        result = result.strftime("%Y-%m-%d")
        #print(str(result))
        return result
    except ValueError:
        print(date_text)
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


class Df_source():
    df_source = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.sources;"
        self.conn = conn
        self.df_source = sqlio.read_sql_query(sql, conn)
        print('df_source:')
        print(self.df_source)

    def ins(self, source):
        source = source.lower()
        print('checking source: "' + source + '"')
        list_ind = self.df_source[(self.df_source['source_desc'] == source)]['id'] \
            .tolist();    #.index.tolist();
        print(list_ind)
        if list_ind ==[]:
            sql = """insert into ppg_data_vault.sources(source_desc) 
                    values('{}');""".format(source)
            print(sql)
            cur.execute(sql)
            conn.commit()
            print("insert source: ", source)
            
            sql = "select id from ppg_data_vault.sources where name = '{}';" \
                .format(source)
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("source num: ", src_num)
            self.df_source = self.df_source.append({'id': src_num 
                            , 'source_desc': source
                            }, ignore_index=True);
            return int(src_num)
        else:
            return int(list_ind[0])



class Df_name():
    df_name = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.h_name;"
        self.conn = conn
        self.df_name = sqlio.read_sql_query(sql, conn)
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


class Df_surname():
    df_surname = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.h_surname;"
        self.conn = conn
        self.df_surname = sqlio.read_sql_query(sql, conn)
        #print('df_surname:')
        #print(self.df_surname)

    def ins(self, text):
        text = text.lower()
        #print('inserting "' + text + '"')
        list_ind = self.df_surname[(self.df_surname['name'] == text)]['id'].tolist();    #.index.tolist();
        #print(list_ind)
        if list_ind ==[]:
            sql = """insert into ppg_data_vault.h_surname(name) 
                values('{}');""".format(text.replace("'", "''"))
            #print(sql)
            cur.execute(sql)
            conn.commit()
            #print("insert ", text)
            
            sql = """select id 
                from ppg_data_vault.h_surname where name = '{}';""" \
                .format(text.replace("'", "''"))
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("text num: ", src_num)
            #self.df_surname = self.df_surname.append({'id': src_num 
            #                , 'name': text
            #                }, ignore_index=True);
            self.df_surname = pd.concat([self.df_surname, \
                    pd.DataFrame.from_records([{ 'id': src_num, 'name': text }])], \
                        ignore_index=True)
            return int(src_num)
        else:
            
            return int(list_ind[0])


class Df_middlename():
    df_surname = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.h_middlename;"
        self.conn = conn
        self.df_middlename = sqlio.read_sql_query(sql, conn)
        #print('df_middlename:')
        #print(self.df_middlename)

    def ins(self, text):
        text = text.lower()
        #print('inserting "' + text + '"')
        list_ind = self.df_middlename[(self.df_middlename['name'] == text)]['id'].tolist();    #.index.tolist();
        #print(list_ind)
        if list_ind ==[]:
            sql = """insert into ppg_data_vault.h_middlename(name) 
                values('{}');""".format(text.replace("'", "''")) 
            #print(sql)
            cur.execute(sql)
            conn.commit()
            #print("insert middlename: ", text)
            
            sql = """select id 
                from ppg_data_vault.h_middlename where name = '{}';""" \
                .format(text.replace("'", "''"))
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("text num: ", src_num)
            #self.df_middlename = self.df_middlename.append({'id': src_num 
            #                , 'name': text
            #                }, ignore_index=True);
            self.df_middlename = pd.concat([self.df_middlename, \
                    pd.DataFrame.from_records([{ 'id': src_num, 'name': text }])], \
                        ignore_index=True)
            return int(src_num)
        else:
            return int(list_ind[0])


# телефон
class Df_phonenumber():
    df_phonenumber = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.h_phonenumber;"
        self.conn = conn
        self.df_phonenumber = sqlio.read_sql_query(sql, conn)
        #print('df_phonenumber:')
        #print(self.df_phonenumber)


    def ins(self, text):
        '''Проверяет есть ли text в датафрейме. Если нет - добавляет'''
        #print('inserting "' + text + '"')
        list_ind = self.df_phonenumber[(self.df_phonenumber['phonenumber'] == text)]['id'].tolist(); #.index.tolist();
        #print(list_ind)
        if list_ind ==[]:
            sql = "insert into ppg_data_vault.h_phonenumber(phonenumber) values('" + text + "');" 
            #print(sql)
            cur.execute(sql)
            conn.commit()
            #print("insert ", text)
            
            sql = "select id from ppg_data_vault.h_phonenumber where phonenumber = '{}';".format(text)
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("text num: ", src_num)
            #self.df_phonenumber = self.df_phonenumber.append({'id': src_num 
            #                , 'phonenumber': text
            #                }, ignore_index=True);
            self.df_phonenumber = pd.concat([self.df_phonenumber, \
                    pd.DataFrame.from_records([{ 'id': src_num, \
                                    'phonenumber': text }])], \
                        ignore_index=True)
            return int(src_num)
        else:
            return int(list_ind[0])


# адрес
class Df_address():
    df_address = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.h_address;"
        self.conn = conn
        self.df_address = sqlio.read_sql_query(sql, conn)
        #print('df_address:')
        #print(self.df_address)


    def ins(self, text):
        '''Проверяет есть ли text в датафрейме. Если нет - добавляет'''
        text = text.lower()
        #print('inserting address"' + text + '"')
        list_ind = self.df_address[(self.df_address['address'] == text)]['id'].tolist(); #.index.tolist();
        #print(list_ind)
        if list_ind ==[]:
            sql = """insert into ppg_data_vault.h_address(address) 
                values('{}');""".format(text.replace("'", "''"))
            #print(sql)
            cur.execute(sql)
            conn.commit()
            #print("insert address: ", text)
            
            sql = """select id 
                from ppg_data_vault.h_address where address = '{}';""" \
                .format(text.replace("'", "''"))
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("text num: ", src_num)
            #self.df_address = self.df_address.append({'id': src_num 
            #                , 'address': text
            #                }, ignore_index=True);
            self.df_address = pd.concat([self.df_address, \
                    pd.DataFrame.from_records([{ 'id': src_num, \
                                    'address': text }])], \
                        ignore_index=True)
            return int(src_num)
        else:
            return int(list_ind[0])


# домен
class Df_domain():
    df_domain = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.h_domain;"
        self.conn = conn
        self.df_domain = sqlio.read_sql_query(sql, conn)
        #print('df_domain:')
        #print(self.df_domain)

    def ins(self, text):
        '''Проверяет есть ли text в датафрейме. Если нет - добавляет'''
        text = text.lower()
        #print('inserting "' + text + '"')
        list_ind = self.df_domain[(self.df_domain['domain'] == text)]['id'].tolist(); #.index.tolist();
        #print(list_ind)
        if list_ind ==[]:
            sql = """insert into ppg_data_vault.h_domain(domain) 
                values('{}');""".format(text.replace("'", "''"))
            #print(sql)
            cur.execute(sql)
            conn.commit()
            #print("insert domain", text)
            
            sql = """select id 
                from ppg_data_vault.h_domain 
                where domain = '{}';""".format(text.replace("'", "''"))
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("text num: ", src_num)
            #self.df_domain = self.df_domain.append({'id': src_num 
            #                , 'domain': text
            #                }, ignore_index=True);
            self.df_domain = pd.concat([self.df_domain, \
                    pd.DataFrame.from_records([{ 'id': src_num, \
                                    'domain': text }])], \
                        ignore_index=True)
            return int(src_num)
        else:
            return int(list_ind[0])

# домен первого уровня
class Df_l1domain():
    df_l1domain = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.h_l1domain;"
        self.conn = conn
        self.df_l1domain = sqlio.read_sql_query(sql, conn)
        #print('df_l1domain:')
        #print(self.df_l1domain)


    def ins(self, l1domain):
        '''Проверяет есть ли l1domain в датафрейме. Если нет - добавляет'''
        l1domain = l1domain.lower()
        #print('inserting l1domain: "' + l1domain + '"')
        list_ind = self.df_l1domain[(self.df_l1domain['l1domain'] == l1domain)]['id'] \
                .tolist(); #.index.tolist();
        #print(list_ind)
        if list_ind ==[]:
            sql = """insert into ppg_data_vault.h_l1domain(l1domain) 
                values('{}');""".format(l1domain) 
            #print(sql)
            cur.execute(sql)
            conn.commit()
            #print("insert l1domain: ", l1domain)
            
            sql = "select id from ppg_data_vault.h_l1domain where l1domain = '{}';" \
                    .format(l1domain)
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("l1domain num: ", src_num)
            #self.df_l1domain = self.df_l1domain.append({'id': src_num 
            #                , 'l1domain': l1domain
            #                }, ignore_index=True);
            self.df_l1domain = pd.concat([self.df_l1domain, \
                    pd.DataFrame.from_records([{ 'id': src_num, \
                                    'l1domain': l1domain }])], \
                        ignore_index=True)
            return int(src_num)
        else:
            return int(list_ind[0])


# ник
class Df_nickname():
    df_nickname = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.h_nickname;"
        self.conn = conn
        self.df_nickname = sqlio.read_sql_query(sql, conn)
        #print('df_nickname:')
        #print(self.df_nickname)

    def ins(self, text):
        '''Проверяет есть ли text в датафрейме. Если нет - добавляет'''
        text = text.lower()
        #print('inserting "' + text + '"')
        list_ind = self.df_nickname[(self.df_nickname['nickname'] == text)]['id'].tolist(); #.index.tolist();
        #print(list_ind)
        if list_ind ==[]:
            sql = """insert into ppg_data_vault.h_nickname(nickname) 
                values('{}');""".format(text.replace("'", "''"))
            #print(sql)
            cur.execute(sql)
            conn.commit()
            #print("insert nickname", text)
            
            sql = """select id 
                from ppg_data_vault.h_nickname 
                where nickname = '{}';""".format(text.replace("'", "''"))
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("text num: ", src_num)
            #self.df_nickname = self.df_nickname.append({'id': src_num 
            #                , 'nickname': text
            #                }, ignore_index=True);
            self.df_nickname = pd.concat([self.df_nickname, \
                    pd.DataFrame.from_records([{ 'id': src_num, \
                                    'nickname': text }])], \
                        ignore_index=True)
            return int(src_num)
        else:
            return int(list_ind[0])


# email
class Df_email():
    df_email = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.h_email;"
        self.conn = conn
        self.df_email = sqlio.read_sql_query(sql, conn)
        #print('df_email:')
        #print(self.df_email)

    def ins(self, nickname_id, domain_id, l1domain_id):
        
        '''Проверяет есть ли text в датафрейме. Если нет - добавляет'''
        #print('inserting nickname_id, domain_id, l1domain_id: {}, {}, {}' \
        #      .format(nickname_id, domain_id, l1domain_id))
        list_ind = self.df_email[(self.df_email['nickname_id'] == nickname_id)& \
                (self.df_email['domain_id'] == domain_id)& \
                (self.df_email['l1domain_id'] == l1domain_id)]['id'].tolist(); #.index.tolist();
        #print(list_ind)
        if list_ind ==[]:
            sql = """insert into ppg_data_vault.h_email(nickname_id, domain_id, l1domain_id) 
                values({}, {}, {});""".format(nickname_id, domain_id, l1domain_id)
            #print(sql)
            cur.execute(sql)
            conn.commit()
            #print("insert nickname_id, domain_id, l1domain_id: {}, {}, {}" \
            #      .format(nickname_id, domain_id, l1domain_id))
            
            sql = """select id 
                from ppg_data_vault.h_email 
                where nickname_id = {}
                    and domain_id = {}
                    and l1domain_id = {};""" \
                    .format(nickname_id, domain_id, l1domain_id)
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("text num: ", src_num)
            #self.df_email = self.df_email.append({'id': src_num 
            #                    , 'nickname_id': nickname_id
            #                    , 'domain_id': domain_id
            #                    , 'l1domain_id': l1domain_id
            #                }, ignore_index=True);
            self.df_email = pd.concat([self.df_email, \
                    pd.DataFrame.from_records([{ 'id': src_num \
                                , 'nickname_id': nickname_id
                                , 'domain_id': domain_id
                                , 'l1domain_id': l1domain_id
                                    }])], \
                        ignore_index=True)
            return int(src_num)
        else:
            return int(list_ind[0])


# city
class Df_city():
    df_city = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.h_city;"
        self.conn = conn
        self.df_city = sqlio.read_sql_query(sql, conn)
        #print('df_city:')
        #print(self.df_city)

    def ins(self, city):
        '''Проверяет есть ли text в датафрейме. Если нет - добавляет'''
        #print("inserting city: {}".format(city))
        list_ind = self.df_city[(self.df_city['city'] == city)]['id'].tolist(); #.index.tolist();
        #print(list_ind)
        if list_ind ==[]:
            sql = """insert into ppg_data_vault.h_city(city) 
                values('{}');""".format(city.replace("'", "''")) 
            #print(sql)
            cur.execute(sql)
            conn.commit()
            #print("insert ", city)
            
            sql = """select id 
                from ppg_data_vault.h_city where city = '{}';""" \
                .format(city.replace("'", "''"))
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("text num: ", src_num)
            #self.df_city = self.df_city.append({'id': src_num 
            #                , 'city': city
            #                }, ignore_index=True);
            self.df_city = pd.concat([self.df_city, \
                    pd.DataFrame.from_records([{ 'id': src_num \
                                , 'city': city
                                    }])], \
                        ignore_index=True)
            return int(src_num)
        else:
            return int(list_ind[0])


# person
class Df_person():
    df_person = pd.DataFrame();
    
    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.h_person;"
        self.conn = conn
        self.df_person = sqlio.read_sql_query(sql, conn)
        #print('df_person:')
        #print(self.df_person)

    def ins(self, source_id, surname_id, name_id, middlename_id, birthdate
                , phonenumber_id, city_id):
        '''Проверяет есть ли text в датафрейме. Если нет - добавляет'''
        
        hashmd5 = hashlib.md5((str(source_id) \
                    + str(surname_id) + str(name_id) \
                    + str(middlename_id) + str(birthdate) \
                    + str(phonenumber_id) + str(city_id)).encode()) \
                        .hexdigest();
        #print('inserting "' + hashmd5 + '"')
        list_ind = self.df_person[(self.df_person['hash'] == hashmd5)]['id'].tolist(); #.index.tolist();
        #print(list_ind)
        if list_ind ==[]:
            sql = """insert into ppg_data_vault.h_person(source_id, hash
                        , surname_id, name_id, middlename_id, birthdate
                        , phonenumber_id, city_id) 
                values({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}');""" \
                    .format(str(source_id), hashmd5, surname_id, name_id \
                            , middlename_id, birthdate, phonenumber_id \
                            , city_id)
            
            #print(sql)
            cur.execute(sql)
            conn.commit()
            #print("insert ", hashmd5)
            
            sql = "select id from ppg_data_vault.h_person where hash = '{}';".format(hashmd5)
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("text num: ", src_num)
            #self.df_person = self.df_person.append({'id': src_num 
            #                , 'hash': hashmd5
            #                , 'surname_id': surname_id
            #                , 'name_id': name_id
            #                , 'middlename_id': middlename_id
            #                , 'birthdate': birthdate
            #                , 'phonenumber_id': phonenumber_id
            #                , 'city_id': city_id
            #                }, ignore_index=True);
            self.df_person = pd.concat([self.df_person, \
                    pd.DataFrame.from_records([{ 'id': src_num \
                            , 'hash': hashmd5
                            , 'surname_id': surname_id
                            , 'name_id': name_id
                            , 'middlename_id': middlename_id
                            , 'birthdate': birthdate
                            , 'phonenumber_id': phonenumber_id
                            , 'city_id': city_id
                                    }])], \
                        ignore_index=True)
                
            return int(src_num)
        else:
            return int(list_ind[0])


# person has email
class Df_person_has_email():
    df_person_has_email = pd.DataFrame();

    def __init__ (self, conn):
        sql = "select * from ppg_data_vault.l_person_email;"
        self.conn = conn
        self.df_person_has_email = sqlio.read_sql_query(sql, conn)
        #print('df_person_has_email:')
        #print(self.df_person_has_email)

    def ins(self, source_id, person_id, email_id):
        '''Проверяет есть ли text в датафрейме. Если нет - добавляет'''
        
        hashmd5 = hashlib.md5((str(source_id) + str(person_id) \
                    + str(email_id)).encode()) \
                        .hexdigest();
        #print('inserting person_has_email:"' + hashmd5 + '"')
        list_ind = self.df_person_has_email[(self.df_person_has_email['hash'] == hashmd5)]['id'].tolist(); #.index.tolist();
        #print(list_ind)
        if list_ind ==[]:
            sql = """insert into ppg_data_vault.l_person_email
                (hash, source_id, person_id, email_id) 
                values('{}', {}, {}, {});""" \
                .format(hashmd5, str(source_id), person_id, email_id)
            
            #print(sql)
            cur.execute(sql)
            conn.commit()
            #print("insert ", hashmd5)
            
            sql = "select id from ppg_data_vault.l_person_email where hash = '{}';" \
                .format(hashmd5)
            cur.execute(sql)
            src_num = cur.fetchone()[0]
            #print("text num: ", src_num)
            #self.df_person_has_email = self.df_person_has_email.append({'id': src_num 
            #                , 'hash': hashmd5
            #                , 'source_id': source_id
            #                , 'person_id': person_id
            #                , 'email_id': email_id
            #                }, ignore_index=True);
            self.df_person_has_email = pd.concat([self.df_person_has_email, \
                    pd.DataFrame.from_records([{ 'id': src_num \
                            , 'hash': hashmd5
                            , 'source_id': source_id
                            , 'person_id': person_id
                            , 'email_id': email_id
                                    }])], \
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
    
    sql = "select count(*) cnt from ppg_data_vault.sources where source_desc = '" + filename + "';"
    print(sql)
    #qcnt = sqlio.read_sql_query(sql, conn)
    cur.execute(sql)
    qcnt = cur.fetchone()[0]
    print(type(qcnt))
    print('qcnt: ', qcnt)
    
    if qcnt == 0 :
        sql = "insert into ppg_data_vault.sources(source_desc) values('" + filename + "');" 
        print(sql)
        cur.execute(sql)
        conn.commit()
        print("insert ", filename)
    
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
    dfsurname = Df_surname(conn)
    dfmiddlename = Df_middlename(conn)
    dfphonenumber = Df_phonenumber(conn)
    dfaddress = Df_address(conn)
    dfdomain = Df_domain(conn)
    dfl1domain = Df_l1domain(conn)
    dfnickname = Df_nickname(conn)
    dfemail = Df_email(conn)
    dfcity = Df_city(conn)
    dfperson = Df_person(conn)
    dfperson_has_email = Df_person_has_email(conn)
    step=''
    
    # Прочесть заголовок файла. Найти номера интересующих столбцов
    
    print('');
    #with open(DATA_DIR + '/' + filename, "r", encoding='cp1251') as txtf:
    with open(DATA_DIR + '/' + filename, "r", encoding='utf8') as txtf:
    #with open(DATA_DIR + '\' + filename, "r", encoding='utf8') as txtf:
    #with open(DATA_DIR + '\\' + filename, "r", encoding='utf8') as txtf:
        
        #src_num = 0
        surn_id = 0
        nam_id = 0
        middlenameid = 0
        birthdate = '1900-01-01'
        phon_id = 0
        city_id = 0
        address_id = 0
        step='preparing'
        num_columns = 0     # Количество колонок
        old_line = ''       # Предыдущая строка, если был перенос до того, 
                        # как набралось количество колонок
        numrow = 1
        csvreader = csv.reader(txtf, delimiter=separator, quotechar='"')
        #for line in txtf: 
        for line_split in csvreader: 
            
            # Перенос строки (\n) портит загрузку
            #line_split = line.split(separator);
            
           
            
            #по первой строке определю где что
            if numrow == 1:
                #print(line);
                print(line_split);
                #num_columns = len(line.split(separator))
                num_columns = len(line_split)
                print('len: ', str(num_columns));
                
                #разобью первую строку по столбцам

                for dict_el in dict_cols:
                    colname_list = dict_cols[dict_el]['colname'];

                    # Перебрать список возможных названий колонки
                    for colname in colname_list:
                        print('looking for colnane ', colname);

                        # найти номер столбца поля colname
                        for i in range(len(line_split)):
                            #print(line_split[i]);
                            if colname == line_split[i].strip():
                                colnum = i+1;
                                dict_cols[dict_el]['colnum'] = colnum;
                                print('colnum:', colnum);

                # если продолжать,то загружаем словарь из файла
                if goon == 1 and os.path.isfile(CHKPNT_FILE):
                    with open(CHKPNT_FILE, "rb") as fchp:
                        d = pickle.load(fchp);
                        print('checkpoint loading. numrow: ', d['numrow']);
                    for _ in range(d['numrow']):
                        next(txtf);
                    numrow = d['numrow'];
                    goon = 0;
                # выведу список полей, которые нашел
                for dict_el in dict_cols:
                    print( dict_cols[dict_el]['colname'][0] + ': ' + str(dict_cols[dict_el]['colnum']));

            # numrow != 1
            else:
                
                if numrow==305699:
                    print('num_columns: ', num_columns)
                    #print(line)
                    print(line_split)
                    print('len(line_split): ', len(line_split))
                    
                 # Если предыдущая строка не полна, то делать склейку
                #if old_line != '':
                #    #склеиваю текущую и следюущую строку
                #    line = old_line + line
                #    print ('extended line: ', line)
                #    line_split = line.split(separator);
                    
                #if num_columns > len(line_split):
                #    print('numrow: ', numrow)
                #    print('len(line_split): ', len(line_split))
                #    print('num_columns: ', num_columns)
                #    print ('len: ', str(len(line_split)))
                #    old_line = line.replace('\n', '')
                #    print ('line: ', line)
                #    print ('old_line: ', old_line)
                #    # Перейти к следующей строке
                #    continue
                #else:
                #    old_line = ''

                step='inserting ' + str(numrow)
                
                
                
                #print('step 1');
                # фамилия
                if dict_cols['surname']['colnum']:
                    surn_id = dfsurname.ins(line_split[dict_cols['surname']['colnum']-1]);
                    #print('surn: ', line_split[dict_cols['surname']['colnum']-1], surn_id);
                    
                # имя
                if dict_cols['name']['colnum']:
                #print('step 2');
                #nam_id = dfnam.get_index(line_split[dict_cols['name']['colnum']]);
                    nam_id = dfname.ins(line_split[dict_cols['name']['colnum']-1]);
                    #print('name: ', line_split[dict_cols['name']['colnum']-1], nam_id);
                    
                # отчество
                if dict_cols['middlename']['colnum']:
                    middlenameid = dfmiddlename.ins(line_split[dict_cols['name']['colnum']-1]);
                    #print('name: ', line_split[dict_cols['name']['colnum']-1], middlenameid);
                    
                # телефон
                #print('step 3');
                if dict_cols['phone']['colnum']:
                    phon_id = dfphonenumber.ins(line_split[dict_cols['phone']['colnum']-1]);
                    #print('phon: ', line_split[dict_cols['phone']['colnum']-1], phon_id);
                
                # адрес
                if dict_cols['address']['colnum']:
                    #print('step 3');
                    address_id = dfphonenumber.ins(line_split[dict_cols['address']['colnum']-1]);
                    #print('address: ', line_split[dict_cols['address']['colnum']-1], address_id);
                
                # city
                if dict_cols['city']['colnum']:
                    #print('step 3');
                    city_id = dfcity.ins(line_split[dict_cols['city']['colnum']-1]);
                    #print('city: ', line_split[dict_cols['city']['colnum']-1], city_id);
                
                # дата рождения
                if dict_cols['birthdate']['colnum'] \
                        and line_split[dict_cols['birthdate']['colnum']-1] != '':
                    birthdate = line_split[dict_cols['birthdate']['colnum']-1]
                    #print('"{}"'.format(birthdate))
                    birthdate = prepare_date(birthdate)
                    #print(birthdate)
                    
                 # 
                if dict_cols['nickname']['colnum']:
                    #nickname_id = 
                    dfnickname.ins(line_split[dict_cols['nickname']['colnum']-1]);
                    #print(birthdate)

                
                # person
                person_id = dfperson.ins(src_num, surn_id, nam_id, middlenameid, birthdate
                , phon_id, city_id)
                #print('person_id: ', person_id)
                
                #if person_id==20221262:
                #    print('dddd')
                #    email = line_split[dict_cols['email']['colnum']-1]
                #    print(email)
                    
                # email
                if dict_cols['email']['colnum']:
                    #print('step 3');
                    email = line_split[dict_cols['email']['colnum']-1]
                    
                    if email != '' and ('@' in email) and ('.' in email) \
                        and email.count('@') == 1:
                        
                        #print ('email: ', email)
                        #print(email.split('@'))
                        nick = email.split('@')[0] 
                            # в некоторых случаях, почему-то несколько @ в почте
                        #print ('nick: ', nick)
                        domain = email.split('@')[1]
                        #print ('domain: ', domain)
                        l1domain = domain.split('.')[1]
                        #print ('l1domain: ', l1domain)
                        domain = domain.split('.')[0]
                        #print ('domain: ', domain)
                        
                        nickname_id = dfnickname.ins(nick)
                        domain_id = dfdomain.ins(domain)
                        l1domain_id = dfl1domain.ins(l1domain)
                            
                        email_id = dfemail.ins(nickname_id, domain_id, l1domain_id);
                        #print('email: ', line_split[dict_cols['email']['colnum']-1], email_id);
                        
                        person_has_emailis = dfperson_has_email.ins(src_num, person_id, email_id)
                
                
                # username
                #print('step 4');
                #unam_id = dfnick.ins(line_split[dict_cols['username']['colnum']]);
                #print('unam: ', unam);
                
                
                # фио: фамилия, имя
                #print('step 5');
                #fio_id = dffio.ins(str(surn_id), str(nam_id));
                #print('fio: ', fio_id);
                
                # lnk(фио, телефон)
                #print('step 6');
                #lnk_fio_phone.ins_element(str(fio_id), str(phon_id));
                
                # lnk(фио, ник)
                #lnk_fio_nick.ins_element(str(fio_id), str(unam_id));
                
                #if colnum != 0:
                    #if dfsur.appended((line.split(separator)[i])):
                        #print(line.split(separator)[i]);
    
            numrow += 1;
            # сохранения и индикация
            if (numrow % 1000 == 0):
                #break
                print(numrow,' :    '
                      , ('|' * int(str(numrow)[0])).ljust(10), '    '
                      , ('|' * int(str(numrow)[1])).ljust(10), '    '
                      , ('|' * int(str(numrow)[2])).ljust(10), '    '
                      , ' - ' * 10)
                #dfnam.save();
                #dfsur.save();
                #dfmail.save();
                #dfphon.save();
                #dfnick.save();
                #dffio.save();
                #lnk_fio_phone.save();
                #lnk_fio_nick.save();
                # сохранить текущее состояние,чтобы можно было начать с того же места
                with open(CHKPNT_FILE, "wb") as fchp:
                    d = {
                        'file_name': filename
                        , 'numrow' : numrow
                        };
                    pickle.dump(d, fchp);
                    

    cur.close()
    conn.close()

except Exception as e:
    print(e.__class__)
    print(e)
    print(step)
finally:
    pass







#