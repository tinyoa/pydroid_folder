
import os

import pandas as pd

import hashlib

from datetime import date
from datetime import datetime

class Df_val_set:
	'Список значений'
	df_file_name =  'names.csv';
	VAL_COL_NAME = 'name';
	IND_COL_NAME = 'ind';
	
	def fix(self):
		"исправить ошибки в наборе"
		print('fix');
		for  i, row in self.df.iterrows():
			row[self.VAL_COL_NAME] = row[self.VAL_COL_NAME].lower().strip();
	    
	def delete_by_index(self, ind):
		"удалить из списка элемент с индексом ind"
		if ind in self.df.index.values:
			self.df = self.df.drop(index=[ind]);
		
	def count(self):
		"получить количество элементов в списке"
		return len(self.df.index);
     	
	def print(self):
		"вывести датафрейм списка"
		print(self.df);
		
	def print_df_name_file(self):
		"вывести имя файла"
		print(self.DF_FILE_NAME);


class Df_fio(Df_val_set):
	'класс хранящий фио человека'
	DF_FILE_NAME =  'fio.csv';
	column_list = ['surname', 'name','secondname',  'md5'];
	
	def __init__(self, df_file_name = 'filename.csv', ind_col_name = 'ind'):
		self.df_file_name = df_file_name;
		print('--init Df_fio with "', df_file_name, '"');
		#self.df_file_name = 'names.csv';
		if os.path.isfile(df_file_name):
			self.df = pd.read_csv(df_file_name, index_col=ind_col_name);
			#print(self.df);
			print('df_name_file: ', df_file_name);
		else:
			#print('selfindcolname:', self.IND_COL_NAME);
			#print('columnlist: ', self.column_list);
			self.df = pd.DataFrame(columns = self.column_list);
	
	def get_index(self, surname = '', name = '', secondname = ''):
		"получить индекс элемента (surname, name, secondname)"
		surname = surname.lower().strip();
		name = name.lower().strip();
		secondname =secondname.lower().strip();
		ind_list = self.df[(self.df['surname'] == surname) & (self.df['secondname'] == secondname) & (self.df['name'] == name)].index.tolist();
		if ind_list ==[]:
			hashmd5 = hashlib.md5((surname + name + secondname).encode()).hexdigest();
			self.df = self.df.append({'surname': surname, 'name': name, 'secondname': secondname, 'md5': hashmd5}, ignore_index=True);
			ind_list = self.df[(self.df['surname'] == surname) & (self.df['secondname'] == secondname) & (self.df['name'] == name)].index.tolist();
		return ind_list[0];
	
	def get_rows_like(self, surname = '', name = '', secondname = ''):
		"поиск по неполным данным, если совпадет surname или name или secondname"
		surname = surname.lower().strip();
		name = name.lower().strip();
		secondname =secondname.lower().strip();
		#ind_list = self.df[(self.df['surname'] == surname) & (self.df['secondname'] == secondname) & (self.df['name'] == name)].index.tolist();
		ret_df = pd.DataFrame(columns = self.df.columns);
		if surname != '':
			ret_df = pd.concat([ret_df, self.df[(self.df['surname'] == surname)]]);
	
	def find_rows(self, name, surname, secondname):
		"найти датафрейм подходящих строк"
		#print('find_rows',
		# перепроверить
		surname = surname.lower().strip();
		name = name.lower().strip();
		secondname =secondname.lower().strip();
		return self.df[(self.df['surname'] == surname) & _
				 (self.df['secondname'] == secondname) & _
				 (self.df['name'] == name)].copy();
		
	def find_rows_name(self, name):
		"вернуть датафрейм со строками где имя = name"
		name = name.lower().strip();
		return self.df[(self.df['name'] == name)].copy();
		
	def find_rows_surname(self, surname):
		"вернуть датафрейм со строками где фамилия = surname"
		surname = surname.lower().strip();
		return self.df[(self.df['surname'] == surname)].copy();
	
	def find_rows_secondname(self, secondname):
		"вернуть датафрейм со строками где отчество = secondname"
		secondname= secondname.lower().strip();
		return self.df[(self.df['secondname'] == secondname)].copy();
	
	def appended(self, surname = '', name = '', secondname = ''):
		"""appended добавляет имя name и возвращает 1, если name 
        уже в списке, то возвращает 0"""
		name = name.lower().strip();
		if self.df[(self.df['surname'] == surname) & (self.df['secondname'] == secondname) & (self.df['name'] == name)].index.tolist() == []:
			hashmd5 = hashlib.md5((surname + name + secondname).encode()).hexdigest();
			self.df = self.df.append({'surname': surname, 'name': name, 'secondname': secondname, 'md5': hashmd5}, ignore_index=True);
			return 1;
		else:
			return 0;
	
	def append(self, surname = '', name = '', secondname = ''):
		"добавить набор surname, name, secondname"
		print('append');
		self.appended(surname, name, secondname);
	
	def fix(self):
		"исправить ошибки в регистре"
		print('fix');
		for i, row in self.df.iterrows():
			row['surname'] = row['surname'].lower().strip();
			row['name'] = row['name'].lower().strip();
			row['secondname'] = row['secondname'].lower().strip();
	    	
	def delete(self, surname, name, secondname):
		"удалить запись с фамилией, именем, отчеством = surname, name, secondname"
		name = name.lower().strip();
		surname = surname.lower().strip();
		secondname= secondname.lower().strip();
		print('deleting "', surname, '", "', name, '", "', secondname, '"');
		list_ind=self.df[((self.df['surname'] == surname) & (self.df['secondname'] == secondname) & (self.df['name'] == name))].index.to_list();
		print('deleting index list: ', list_ind);
		self.df.drop(list_ind, inplace=True);
     
	def save(self):
		"сохранить датафрейм в файл"
		print('df_name_file: ', self.df_file_name);
		self.df.to_csv(self.df_file_name, columns=self.column_list, index_label=self.IND_COL_NAME);


class Df_val_cnt(Df_val_set):
	'класс со списком элементов и их количеством'
	val_col_name = 'name';
	CNT_COL_NAME = 'cnt';
	CODE_COL_NAME = 'code';
	
	def __init__(self
                  , df_file_name = 'filename.csv'
                  , col_name = 'name'
                  , ind_col_name = 'ind'):
		"инициализация. Добавление столбца cnt, если его нет."
		self.df_file_name = df_file_name;
		self.val_col_name = col_name;
		self.code_col_name = self.CODE_COL_NAME;
		
		print('--init Df_val_cnt with "', df_file_name, '"');
		#self.df_file_name = 'names.csv';
		if os.path.isfile(df_file_name):
			self.df = pd.read_csv(df_file_name, index_col = ind_col_name);
			#print(self.df);
			print('df_name_file: ', df_file_name);
			# проверю существование колонки cnt. Если нету - создать.
			if not 'cnt' in self.df.columns:
				self.df['cnt'] = None;
				self.df.loc[:, 'cnt'] = 0;
			self.code = self.df[self.code_col_name].max() + 1;
		else:
			self.df = pd.DataFrame(columns = [self.code_col_name
                                             , self.val_col_name
                                             , self.CNT_COL_NAME])
			self.code = 1;

	def in_list(self, name):
		"проверить, есть ли элемент name в списке."
		name = name.lower().strip();
		list_ind = self.df[self.df[self.val_col_name] == name].index.tolist();
		if list_ind ==[]:
			return 0
		else:
			return len(list_ind);
	
	def inc_cnt(self, name):
		"увеличить счетчик элемента name"
		self.df.loc[(self.df[self.val_col_name] == name), self.CNT_COL_NAME] = self.df[self.df[self.val_col_name] == name][self.CNT_COL_NAME].values[0] + 1;
	
	def ins_element(self, name):
		"добавить элемент name"
		self.df = self.df.append({
                self.code_col_name : self.code
                , self.val_col_name: name
                , self.CNT_COL_NAME: 1}
            , ignore_index=True);
		self.code += 1;

	def get_index(self, name):
		"получить индекс элемента name, если нету, то вставить и получить."
		name = name.lower().strip();
		if self.in_list(name):
			self.inc_cnt(name);
		else:
			self.ins_element(name);
		return self.df[self.df[self.val_col_name] == name].index.tolist();
    
	def get_code(self, name):
		"получить индекс элемента name, если нету, то вставить и получить."
		name = name.lower().strip();
		if self.in_list(name):
			self.inc_cnt(name);
		else:
			self.ins_element(name);
		return self.df[self.df[self.val_col_name] == name][self.code_col_name].tolist()[0];
    
	def appended(self, name):
		"""проверить, что элемент уже есть, если нет, то создать. Если элемента 
        не было, то добавить и вернуть 0, если элемент был,то увеличить 
        счетчик, вернуть 1
        """
		name = name.lower().strip();
		if self.in_list(name):
			self.inc_cnt(name);
			return 0;
		else:
			self.ins_element(name);
			return 1;
	
	def append(self, name):
		"отметить элемент name"
		print('Df_val_cnt append', name);
		self.appended(name);
    
	def delete(self, name):
		"получить индекс элемента name, если нету, то вставить и получить."
		name = name.lower().strip();
		if self.in_list(name):
			list_ind=self.df[(self.df[self.val_col_name] == name)].index.to_list();
			print('deleting index list: ', list_ind);
			self.df.drop(list_ind, inplace=True);

	def save(self):
		"сохранить список в файл"
		print('df_name_file: ', self.df_file_name);
		self.df.to_csv(self.df_file_name, columns=[self.code_col_name
                                             , self.val_col_name
                                             , self.CNT_COL_NAME]
                                             , index_label=self.IND_COL_NAME);
	
	def find_rows(self, name):
		"найти датафрейм походящих строк"
		#print('find_rows', name);
		return self.df[self.df[self.val_col_name] == name].copy();


class Df_link_lst():
	'класс связи двух кодов'
	CODE_COL_NAME = 'code';
    
	def __init__(self, df_file_name = 'filename.csv'
                  , l_code = 'l_code'
                  , r_code = 'r_code'
                  , l_hash = 'l_hash'
                  , r_hash = 'r_hash'
                  , ind_col_name = 'ind'
                  , lnk_hash = 'lnk_hash'):
		"инициализация"
		self.df_file_name = df_file_name; # путь к файлу
		self.l_code = l_code;         # название поля для кода левого эл-та
		self.r_code = r_code;         # название поля для кода правого эл-та
		self.l_hash = l_hash;         # название поля для хэша левого эл-та
		self.r_hash = r_hash;         # название поля для хэша правого эл-та
		self.ind_col_name = ind_col_name; # название поля индекса
		self.lnk_hash = lnk_hash;     # навзвание поля хэша строки
		self.code_col_name = self.CODE_COL_NAME;    # Название колонки кода
		
		print('--init Df_link_lst with "', df_file_name, '"');
		#self.df_file_name = 'names.csv';
		if os.path.isfile(df_file_name):
			self.df = pd.read_csv(df_file_name, index_col = ind_col_name);
			#print(self.df);
			# проверю существование колонок. Если нету - создам
			for el in [self.code_col_name, lnk_hash, l_code, r_code, l_hash, r_hash]:
				if not el in self.df.columns:
					self.df[el] = None;
					self.df.loc[:, el] = 0;
			self.code = self.df[self.code_col_name].max() + 1;
		else:
			self.df = pd.DataFrame(columns = [self.code_col_name
                                             , lnk_hash
                                             , l_code
                                             , r_code
                                             , l_hash
                                             , r_hash]);
			self.code = 1;
			
	def in_list_codes(self, l_code, r_code):
		"получить список индексов строк соответствующих кодам l_code, r_code"
		l_code = l_code.lower().strip();
		r_code = r_code.lower().strip();
		list_ind = self.df[(self.df[self.l_code] == l_code)&(self.df[self.r_code] == r_code)].index.tolist();
		if list_ind ==[]:
			return 0
		else:
			return len(list_ind);
		
	def ins_element(self, l_code, r_code):
		"добавить связь между кодами l_code, r_code"
		l_hashmd5 = hashlib.md5(l_code.encode()).hexdigest();
		r_hashmd5 = hashlib.md5(r_code.encode()).hexdigest();
		hashmd5 = hashlib.md5((l_code + r_code).encode()).hexdigest();
		self.df = self.df.append({
                            self.code_col_name: self.code
                            , self.l_code: l_code
                            , self.r_code: r_code
                            , self.l_hash: l_hashmd5
                            , self.r_hash: r_hashmd5
                            , self.lnk_hash: hashmd5}, ignore_index=True);
		self.code += 1;
		
	def save(self):
		"сохранить"
		print('df_name_file: ', self.df_file_name);
		self.df.to_csv(self.df_file_name, columns=[self.code_col_name
                                             , self.l_code
                                             , self.r_code
                                             , self.l_hash
                                             , self.r_hash
                                             , self.lnk_hash]
                                         , index_label=self.ind_col_name);


class Df_hub(Df_val_cnt):
	"класс хаб"
	def __init__(self, df_file_name = 'df_hub.csv', bk = 'bk'
              , src_system_id = 'src_system_id', load_dt = 'load_dt', ind_col_name = 'ind'):
		"инициализация"
		self.df_file_name = df_file_name;
		self.bk = bk;
		self.src_system_id = src_system_id;
		self.load_dt = load_dt;
		self.ind_col_name = ind_col_name;
		
		print('--init Df_hub with "', df_file_name, '"');
		
		if os.path.isfile(df_file_name):
			self.df = pd.read_csv(df_file_name, index_col = ind_col_name);
			#print(self.df);
			# проверю существование колонок. Если нету - создам
			for el in [bk, src_system_id, load_dt]:
				if not el in self.df.columns:
					self.df[el] = None;
					self.df.loc[:, el] = 0;
		else:
			self.df = pd.DataFrame(columns = [bk, src_system_id, load_dt]);
			
	def _in_list_codes(self, bk, src_system_id):
		"получить список индексов строк соответствующих кодам l_code, r_code"
		bk = bk.lower().strip();
		list_ind = self.df[(self.df[self.bk] == bk)&(self.df[self.src_system_id] == src_system_id) ].index.tolist();
		if list_ind ==[]:
			return 0
		else:
			return len(list_ind);
	
	def _ins_element(self, bk, src_system_id):
		"добавить элемент bk из источника src_system_id"
		self.df = self.df.append({self.bk: bk
                            , self.src_system_id: src_system_id
                            #, self.load_dt: datetime.now()
                            , self.load_dt: str(date.today())
                            }, ignore_index=True);


	def get_index(self, bk, src_system_id):
		"получить индекс элемента name, если нету, то вставить и получить."
		bk = bk.lower().strip();
		if self._in_list_codes(bk, src_system_id) == 0:
			self._ins_element(bk, src_system_id);
		return self.df[(self.df[self.bk] == bk)
                 &(self.df[self.src_system_id] 
                   == src_system_id)].index.tolist()[0];
    
	def appended(self, bk, src_system_id):
		"""проверить, что элемент уже есть, если нет, то создать. Если элемента 
        не было, то добавить и вернуть 0, если элемент был,то увеличить 
        счетчик, вернуть 1"""
		bk = bk.lower().strip();
		if self._in_list_codes(bk, src_system_id):
			return 0;
		else:
			self._ins_element(bk, src_system_id);
			return 1;
    
	def append(self, bk, src_system_id):
		"отметить элемент name"
		print('Df_val_cnt append', bk, src_system_id);
		self.appended(bk, src_system_id);
        
	def delete(self, bk, src_system_id):
		"удалить запись с указанным бизнес-ключём"
		'''bk = bk.lower().strip();
		print('deleting "', bk, '", "', src_system_id, '"');
		list_ind=self.df[((self.df['bk'] == bk) 
					& (self.df['src_system_id'] == src_system_id))].index.to_list();
		print('deleting index list: ', list_ind);
		self.df.drop(list_ind, inplace=True);'''
		pass;
        
	def save(self):
		"сохранить"
		print('df_name_file: ', self.df_file_name);
		self.df.to_csv(self.df_file_name
				, columns=[self.bk, self.src_system_id, self.load_dt]
                 , index_label=self.ind_col_name);


class Df_link(Df_val_cnt):
	"класс связи по Data Vault"
	CODE_COL_NAME = 'code';
	
	def __init__(self, df_file_name = 'df_link.csv'
				, link_type = 0
            	, left_id = 'left_id'
            	, right_id = 'right_id'
            	, from_dt = 'from_dt'
            	, to_dt = 'to_dt'
            	, ind_col_name = 'ind'):
		"инициализация"
		self.df_file_name = df_file_name;
		self.link_type = link_type;   # тип соединения (0 - O2O, 1 - O2M, 2 - M2M, 3 - M2O)
		self.left_id = left_id;
		self.right_id = right_id;
		self.from_dt = from_dt;
		self.to_dt = to_dt;
		self.ind_col_name = ind_col_name;
		self.code_col_name = self.CODE_COL_NAME;
		
		print('--init Df_link with "', df_file_name, '"');
		
		if os.path.isfile(df_file_name):
			self.df = pd.read_csv(df_file_name, index_col = ind_col_name);
			#print(self.df);
			# проверю существование колонок. Если нету - создам
			for el in [self.code_col_name, left_id, right_id, from_dt, to_dt]:
				if not el in self.df.columns:
					self.df[el] = None;
					self.df.loc[:, el] = 0;
			self.code = self.df[self.code_col_name].max() + 1;
		else:
			self.df = pd.DataFrame(columns = [self.code_col_name, left_id, right_id, from_dt, to_dt]);
			self.code = 1;
			
	def _in_list_codes(self, left_id, right_id):
		"получить код строк соответствующих кодам left_id, right_id"
		list_ind = self.df[(self.df[self.left_id] == left_id)
				&(self.df[self.right_id] == right_id)][self.CODE_COL_NAME].tolist();    #.index.tolist();
		if list_ind ==[]:
			return 0
		else:
			return list_ind[0];	#len(list_ind);
	
	def _in_list_left(self, left_id):
		"Возвращает признак наличия левого кода среди активных связей"
		list_ind = self.df[(self.df[self.left_id] == left_id)][self.CODE_COL_NAME].tolist();    #.index.tolist();
		if list_ind ==[]:
			return 0
		else:
			return list_ind[0];	#len(list_ind);
    
	def _in_list_right(self, right_id):
		"Возвращает признак наличия правого кода среди активных связей"
		list_ind = self.df[(self.df[self.right_id] == right_id)][self.CODE_COL_NAME].tolist();    #.index.tolist();
		if list_ind ==[]:
			return 0
		else:
			return list_ind[0];	#len(list_ind);
    
	def _ins_element(self, left_id, right_id):
		"добавить связь left_id и right_id"
		self.df = self.df.append({self.code_col_name: self.code 
							, self.left_id: left_id
                            , self.right_id: right_id
                            #, self.from_dt: datetime.now()
                            , self.from_dt: str(date.today())
                            #, self.to_dt: '9999-12-31 23:59:59.99991'
                            , self.to_dt: '9999-12-31'
                            }, ignore_index=True);
		self.code += 1;


	def get_index(self, left_id, right_id):
		"получить индекс связи left_id и right_id, если нету, то вставить и получить."
		#print(str(self._in_list_left(left_id)));
		#print("self.link_type: " + str(self.link_type));
		#print("_in_list_left: " , self._in_list_left(left_id) );
		#print("_in_list_right: " , str(self._in_list_right(right_id)));
		
		# тип соединения (0 - O2O, 1 - O2M, 2 - M2M, 3 - M2O)
		# определяет как именно проверяется вхождение
		if (self.link_type == 0):
			if (self._in_list_left(left_id)==0 and self._in_list_right(right_id)==0): 
				self._ins_element(left_id, right_id);
				print("type 0 ins");
			else:
				# если тип соединения 0 (O2O), то считается вхождение 
				# и левого ид и правого
				return self.df[(self.df[self.left_id] == left_id)
                    |(self.df[self.right_id] == right_id)][self.CODE_COL_NAME].tolist()[0];    #.index.tolist()[0];
        # 1 - O2M
		elif (self.link_type == 1):
			if(self._in_list_codes(left_id, right_id)==0):
				self._ins_element(left_id, right_id);
				#print("type 1 ins");
			else:
				return self.df[(self.df[self.left_id] == left_id)][self.CODE_COL_NAME].tolist()[0];    #.index.tolist()[0];
        # 2 - M2M
		elif (self.link_type == 2):
			if (self._in_list_codes(left_id, right_id)==0): 
				self._ins_element(left_id, right_id);
				#print("type 2 ins");
			else:
				return self.df[(self.df[self.left_id] == left_id)
					|(self.df[self.right_id] == right_id)][self.CODE_COL_NAME].tolist()[0];    #.index.tolist()[0];
        # 3 - M2O
		elif (self.link_type == 3):
			if (self._in_list_codes(left_id, right_id)==0): 
				self._ins_element(left_id, right_id);
				#print("type 3 ins");
			else:
				# если тип соединения 0 (O2O), то считается вхождение 
				# и левого ид и правого
				return self.df[(self.df[self.right_id] == right_id)][self.CODE_COL_NAME].tolist()[0];    #.index.tolist()[0];

    
	def appended(self, left_id, right_id):
		"""проверить и добавить элемент если нет. 
        Если тип соединения 0 (O2O) и элемента не было, то добавить и вернуть 0, 
        если элемент был, то вернуть 1"""
		#print("link_type: " + str(self.link_type));
		#print("self._in_list_codes(left_id, right_id)("+str(left_id)+", "+ str(right_id)+"): " + str(self._in_list_codes(left_id, right_id)));
		
		if self._in_list_codes(left_id, right_id)==0:
			if self.link_type == 0 \
					and (self._in_list_left(left_id)!=0 or self._in_list_right(right_id)!=0):
				print("0 reject insertion: "+str(left_id)+" "+str(right_id));
				return 0;
			elif self.link_type == 1 \
					and (self._in_list_codes(left_id, right_id)!=0):
				print("1 reject insertion: "+str(left_id)+" "+str(right_id));
				return 0;
			elif self.link_type == 3 \
					and (self._in_list_codes(left_id, right_id)!=0):
				print("3 reject insertion: "+str(left_id)+" "+str(right_id));
				return 0;
			else:
				self._ins_element(left_id, right_id);
				return 1;
		else:
			print("reject insertion: "+str(left_id)+" "+str(right_id));
			return 0;
    
	def append(self, left_id, right_id):
		"отметить элемент name"
		#print('Df_val_cnt append', left_id, right_id);
		
		self.appended(left_id, right_id);
        
	def delete(self, left_id, right_id):
		"закрыть привязку left_id к right_id"
		'''bk = bk.lower().strip();
		print('deleting "', bk, '", "', src_system_id, '"');
		list_ind=self.df[((self.df['bk'] == bk) 
					& (self.df['src_system_id'] == src_system_id))].index.to_list();
		print('deleting index list: ', list_ind);
		self.df.drop(list_ind, inplace=True);'''
		pass;
        
	def change(left_id, right_id):
		"""заменить привязку right_id к другому left_id. 
        Закрыть старую привязку left_id к right_id. 
        Открыть сегодняшним днем новую привязку.
        """
		pass;
        
	def save(self):
		"сохранить"
		print('df_name_file: ', self.df_file_name);
		self.df.to_csv(self.df_file_name
				, columns=[self.bk, self.src_system_id, self.load_dt]
                 , index_label=self.ind_col_name);


class Df_sattelite(Df_val_cnt):
    "класс спутника по Data Vault"
    CODE_COL_NAME = 'code';
	
    def __init__(self, df_file_name = 'df_sattelite.csv'
            	, main_entity_code = 'main_entity_code'
                , from_dt = 'from_dt'
            	, to_dt = 'to_dt'
            	, ind_col_name = 'ind'
                , list_of_attributes = ['attr1']
                ):
        "инициализация"
        self.df_file_name = df_file_name;
        self.from_dt = from_dt;
        self.to_dt = to_dt;
        self.ind_col_name = ind_col_name;
        self.code_col_name = self.CODE_COL_NAME;
		
        print('--init Df_link with "', df_file_name, '"');
		
        if os.path.isfile(df_file_name):
            self.df = pd.read_csv(df_file_name, index_col = ind_col_name);
			#print(self.df);
			# проверю существование колонок. Если нету - создам
            for el in [self.code_col_name, from_dt, to_dt].append(list_of_attributes):
                if not el in self.df.columns:
                    self.df[el] = None;
                    self.df.loc[:, el] = 0;
            self.code = self.df[self.code_col_name].max() + 1;
        else:
            self.df = pd.DataFrame(columns = [self.code_col_name, left_id, right_id, from_dt, to_dt]);
            self.code = 1;
			
    def _in_list_codes(self, left_id, right_id):
        "получить код строк соответствующих кодам left_id, right_id"
        list_ind = self.df[(self.df[self.left_id] == left_id)
				&(self.df[self.right_id] == right_id)][self.CODE_COL_NAME].tolist();    #.index.tolist();
        if list_ind ==[]:
            return 0
        else:
            return list_ind[0];	#len(list_ind);
	
    def _in_list_left(self, left_id):
        "Возвращает признак наличия левого кода среди активных связей"
        list_ind = self.df[(self.df[self.left_id] == left_id)][self.CODE_COL_NAME].tolist();    #.index.tolist();
        if list_ind ==[]:
            return 0
        else:
            return list_ind[0];	#len(list_ind);
    
    def _in_list_right(self, right_id):
        "Возвращает признак наличия правого кода среди активных связей"
        list_ind = self.df[(self.df[self.right_id] == right_id)][self.CODE_COL_NAME].tolist();    #.index.tolist();
        if list_ind ==[]:
            return 0
        else:
            return list_ind[0];	#len(list_ind);
    
    def _ins_element(self, left_id, right_id):
        "добавить связь left_id и right_id"
        self.df = self.df.append({self.code_col_name: self.code 
							, self.left_id: left_id
                            , self.right_id: right_id
                            #, self.from_dt: datetime.now()
                            , self.from_dt: str(date.today())
                            #, self.to_dt: '9999-12-31 23:59:59.99991'
                            , self.to_dt: '9999-12-31'
                            }, ignore_index=True);
        self.code += 1;


    def get_index(self, left_id, right_id):
        "получить индекс связи left_id и right_id, если нету, то вставить и получить."
        pass;

    
    def appended(self, left_id, right_id):
        """проверить и добавить элемент если нет. 
        Если тип соединения 0 (O2O) и элемента не было, то добавить и вернуть 0, 
        если элемент был, то вернуть 1"""
		#print("link_type: " + str(self.link_type));
		#print("self._in_list_codes(left_id, right_id)("+str(left_id)+", "+ str(right_id)+"): " + str(self._in_list_codes(left_id, right_id)));

        pass;
    
    def append(self, left_id, right_id):
        "отметить элемент name"
		#print('Df_val_cnt append', left_id, right_id);
		
        self.appended(left_id, right_id);
        
    def delete(self, left_id, right_id):
        "закрыть привязку left_id к right_id"
        pass;
        
    def change(left_id, right_id):
        pass;
        
    def save(self):
        "сохранить"
        print('df_name_file: ', self.df_file_name);
        self.df.to_csv(self.df_file_name
				, columns=[self.bk, self.src_system_id, self.load_dt]
                 , index_label=self.ind_col_name);

	

# проверка Df_val_cnt
#df_email = Df_val_cnt('email.csv');
#print();
#df_email.append('brin@gmail.com');
#df_email.append('bezos@gmail.com');
#df_email.append('thanks@gmail.com');
#df_email.print();
#print('code: ', df_email.get_code('thanks@gmail.com'));
#df_email.append('tliri@gmail.com');
#df_email.appended('bezos@gmail.com');
#df_email.append('bezos@gmail.com');
#df_email.print();
#df_email.delete('thanks@gmail.com');
#df_email.print();
#df_email.save();

#dfnam = Df_name();
#dfnam=Df_val_cnt('names.csv');
#dfnam.print();
#dfnam.append('Инокентий');
#dfnam.append('петя');
#dfnam.fix();
#dfnam.print();
#dfnam.delete('инакентий');
#dfnam.delete_by_index(2);
#dfnam.print();
#dfnam.print_df_name_file();
#dfnam.print_const();
#dfnam.save();



#dfsur = Df_surname();
#dfsur.print();
#dfsur.get_index('дуков');
#dfsur.append('гуков');
#dfsur.fix();
#dfsur.print();
#dfsur.save();


# проверка Df_fio
#dffio = Df_fio();
#print(dffio.get_index('Famil', '    imya   ', 'otch'));
#dffio.append('Dgeronimov', 'petr', 'albertovich');
#dffio.print();
#print(dffio.get_index('Dgeronimov', 'petr', 'albertovich'));
#dffio.fix();
#dffio.delete('   Famil', '  imya  ', 'otch');
#dffio.print();


# проверка Df_hub
#dfhfio = Df_hub('h_name.csv');
#print('dfhfio.get_index (Famil): ', dfhfio.get_index('Famil', 1));
#dfhfio.append('Dgeronimov', 1);
#print();
#dfhfio.print();
#print('dfhfio.get_index (Dgeronimov): ', dfhfio.get_index('Dgeronimov', 1));
#dfhfio.delete('   Famil', 1);
#print('dfhfio.appended (Ismailov): ', dfhfio.appended('Ismailov', 1));
#print();
#dfhfio.print();
#fhfio.print();
        
        
# проверка Df_link
#print("-- -- -- type 0");
#df_lnk0 = Df_link('tst_link0.csv');
#df_lnk0.print();
#df_lnk0.append(1, 1);
#df_lnk0.print();
#df_lnk0.append(1, 2);	# не должна сработать
#df_lnk0.append(1, 1);	# не должна сработать
#df_lnk0.append(2, 1);	# не должна сработать
#df_lnk0.append(2, 3);
#df_lnk0.append(2, 2);	# не должна сработать
#df_lnk0.print();

##print("-- -- -- type 1");
#df_lnk1 = Df_link('tst_link1.csv', 1);
##df_lnk1.print();
#df_lnk1.append(1, 1);
#df_lnk1.print();
#df_lnk1.append(1, 2);
#df_lnk1.append(2, 1);
#df_lnk1.append(2, 2);	
#df_lnk1.append(2, 3);
#df_lnk1.append(2, 2);	# не должна сработать
#df_lnk1.append(1, 2);	# не должна сработать
#df_lnk1.print();

#print("-- -- -- type 2");
#df_lnk1 = Df_link('tst_link2.csv', 2);
#df_lnk1.print();
#df_lnk1.append(1, 1);
#df_lnk1.print();
#df_lnk1.append(1, 2);
#df_lnk1.append(2, 1);
#df_lnk1.append(2, 2);	
#df_lnk1.append(2, 3);
#df_lnk1.append(2, 2);	# не должна сработать
#df_lnk1.append(1, 2);	# не должна сработать
#df_lnk1.print();


#print("-- -- -- type 3");
#df_lnk1 = Df_link('tst_link3.csv', 3);
#df_lnk1.print();
#df_lnk1.append(1, 1);
#df_lnk1.print();
#df_lnk1.append(1, 2);
#df_lnk1.append(2, 1);
#df_lnk1.append(2, 2);	
#df_lnk1.append(2, 3);
#df_lnk1.append(2, 2);	# не должна сработать
#df_lnk1.append(1, 2);	# не должна сработать
#df_lnk1.print();


#print('--------');









