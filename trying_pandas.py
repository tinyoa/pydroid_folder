
import os


#вывести текущий путь
curr_dir = os.getcwd();

print('curr_dir = ' + curr_dir);



import pandas as pd

#df_names = pd.DataFrame(['Петя', 'Паша', 'Вова'], columns=['name'])

#df_names.to_csv('names.csv');
df_names = pd.read_csv('names.csv', index_col='ind');
print(df_names);

df_secondname=pd.DataFrame(['Петров', 'Павлов', 'Иванов', 'Сергеев', 'Мичурин'], columns = ['secondname']);

df_fio = pd.DataFrame(columns = ['surname', 'name', 'secondname']);
print(df_fio);

df_fio = df_fio.append({'surname': 'fam', 'name': 'im', 'secondname': 'otch'}, ignore_index=True);
print(df_fio);

print('trying ');
#print(df_fio[((df_fio['surname'] == 'fam') & (df_fio['secondname'] == 'otch') & (df_fio['name'] == 'im'))].index.to_int(4));

print(df_fio[((df_fio['surname'] == 'fam') & (df_fio['secondname'] == 'otch') & (df_fio['name'] == 'im'))].index.to_list());

df_fio.drop(df_fio[((df_fio['surname'] == 'fam') & (df_fio['secondname'] == 'otch') & (df_fio['name'] == 'im'))].index.to_list(), inplace=True);

print(df_fio);
print();

#df_secondname.to_csv('secondnames.csv', columns=['secondname'], index_label='ind');

#dataframe_prediction.to_csv('filename.csv', sep=',', encoding='utf-8', index=False)

df_secondname = pd.read_csv('secondnames.csv', index_col='ind');

print(df_secondname);


#выведу индекс элемента
print('Sergeev index:',df_secondname[df_secondname['secondname'] == 'Сергеев'].index.tolist());

#индекс несуществующего элемента
print('nonexisting value index:',df_secondname[df_secondname['secondname'] == 'Соломонов'].index.tolist());
if df_secondname[df_secondname['secondname'] == 'Соломонов'].index.tolist() ==[]:
	print('empty index list for value');

# вывести только первый элемент
ind_list = df_secondname[df_secondname['secondname'] == 'Сергеев'].index.tolist();
print('ind_list[0]: ', ind_list)


#добавлю имя и выведу его индекс
df_names = df_names.append({'name': 'Викентий'}, ignore_index=True);
print(df_names);
print();

# попробую найти определенную строку
print('ищу определенную строку');
print(df_names[df_names['name'] == 'паша']['name'].values[0]);
print();

# добавляю к значению ячейки еще одно
df_names.loc[(df_names['name'] == 'паша'), 'name'] = df_names[df_names['name'] == 'паша']['name'].values[0] + 'валера';
#print(df_names['name'] == 'паша');

print(df_names);


#проверить, что столбец существует
if set(['cnt','ts']).issubset(df_names.columns):
	print('Нет определенных колонок');
	
if 'cnt'  in df_names.columns:
	print('колонка cnt есть в датафрейме')
else:
	print('в датафрейме нет колонки cnt');

# скопировать только структуру датафрейма
df_names2 = pd.DataFrame(columns=df_names.columns);
print('df_names2.index.name:', df_names2.index.name, '\n');
print('df_names2', df_names2);

#после копирования добавить снизу несколько строк

print(df_names.loc[1:5]);



