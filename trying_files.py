import os

#вывести текущий путь
curr_dir = os.getcwd();

print('curr_dir = ' + curr_dir);

#Получаем список файлов в переменную files
files = os.listdir(curr_dir );

print('cnt files: ', len(files));

#пробую вывести файлы
for file in files:
	print(file);


#проверяю что файл существует
print('файл существует', os.path.isfile('names.csv'));
print('\n');


#попробую открыть файл, вычитать несколько строк и остаток записать во временный файл.




