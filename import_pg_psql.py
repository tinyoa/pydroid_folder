# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 22:02:46 2022

@author: Sam

Импортер файлов csv в таблицу грязного импорта в pg


1. Определить структуру файла
    разделитель
    список полей
2. Выделить список полей, которые подходят по содержанию для импорта
3. Построчно прочесть файл и сформировать два файла:
    содержащий только необходимые для импорта поля
    содержаций строки по каким-либо причинам не подходит по формату 
        не совпадает количество столбцов, не совпадает тип столбца


"""








