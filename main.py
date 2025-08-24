import json
import os
import re

from py_scripts import create_db, transactions, terminals, passports, execute_sql_scripts, report

#  Считываем параметры подключения к создаваемой базе данных, хранящиеся в json файле 
with open("cred.json", "r", encoding="utf-8") as f:
    credentials = json.loads(f.read())

# Вызываем функцию, создающую базу данных  на PostgreSQL сервере, c именем, указанным в файле с параметрами подключения
create_db.create_db(credentials)

# Вызываем функцию, выполняющую sql скрипты по созданию и заполнению таблиц c информацией о платежных карточках,
# счетах и клиентах
execute_sql_scripts.execute_sql_scripts("sql_scripts/ddl_dml.sql", credentials)

# Выполним проверку существования директории дата, в которой содержаться файлы ежедневной загрузки
if not os.path.exists("data"):
    raise FileNotFoundError("Директория data не существует")
if not os.path.isdir("data"):
    raise NotADirectoryError("data не является директорией")

files_terminals = []
files_passport_blacklist = []
files_transactions = []

for filename in os.listdir("data"):
    if re.compile(r'^terminals_(\d{2})(\d{2})(\d{4})\.xlsx$').match(filename):
        filepath = os.path.join("data", filename)
        files_terminals.append(filepath)
    elif re.compile(r'^passport_blacklist_(\d{2})(\d{2})(\d{4})\.xlsx$').match(filename):
        filepath = os.path.join("data", filename)
        files_passport_blacklist.append(filepath)   
    elif re.compile(r'^transactions_(\d{2})(\d{2})(\d{4})\.txt$').match(filename):
        filepath = os.path.join("data", filename)
        files_transactions.append(filepath)
    
files_terminals = sorted(files_terminals)
files_passport_blacklist = sorted(files_passport_blacklist)
files_transactions = sorted(files_transactions)

for i in range(len(files_terminals)):
    # Вызываем функцию, загружающие ежедневные данные о терминалах в стейдж
    terminals.xlsx2sql_terminals(credentials, files_terminals[i])
    # Вызываем функцию, создающую таблицу, содержащую информацию о терминалах и хранящую историю их "движения"
    terminals.terminals_hist(credentials)
    # Вызываем функцию, наполняющую инкрементально данными "историческую" таблицу о терминалах
    terminals.terminals_increment(credentials)

    # Вызываем функцию, загружающие ежедневные данные о паспортах, находящихся в "черном" списке в стейдж
    passports.xlsx2sql_passports(credentials, files_passport_blacklist[i])
    # Вызываем функцию, создающую таблицу, содержащую информацию о паспортах, находящихся в "черном" списке
    passports.passports_fact(credentials)
    # Вызываем функцию, наполняющую инкрементально данными таблицу о паспортах, находящихся в "черном" списке
    passports.passports_increment(credentials)

    # Вызываем функцию, загружающие ежедневные данные о транзакциях в стейдж
    transactions.csv2sql_transactions(credentials, files_transactions[i])
    # Вызываем функцию, создающую таблицу транзакций и наполняющую ее данными ежедневно
    transactions.transactions_fact(credentials)

    # Вызываем функцию, создающую "витрину" данных о выявленных мошеннических операциях
    report.create_report(credentials)