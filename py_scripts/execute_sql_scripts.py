import psycopg2

def execute_sql_scripts(filepath, credentials):
    """
    Функция, выпоняяющая sql scripts, прописанные в файле filepath, создающие таблицы в базе данных с
    параметрами подключения, указанными в credentials, и заполняющие эти таблицы данными
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        sql_scripts = f.read()
        sql_scripts_modified = sql_scripts.replace("cards","dwh_dim_cards")\
                                            .replace("accounts","dwh_dim_accounts")\
                                            .replace("clients","dwh_dim_clients")
    try:
        connection = psycopg2.connect(**credentials)
        cursor = connection.cursor()
        # Создадим в используемой базе данных схему DWH, если ее нет
        cursor.execute("CREATE SCHEMA IF NOT EXISTS DWH")
        connection.commit()       
        cursor.execute("SET SEARCH_PATH TO DWH")
        # Запускаем выполение sql скрипта, прочитанного из файла
        cursor.execute(sql_scripts_modified)
        connection.commit()
    except Exception as e:
        print(f'''При попытке подключения к базе данных "{credentials["dbname"]}" и 
        выполнения sql скрипта содержащегося в файле "{filepath}" возникла ошибка {e}''')
    finally:
        if connection:
            cursor.close()
            connection.close()