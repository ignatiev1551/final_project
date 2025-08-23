import psycopg2

def create_db(credentials):
    """
    Функция, создающая базу данных на сервере PostgreSQL с именем, указанным в передаваемом словаре credentials 
    """
    try:
        # Подключаемся к серверу PostgresSQL(к созданной по умолчанию базе данных postgres)
        connection = psycopg2.connect(
            host=credentials["host"],
            user=credentials["user"],
            password=credentials["password"],
            port=credentials["port"],
            database="postgres"
        )        
        
        # Отключаем транзакции для текущего соединения
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        
        cursor = connection.cursor()

        # Выполняем проверку существования создаваемой базы данных
        cursor.execute("""
            SELECT 1 FROM pg_database WHERE datname = %s
        """, [credentials["dbname"]])
        result = cursor.fetchone()
        
        # Если база данных отсутствует-создаем
        dbname = credentials["dbname"]
        if result is None:
            cursor.execute(f"""
                CREATE DATABASE {dbname};
            """)
            print(f'База данных "{credentials["dbname"]}" успешно создана')
    except Exception as e:
        print(f"Ошибка при создании базы данных: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()