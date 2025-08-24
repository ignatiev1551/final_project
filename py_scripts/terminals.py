import pandas as  pd
from sqlalchemy import create_engine, text
import psycopg2
 
from . import backup_file

def xlsx2sql_terminals(credentials, filepath):
    """
    Функциия, выгружающая список терминалов полным срезом из filepath, и загружающая данные в стейджинговую таблицу
    базы данных с параметрами подключения, указанными в credentials
    """
    # Формируем URL-адрес для подключения к базе данных
    url = f"postgresql://{credentials['user']}:{credentials['password']}@{credentials['host']}: \
        {credentials['port']}/{credentials['dbname']}"
    engine = create_engine(url)

    # Создадим в используемой базе данных схему STG, если ее нет
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS STG"))

    # Cоздаем датафрэйм из файла списка терминалов полным срезом формата Excel
    df = pd.read_excel(filepath, sheet_name='terminals')

    # Создаем стейджинговую таблицу в базе данных первоначальной загрузки терминалов полным срезом
    df.to_sql(name="stg_terminals", con=engine, schema="stg", if_exists="replace", index=False)
    
    # Удаляем созданный датафрейм
    del df

    # Вызываем функцию, выполняющую переименование обработанного файла и перемещающая его в папку archive
    backup_file.backup_file(filepath)


def terminals_hist(credentials):
    """
    Функциия, создающая таблицу в DWH базы данных с параметрами подключения, указанными в credentials, 
    которая будет хранить информацию (с учетом истории) об установленных терминалах
    """    
    try:
        connection = psycopg2.connect(**credentials)
        cursor = connection.cursor()
        
        # Создадим в используемой базе данных схему DWH, если ее нет
        cursor.execute("CREATE SCHEMA IF NOT EXISTS DWH")
        connection.commit()

        # Создаем таблицу, которая будет хранить историю расположения терминалов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dwh.dwh_dim_terminals_hist(
            terminal_id VARCHAR(16),
            terminal_type VARCHAR(8),
            terminal_city VARCHAR(64),
            terminal_address VARCHAR(128),
            effective_from TIMESTAMP DEFAULT current_timestamp,
            effective_to TIMESTAMP DEFAULT ('5999-12-31 23:59:59'::TIMESTAMP),
            deleted_flg INTEGER DEFAULT 0
            );
        """)
        connection.commit()
        
    except Exception as e:
        print(f'''При выполнении функции "terminals_hist" возникла ошибка {e}''')
    
    finally:
        if connection:
            cursor.close()
            connection.close()

def terminals_increment(credentials):
    """Функциия, которая наполняет данными тавлицу в DWH базы данных с параметрами подключения, указанными в credentials, 
    которая хранит историческую информацию об установленных терминалах
    """
    try:
        connection = psycopg2.connect(**credentials)
        cursor = connection.cursor()
        
        # Создаем представление, которое содержит информацию о действующих терминалах
        cursor.execute("DROP VIEW IF EXISTS stg.stg_v_terminals")
        cursor.execute("""
            CREATE VIEW stg.stg_v_terminals AS
                SELECT
                    terminal_id,
                    terminal_type,
                    terminal_city,
                    terminal_address
                FROM dwh.dwh_dim_terminals_hist
                WHERE deleted_flg=0
                AND current_timestamp BETWEEN effective_from AND effective_to        
        """)
        connection.commit()

        # Создаем временную таблицу, которая будет содержать информацию о новых терминалах
        cursor.execute("DROP TABLE IF EXISTS stg.stg_terminals_new")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg.stg_terminals_new AS
                SELECT
                    t1.terminal_id,
                    t1.terminal_type,
                    t1.terminal_city,
                    t1.terminal_address
                FROM stg.stg_terminals t1
                LEFT JOIN stg.stg_v_terminals t2
                ON t1.terminal_id=t2.terminal_id
                WHERE t2.terminal_id IS NULL
        """)
        connection.commit()
        
        # Создаем временную таблицу, которая будет содержать информацию об удаленных терминалах
        cursor.execute("DROP TABLE IF EXISTS stg.stg_terminals_deleted")        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg.stg_terminals_deleted AS
                SELECT
                    t1.terminal_id,
                    t1.terminal_type,
                    t1.terminal_city,
                    t1.terminal_address
                FROM stg.stg_v_terminals t1
                LEFT JOIN stg.stg_terminals t2
                ON t1.terminal_id=t2.terminal_id
                WHERE t2.terminal_id IS NULL
        """)
        connection.commit()
        
        # Создаем временную таблицу, которая будет содержать информацию о терминалах c измененными данными
        cursor.execute("DROP TABLE IF EXISTS stg.stg_terminals_updated")            
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg.stg_terminals_updated AS
                SELECT
                    t1.terminal_id,
                    t1.terminal_type,
                    t1.terminal_city,
                    t1.terminal_address
                FROM stg.stg_terminals t1
                INNER JOIN stg.stg_v_terminals t2
                ON t1.terminal_id=t2.terminal_id
                WHERE (t1.terminal_type != t2.terminal_type
                    OR (t1.terminal_type IS NULL AND t2.terminal_type IS NOT NULL)
                    OR (t1.terminal_type IS NOT NULL AND t2.terminal_type IS NULL)
                    )
                    OR (t1.terminal_city != t2.terminal_city
                    OR (t1.terminal_city IS NULL AND t2.terminal_city IS NOT NULL)
                    OR (t1.terminal_city IS NOT NULL AND t2.terminal_city IS NULL)
                    )
                    OR (t1.terminal_address != t2.terminal_address
                    OR (t1.terminal_address IS NULL AND t2.terminal_address IS NOT NULL)
                    OR (t1.terminal_address IS NOT NULL AND t2.terminal_address IS NULL)
                    )                   
        """)
        connection.commit()

        # Обновляем таблицу "terminal_hist" с учетом полученных данных о новых терминалах
        cursor.execute("""
            INSERT INTO dwh.dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address)
            SELECT terminal_id, terminal_type, terminal_city, terminal_address FROM stg.stg_terminals_new
        """)

        # Обновляем таблицу "terminal_hist" с учетом полученных данных об удаленных терминалах
        cursor.execute("""
            UPDATE dwh.dwh_dim_terminals_hist
            SET effective_to = current_timestamp - INTERVAL '1 second'
            WHERE terminal_id IN (SELECT terminal_id FROM stg.stg_terminals_deleted)
            AND effective_to = '5999-12-31 23:59:59'::TIMESTAMP
        """)
        cursor.execute("""
            INSERT INTO dwh.dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, deleted_flg)
            SELECT terminal_id, terminal_type, terminal_city, terminal_address, 1 FROM stg.stg_terminals_deleted
        """)
        
        # Обновляем таблицу "terminal_hist" с учетом полученных данных об измененной информации о терминалах
        cursor.execute("""
            UPDATE dwh.dwh_dim_terminals_hist
            SET effective_to = current_timestamp - INTERVAL '1 second'
            WHERE terminal_id IN (SELECT terminal_id FROM stg.stg_terminals_updated)
            AND effective_to = '5999-12-31 23:59:59'::TIMESTAMP
        """)
        cursor.execute("""
            INSERT INTO dwh.dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address)
            SELECT terminal_id, terminal_type, terminal_city, terminal_address FROM stg.stg_terminals_updated
        """)
        
        connection.commit()

    except Exception as e:
        print(f'''При выполнении функции "terminals_increment" возникла ошибка {e}''')
    finally:
        if connection:
            cursor.close()
            connection.close()