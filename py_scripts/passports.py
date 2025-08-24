import pandas as  pd
from sqlalchemy import create_engine, text
import psycopg2

from . import backup_file

def xlsx2sql_passports(credentials, filepath):
    """
    Функциия, выгружающая из filepath список паспортов, находящихся в черном списке, и загружающая данные в стейджинговую таблицу
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

    # Cоздаем датафрэйм из файла списка паспортов, занесенных в "черный список", на данный момент
    df = pd.read_excel(filepath, sheet_name='blacklist')

    # Создаем стейджинговую таблицу в базе данных первоначальной загрузки списка паспортов, занесенных в "черный список"
    df.to_sql(name="stg_passport_blacklist", con=engine, schema="stg", if_exists="replace", index=False)
    
    # Удаляем созданный датафрейм
    del df
    
    # Вызываем функцию, выполняющую переименование обработанного файла и перемещающая его в папку archive
    backup_file.backup_file(filepath)

def passports_fact(credentials):
    """
    Функциия, создающая таблицу в DWH базы данных с параметрами подключения, указанными в credentials, 
    которая будет хранить актуальную информацию о паспортах, находящихся в черном списке
    """       
    try:
        connection = psycopg2.connect(**credentials)
        cursor = connection.cursor()
        
        # Создадим в используемой базе данных схему DWH, если ее нет
        cursor.execute("CREATE SCHEMA IF NOT EXISTS DWH")
        connection.commit()

        # Создаем таблицу, которая будет хранить историю паспортов, находившихся(находящихся) в "черном списке"  
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dwh.dwh_fact_passport_blacklist(
            passport_num VARCHAR(16),
            entry_dt TIMESTAMP,
            effective_from TIMESTAMP DEFAULT current_timestamp,
            effective_to TIMESTAMP DEFAULT ('5999-12-31 23:59:59'::TIMESTAMP),
            deleted_flg INTEGER DEFAULT 0
            );
        """)
        connection.commit()
        
    except Exception as e:
        print(f'''При выполнении функции "passports_fact" возникла ошибка {e}''')
    
    finally:
        if connection:
            cursor.close()
            connection.close()

def passports_increment(credentials):
    """Функциия, которая наполняет данными тавлицу в DWH базы данных, содержащую актуальную информацию о паспортах, 
    находящихся в черном списке
    """   
    try:
        connection = psycopg2.connect(**credentials)
        cursor = connection.cursor()
        
        # Создаем представление, которое содержит информацию о действующих паспортах, находящихся в "черном списке"  
        cursor.execute("DROP VIEW IF EXISTS stg.stg_v_passport_blacklist")
        cursor.execute("""
            CREATE VIEW stg.stg_v_passport_blacklist AS
                SELECT
                    passport_num,
                    entry_dt
                FROM dwh.dwh_fact_passport_blacklist
                WHERE deleted_flg=0
                AND current_timestamp BETWEEN effective_from AND effective_to        
        """)
        connection.commit()

        # Создаем временную таблицу, которая будет содержать информацию о новых паспортах, внесенных в "черный список"
        cursor.execute("DROP TABLE IF EXISTS stg.stg_passport_blacklist_new")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg.stg_passport_blacklist_new AS
                SELECT
                    t1.passport AS passport_num,
                    t1.date AS entry_dt
                FROM stg.stg_passport_blacklist t1
                LEFT JOIN stg.stg_v_passport_blacklist t2
                ON t1.passport=t2.passport_num
                WHERE t2.passport_num IS NULL
        """)
        connection.commit()
        
        # Создаем временную таблицу, которая будет содержать информацию о паспортах, удаленных из "черного списка" 
        cursor.execute("DROP TABLE IF EXISTS stg.stg_passport_blacklist_deleted")        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg.stg_passport_blacklist_deleted AS
                SELECT
                    t1.passport_num,
                    t1.entry_dt
                FROM stg.stg_v_passport_blacklist t1
                LEFT JOIN stg.stg_passport_blacklist t2
                ON t1.passport_num=t2.passport
                WHERE t2.passport IS NULL
        """)
        connection.commit()
        
        # Обновляем таблицу "fact_passport_blacklist" с учетом полученных данных о новых паспортах, внесенных в "черный список"
        cursor.execute("""
            INSERT INTO dwh.dwh_fact_passport_blacklist (passport_num, entry_dt)
            SELECT passport_num, entry_dt FROM stg.stg_passport_blacklist_new
        """)

        # Обновляем таблицу "fact_passport_blacklist" с учетом полученных данных о паспортах, удаленных из "черного списка"
        cursor.execute("""
            UPDATE dwh.dwh_fact_passport_blacklist
            SET effective_to = current_timestamp - INTERVAL '1 second'
            WHERE passport_num IN (SELECT passport_num FROM stg.stg_passport_blacklist_deleted)
            AND effective_to = '5999-12-31 23:59:59'::TIMESTAMP
        """)
        cursor.execute("""
            INSERT INTO dwh.dwh_fact_passport_blacklist (passport_num, entry_dt, deleted_flg)
            SELECT passport_num, entry_dt, 1 FROM stg.stg_passport_blacklist_deleted
        """)
        
        connection.commit()

    except Exception as e:
        print(f'''При выполнении функции "passports_increment" возникла ошибка {e}''')
    finally:
        if connection:
            cursor.close()
            connection.close()