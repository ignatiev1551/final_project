import pandas as  pd
from sqlalchemy import create_engine, text
import psycopg2

from . import backup_file

def csv2sql_transactions(credentials, filepath):
    """
    Функциия, выгружающая из filepath список транзакций за текущий день, и загружающая данные в стейджинговую таблицу
    базы данных с параметрами подключения, указанными в credentials
    """

    # Формируем URL-адрес для подключения к базе данных
    url = f"postgresql://{credentials['user']}:{credentials['password']}@{credentials['host']}: \
        {credentials['port']}/{credentials['dbname']}"
    engine = create_engine(url)

    # Создадим в используемой базе данных схему DWH, если ее нет
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS STG"))

    # Cоздаем датафрэйм из файла списка транзакций за текущий день
    df = pd.read_csv(filepath, sep=";")

    # Приведем поле "transaction_date" к временному типу, а поле "amount" к числовому типу
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['amount'] = df['amount'].str.replace(',','.').astype(float).round(2)

    # Создаем стейджинговую таблицу транзакций в базе данных первоначальной загрузки транзакции совершенное за текущий день
    df.to_sql(name="stg_transactions", con=engine, schema="stg", if_exists="replace", index=False)

    # Удаляем созданный датафрейм
    del df

    # Вызываем функцию, выполняющую переименование обработанного файла и перемещающая его в папку archive
    backup_file.backup_file(filepath)

def transactions_fact(credentials):
    """
    Функциия, создающая таблицу в DWH базы данных с параметрами подключения, указанными в credentials,
    и заполняющая ее данными о совершенных транзакциях из стейджинговой таблицы 
    """        
    try:
        connection = psycopg2.connect(**credentials)
        cursor = connection.cursor()
        
        # Создадим в используемой базе данных схему DWH, если ее нет
        cursor.execute("CREATE SCHEMA IF NOT EXISTS DWH")
        connection.commit()

        # Создаем таблицу, которая будет хранить совершенные транзакции
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dwh.dwh_fact_transactions(
                trans_id VARCHAR(128) PRIMARY KEY,
                trans_date TIMESTAMP,
                card_num VARCHAR(128),
                oper_type VARCHAR(16),
                amt DECIMAL,
                oper_result VARCHAR(16),
                terminal VARCHAR(16)
            );
        """)
        # Добавляем данные из стейджинговую таблицу транзакций в таблицу фактов совершенных транзакций        
        cursor.execute("""
            INSERT INTO dwh.dwh_fact_transactions (trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal)
            SELECT
                transaction_id,
                transaction_date,
                card_num,
                oper_type,
                amount,
                oper_result,
                terminal
            FROM stg.stg_transactions
        """)

        connection.commit()

        
    except Exception as e:
        print(f'''При выполнении функции "transactions_fact" возникла ошибка {e}''')
    
    finally:
        if connection:
            cursor.close()
            connection.close()