import psycopg2

def create_report(credentials):
    """"
    Функция, создающая таблицу-отчет о выявленных мошеннических операциях    
    """
    try:
        connection = psycopg2.connect(**credentials)
        cursor = connection.cursor()
    
        cursor.execute("""
            SET SEARCH_PATH TO DWH;
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rep_fraud(
                event_dt TIMESTAMP,
                passport VARCHAR(128),
                fio VARCHAR(128),
                event_type VARCHAR,
                report_dt TIMESTAMP DEFAULT current_timestamp
            );       
        """)

        cursor.execute("DROP VIEW IF EXISTS transactions_full;")
        cursor.execute("""
            CREATE VIEW transactions_full AS
            SELECT
                t1.trans_date,
                t1.card_num,
                t1.oper_type,
                t1.amt,
                t1.oper_result,
                t2.account,
                t3.valid_to,
                t4.passport_num,
                t4.passport_valid_to,
                CONCAT_WS(' ', t4.last_name, t4.first_name, t4.patronymic) AS fio,
                t4.phone AS phone,
                CASE 
                    /*
                    Формирование условия для поиска операциий, совершенных при недействующем паспорте
                    или паспорте, занесенном в черный список
                    WHEN
                        DATE_TRUNC('day',t1.trans_date) > t4.passport_valid_to::TIMESTAMP
                        OR t4.passport_num in (SELECT passport_num FROM dwh.dwh_fact_passport_blacklist)
                    THEN 0 | (1<<0)
                    */
                    WHEN
                        DATE_TRUNC('day',t1.trans_date) > t3.valid_to::TIMESTAMP
                    THEN 0 | (1<<1)                    
                    /*
                    Формирование условия для поиска операциий, совершенных при недействующем договоре
                    */
                    WHEN
                        DATE_TRUNC('day',t1.trans_date) > t3.valid_to::TIMESTAMP
                    THEN 0 | (1<<1)
                    /*
                    Формирование условия для поиска операциий, совершенных в разных городах в течение часа
                    */
                    WHEN                 
                        (SELECT
                            COUNT(DISTINCT t55.terminal_city)
                        FROM dwh_fact_transactions t11
                        INNER JOIN dwh_dim_terminals_hist t55
                        ON t11.terminal=t55.terminal_id
                        AND t55.deleted_flg=0
                        AND current_timestamp BETWEEN t55.effective_from AND t55.effective_to
                        WHERE t11.card_num=t1.card_num
                        AND t11.trans_date BETWEEN t1.trans_date - INTERVAL '1' HOUR
                        AND t1.trans_date) > 1
                    THEN 0 | (1<<2)
                    /* 
                    Формирование условия для поска 3 операций, совершенных в течене 20 минут,
                    со следующим  шаблоном: каждая последующая меньше предыдущей, при этом отклонены
                    все кроме последней
                    */ 
                    WHEN 
                        (LAG(t1.amt, 2) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date) - 
                        LAG(t1.amt, 1) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date) > 0) 
                        AND 
                        (LAG(t1.amt, 1) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date) - 
                        t1.amt > 0)
                        AND (t1.trans_date - 
                        LAG(t1.trans_date, 2) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date) <= INTERVAL '20' MINUTE) 
                        AND 
                        (LAG(t1.oper_result, 2) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date) = 'REJECT')
                        AND 
                        (LAG(t1.oper_result, 1) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date) = 'REJECT')
                        AND 
                        (t1.oper_result = 'SUCCESS')
                        AND
                        (LAG(t1.oper_type, 2) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date) != 'DEPOSIT')
                        AND 
                        (LAG(t1.oper_type, 1) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date) != 'DEPOSIT')
                        AND 
                        (t1.oper_type != 'DEPOSIT')                        
                    THEN 0 | (1<<3)
                    ELSE 0
                END AS fraud_type,
                t5.terminal_city
            FROM dwh_fact_transactions t1
            INNER JOIN dwh_dim_cards t2
            ON REGEXP_REPLACE(t1.card_num, '\\s', '', 'g')::BIGINT=REGEXP_REPLACE(t2.card_num, '\\s', '', 'g')::BIGINT
            INNER JOIN dwh_dim_accounts t3
            ON t2.account=t3.account
            INNER JOIN dwh_dim_clients t4
            ON LOWER(t3.client)=LOWER(t4.client_id)
            INNER JOIN dwh_dim_terminals_hist t5
            ON t1.terminal=t5.terminal_id
            AND t5.deleted_flg=0
            AND current_timestamp BETWEEN t5.effective_from AND t5.effective_to
            WHERE t1.trans_date>=(SELECT 
                                    MAX(DATE_TRUNC('day', trans_date)) - INTERVAL '1' HOUR  
                                  FROM dwh_fact_transactions);
        """)
        cursor.execute("""
            INSERT INTO rep_fraud(event_dt, passport, fio, event_type)
            SELECT
                trans_date,
                passport_num,
                fio,
                CONCAT_WS(', ' ,
                    CASE WHEN (fraud_type & (1<<0)) != 0 THEN 'просроченный или заблокированный паспорт' END,
                    CASE WHEN (fraud_type & (1<<1)) != 0 THEN 'простроченный договор' END,
                    CASE WHEN (fraud_type & (1<<2)) != 0 THEN 'операции в разных городах в течение часа' END,
                    CASE WHEN (fraud_type & (1<<3)) != 0 THEN 'операции подбора суммы' END
                ) AS event_type
            FROM transactions_full
            WHERE trans_date >= (SELECT 
                                    MAX(DATE_TRUNC('day', trans_date)) 
                                FROM dwh_fact_transactions)
            AND fraud_type != 0;
        """)
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f'При попытке подключения к базе данных "{credentials["dbname"]}" возникла ошибка {e}')