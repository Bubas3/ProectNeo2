import pandas as pd
import psycopg2
from datetime import datetime
import time
from psycopg2 import OperationalError, sql
from settings import DBNAME, DBUSER, DBPASS, DBHOST, DBPORT

#функция подключения к бд
def create_connection(dbname, dbuser, dbpass, dbhost, dbport):
    conn = None
    try:
        conn = psycopg2.connect(database=dbname, user=dbuser, password=dbpass, host=dbhost, port=dbport)
        print('Успешное подключение')
    except OperationalError as e:
        print(f'Ошибка подключения: {e}')
    return conn

#функция добавления в логи
def insert_log_etl(connection, status, message):
    if connection is not None:
        cursor = connection.cursor()
        start_time = datetime.now()
        cursor.execute("INSERT INTO LOGS.ETL_LOG (start_time, status, message) VALUES (%s, %s, %s) RETURNING log_id",
                       (start_time, status, message))
        log_id = cursor.fetchone()[0]
        connection.commit()
        cursor.close()
        return log_id, start_time
    else:
        print('Нет подключения к БД')

# функция апдейта лога
def update_log_etl(connection, log_id, status, message):
    if connection is not None:
        cursor = connection.cursor()
        end_time = datetime.now()
        cursor.execute("UPDATE LOGS.ETL_LOG SET end_time = %s, status = %s, message = %s WHERE log_id = %s",
                       (end_time, status, message, log_id))
        connection.commit()
        cursor.close()
    else:
        print('Нет подключения к БД')


def parse_csv(file_path, sep,enc):
    df = pd.read_csv(file_path, sep=sep, header=0, encoding=enc, parse_dates=True, dtype=str)
    df = df.where(pd.notnull(df), None)  # NaN -> None
    return df


#функция загрузки данных бд
def load_csv_to_db(connection, df, table_name, conflict_columns, mod):
    log_id, start_time = insert_log_etl(connection,'STARTED', f'Loading {table_name}')
    try:
        cursor = connection.cursor()
        if mod == 'full':
            cursor.execute(f'TRUNCATE {table_name} RESTART IDENTITY')

        for i, row in df.iterrows():
            columns = ', '.join(row.index).replace(';',',')
            values = ', '.join(['%s'] * len(row))
            if conflict_columns:
                conflict_action = ', '.join([f"{col} = EXCLUDED.{col}" for col in row.index])
                query = f"""
                    INSERT INTO {table_name} ({columns}) VALUES ({values})
                    ON CONFLICT ({conflict_columns}) DO UPDATE SET {conflict_action}"""
            else:
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
                cursor.execute(query, tuple(row))
        connection.commit()
        cursor.close()
        time.sleep(5)  # пауза на 5 секунд
        update_log_etl(connection,log_id, 'SUCCESS', f'Successfully loaded {table_name}')
        print(f'Таблица {table_name} успешно загружена')
    except Exception as e:
        print(e)
        connection.rollback()
        update_log_etl(connection,log_id, 'FAILED', str(e))

# функция выгрузки данных с бд
def unload_db_to_csv(connection, table_name):
    log_id, start_time = insert_log_etl(connection, 'STARTED', f'UnLoading {table_name}')
    try:
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM {table_name}')
        rows = cursor.fetchall()
        data = pd.DataFrame(rows)
        schema,table = table_name.split('.')
        col_name = pd.read_sql( f"SELECT column_name FROM information_schema.columns where table_schema='{schema}' and table_name='{table}'", con=connection)
        data.columns = col_name['column_name'].values
        data = data.where(pd.notnull(data), None)#NaN -> None
        data.to_csv(f'CSVLOAD/{table_name}.csv',index=False,sep=';',encoding='UTF-8')
        update_log_etl(connection, log_id, 'SUCCESS', f'Successfully unloaded {table_name}')
        print('Файл успешно выгружен')
    except Exception as e:
        connection.rollback()
        update_log_etl(connection, log_id, 'FAILED', str(e))

def check_miss_date(connection, date_to_check):
    cursor = connection.cursor()
    query = sql.SQL("""
            SELECT 1 FROM rd.product
            WHERE %s BETWEEN effective_from_date AND effective_to_date
            LIMIT 1;
        """)
    cursor.execute(query, (date_to_check,))
    result = cursor.fetchone()
    cursor.close()
    return result is None


pd.set_option('display.max_columns', None)  # Показать все колонки
pd.set_option('display.max_rows', None)  # Показать все строки
pd.set_option('display.width', None)  # Ширина для отображения строк
pd.set_option('display.max_colwidth', None)  # Полная ширина колонки

DataFrame_deal_info = parse_csv('data/deal_info.csv', ',', 'cp1251')
DataFrame_dict_currency = parse_csv('data/dict_currency.csv', ',', 'cp1251')
DataFrame_product_info = parse_csv('data/product_info.csv', ',', 'cp1251')
DataFrame_product_info = DataFrame_product_info[DataFrame_product_info['effective_from_date']!='2023-03-15']

connection = create_connection(DBNAME, DBUSER, DBPASS, DBHOST, DBPORT)
load_csv_to_db(connection, DataFrame_dict_currency, 'dm.dict_currency', '','full')
load_csv_to_db(connection, DataFrame_deal_info, 'rd.deal_info','','append')
load_csv_to_db(connection, DataFrame_product_info, 'rd.product','','append')
connection.close()





"""
data1 = pd.read_csv('CSVLOAD/rd.product.csv', sep=';', header=0,encoding="cp65001", parse_dates=True,dtype=str)
data1=data1.sort_values(by='product_rk')
data2 = pd.read_csv('data/product_info.csv', sep=',', header=0,encoding="cp1251", parse_dates=True,dtype=str)
data2 = data2[data2['effective_from_date'] == '2023-03-15']
data2=data2.sort_values(by='product_rk')
data1 = data1.reset_index(drop=True)
data2 = data2.reset_index(drop=True)
print(data1.iloc[2315], data2.iloc[2315])
print(data1.iloc[2316], data2.iloc[2316])
print(data1.compare(data2,align_axis=1,keep_shape=False))
"""
