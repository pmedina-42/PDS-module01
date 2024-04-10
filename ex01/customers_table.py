import os
import psycopg2
from dotenv import dotenv_values

env_vars = dotenv_values('../.env')

DB_USER = env_vars['DB_USER']
DB_PASS = env_vars['DB_PASS']
DB_NAME = env_vars['DB_NAME']
DB_HOST = env_vars['DB_HOST']
DB_PORT = env_vars['DB_PORT']

def insert_data_to_db():
    table_name = 'customers'
    conn = psycopg2.connect(
        dbname = DB_NAME,
        user = DB_USER,
        password = DB_PASS,
        host = DB_HOST,
        port = DB_PORT
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS {} (
            event_time timestamptz,
            event_type text,
            product_id integer,
            price float8,
            user_id bigint,
            user_session text
        )
    """.format(table_name))
    print('Tabla customers creada. Insertando datos...')
    cursor.execute("""
        INSERT INTO {} SELECT * FROM data_2022_oct;
    """.format(table_name))
    print('Datos de la tabla data_2022_oct insertados...')
    cursor.execute("""
        INSERT INTO {} SELECT * FROM data_2022_nov;
    """.format(table_name))
    print('Datos de la tabla data_2022_nov insertados...')
    cursor.execute("""
        INSERT INTO {} SELECT * FROM data_2022_dec;
    """.format(table_name))
    print('Datos de la tabla data_2022_dec insertados...')
    cursor.execute("""
        INSERT INTO {} SELECT * FROM data_2023_jan;
    """.format(table_name))
    print('Datos de la tabla data_2023_jan insertados...')
    cursor.execute("""
        INSERT INTO {} SELECT * FROM data_2023_feb;
    """.format(table_name))
    print('Datos de la tabla data_2023_feb insertados...')

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    insert_data_to_db()