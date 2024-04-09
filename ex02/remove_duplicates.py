import os
import psycopg2
from dotenv import dotenv_values

env_vars = dotenv_values('../.env')

DB_USER = env_vars['DB_USER']
DB_PASS = env_vars['DB_PASS']
DB_NAME = env_vars['DB_NAME']
DB_HOST = env_vars['DB_HOST']
DB_PORT = env_vars['DB_PORT']

def remove_duplicates():
    conn = psycopg2.connect(
        dbname = DB_NAME,
        user = DB_USER,
        password = DB_PASS,
        host = DB_HOST,
        port = DB_PORT
    )
    cursor = conn.cursor()
    cursor.execute("""
        ALTER TABLE customers RENAME TO old_customers;
        CREATE TABLE customers AS
        WITH RankedEvents AS (
            SELECT
                event_time,
                event_type,
                product_id,
                price,
                user_id,
                user_session,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        event_type,
                        product_id,
                        user_id,
                        user_session,
                        DATE_TRUNC('minute', event_time)
                ) AS rn
            FROM
                old_customers
        )
        SELECT
            event_time,
            event_type,
            product_id,
            user_id,
            user_session
        FROM
            RankedEvents
        WHERE rn = 1;
    """)
    print('Datos no duplicados insertados en la nueva tabla customers')

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    remove_duplicates()