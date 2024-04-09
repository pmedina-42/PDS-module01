import os
import psycopg2
from dotenv import dotenv_values

env_vars = dotenv_values('../.env')

DB_USER = env_vars['DB_USER']
DB_PASS = env_vars['DB_PASS']
DB_NAME = env_vars['DB_NAME']
DB_HOST = env_vars['DB_HOST']
DB_PORT = env_vars['DB_PORT']

def fusion():
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
        ALTER TABLE customers
        ADD COLUMN category_code TEXT,
        ADD COLUMN category_id TEXT,
        ADD COLUMN brand TEXT;

        WITH items_aggregated AS (
            SELECT
                product_id,
                STRING_AGG(DISTINCT category_code, ',') AS category_codes,
                STRING_AGG(DISTINCT CAST(category_id AS VARCHAR), ',') AS category_ids,
                STRING_AGG(DISTINCT brand, ',') AS brands
            FROM item
            GROUP BY product_id
        )
        UPDATE customers c
        SET
            category_code = i.category_codes,
            category_id = i.category_ids,
            brand = i.brands
        FROM items_aggregated i
        WHERE c.product_id = i.product_id;
    """.format(table_name))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    fusion()