import os
import sys

import dotenv
import psycopg2
from psycopg2 import extras


def authentication_db():
    dotenv.load_dotenv(dotenv.find_dotenv())

    param_dic = {
        "host": os.getenv("host"),
        "database": os.getenv("dbname"),
        "user": os.getenv("user"),
        "password": os.getenv("password")
    }

    return param_dic


def create_db_table(col_str, conn, tbl_name):
    cursor = conn.cursor()
    cursor.execute("drop table if exists %s CASCADE;" % tbl_name)
    cursor.execute("create table %s (%s);" % (tbl_name, col_str))
    conn.commit()
    print('{0} was created successfully'.format(tbl_name))


def create_db_view(conn, view_name, query):
    cursor = conn.cursor()
    cursor.execute("drop view if exists %s;" % view_name)
    cursor.execute("create view %s as (%s);" % (view_name, query))
    conn.commit()
    print('{0} was created successfully'.format(view_name))


def connect_database(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


def execute_values(conn, df, table):
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()
