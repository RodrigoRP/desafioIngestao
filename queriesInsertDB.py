import pandas as pd

from cleaner import replace_dtype
from databaseFun import authentication_db, connect_database, execute_values, create_db_table

auth = authentication_db()
conn = connect_database(auth)


def add_result_view_1_db():
    sql_query = pd.read_sql_query(
        '''select * from view1 ''', conn)

    df = pd.DataFrame(sql_query, columns=['ano', 'total_linhas', 'total_qtd_venda'])

    # Replace dtype csv
    col_str = replace_dtype(df)
    # Create table database
    create_db_table(col_str, conn, 'result_view1')
    execute_values(conn, df, 'result_view1')


def add_result_view_2_db():
    sql_query = pd.read_sql_query(
        '''select * from view2 ''', conn)

    df = pd.DataFrame(sql_query, columns=['mes', 'total_linhas', 'total_qtd_venda'])

    # Replace dtype csv
    col_str = replace_dtype(df)
    # Create table database
    create_db_table(col_str, conn, 'result_view2')
    execute_values(conn, df, 'result_view2')


def add_result_view_3_db():
    sql_query = pd.read_sql_query(
        '''select * from view3 ''', conn)

    df = pd.DataFrame(sql_query, columns=['marca', 'total_linhas', 'total_qtd_venda'])

    # Replace dtype csv
    col_str = replace_dtype(df)
    # Create table database
    create_db_table(col_str, conn, 'result_view3')
    execute_values(conn, df, 'result_view3')


def add_result_view_4_db():
    sql_query = pd.read_sql_query(
        '''select * from view4 ''', conn)

    df = pd.DataFrame(sql_query, columns=['ano', 'total_linhas', 'total_qtd_venda'])

    # Replace dtype csv
    col_str = replace_dtype(df)
    # Create table database
    create_db_table(col_str, conn, 'result_view4')
    execute_values(conn, df, 'result_view4')


def add_result_view_5_db():
    sql_query = pd.read_sql_query(
        '''select * from view5 ''', conn)

    df = pd.DataFrame(sql_query, columns=['ano', 'total_linhas', 'total_qtd_venda'])

    # Replace dtype csv
    col_str = replace_dtype(df)
    # Create table database
    create_db_table(col_str, conn, 'result_view5')
    execute_values(conn, df, 'result_view5')


def add_result_view_6_db():
    sql_query = pd.read_sql_query(
        '''select * from view6 ''', conn)

    df = pd.DataFrame(sql_query, columns=['ano', 'total_linhas', 'total_qtd_venda'])

    # Replace dtype csv
    col_str = replace_dtype(df)
    # Create table database
    create_db_table(col_str, conn, 'result_view6')
    execute_values(conn, df, 'result_view6')


add_result_view_1_db()
add_result_view_2_db()
add_result_view_3_db()
add_result_view_4_db()
add_result_view_5_db()
add_result_view_6_db()