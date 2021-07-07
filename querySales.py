import pandas as pd

from cleaner import replace_dtype
from databaseFun import authentication_db, connect_database, create_db_table, execute_values

params_dic = authentication_db()
conn = connect_database(params_dic)
tbl_name = 'tbl_vendas_anual'


# Consolidado de vendas por ano
def get_sales_by_year():
    dataframe = pd.read_sql("""
                SELECT DATE_TRUNC('year', data_venda),
                COUNT(1) AS total_linhas, 
                SUM(qtd_venda) as total_qtd_venda
                FROM tbl_vendas
                GROUP BY 1
                """, con=conn)  # return your first five rows

    return dataframe


# Consolidado de vendas mês;
def get_sales_by_month():
    dataframe = pd.read_sql("""
                    SELECT DATE_TRUNC('month', data_venda),
                    COUNT(1) AS total_linhas, 
                    SUM(qtd_venda) as total_qtd_venda
                    FROM tbl_vendas
                    GROUP BY 1
                    """, con=conn)  # return your first five rows
    return dataframe
    # execute_values(conn, dataframe, tbl_name)


# Consolidado de vendas por marca
def get_sales_by_marca():
    dataframe = pd.read_sql("""
                    SELECT marca,
                    COUNT(1) AS total_linhas, 
                    SUM(qtd_venda) as total_qtd_venda
                    FROM tbl_vendas
                    GROUP BY 1
                    """, con=conn)  # return your first five rows
    return dataframe


# Consolidado de vendas por linha;
def get_sales_by_linha():
    dataframe = pd.read_sql("""
                    SELECT linha,
                    COUNT(1) AS total_linhas, 
                    SUM(qtd_venda) as total_qtd_venda
                    FROM tbl_vendas
                    GROUP BY 1
                    """, con=conn)  # return your first five rows
    return dataframe


# Consolidado de vendas por marca, ano e mês;
def get_sales_by_marca_ano_mes():
    dataframe = pd.read_sql("""
                    SELECT marca,
                    COUNT(1) AS total_linhas, 
                    EXTRACT(YEAR FROM data_venda) as ano,
                    EXTRACT(MONTH FROM data_venda) as mes,
                    SUM(qtd_venda) as total_qtd_venda
                    FROM tbl_vendas
                    GROUP BY data_venda, marca
                    """, con=conn)  # return your first five rows
    return dataframe


# Consolidado de vendas por linha, ano e mês;
def get_sales_by_linha_ano_mes():
    dataframe = pd.read_sql("""
                    SELECT linha,
                    COUNT(1) AS total_linhas, 
                    EXTRACT(YEAR FROM data_venda) as ano,
                    EXTRACT(MONTH FROM data_venda) as mes,
                    SUM(qtd_venda) as total_qtd_venda
                    FROM tbl_vendas
                    GROUP BY data_venda, linha;
                    """, con=conn)  # return your first five rows
    return dataframe


def create_data_sales_by_year_and_month():
    dataframe_year = get_sales_by_year()
    execute_values(conn, dataframe_year, tbl_name)

    dataframe_month = get_sales_by_month()
    execute_values(conn, dataframe_month, tbl_name)


df = get_sales_by_year()
create_db_table(replace_dtype(df), conn, tbl_name)
create_data_sales_by_year_and_month()
