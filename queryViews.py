# Consolidado de vendas por ano
def get_sales_by_year():
    return ("""
                SELECT EXTRACT(YEAR FROM data_venda) AS ano,
                COUNT(1) AS total_linhas, 
                SUM(qtd_venda) as total_qtd_venda
                FROM tbl_vendas
                GROUP BY 1
                """)


# Consolidado de vendas mês;
def get_sales_by_month():
    return ("""
                    SELECT EXTRACT(MONTH FROM data_venda) AS mes,
                    COUNT(1) AS total_linhas, 
                    SUM(qtd_venda) as total_qtd_venda
                    FROM tbl_vendas
                    GROUP BY 1
                    """)


# Consolidado de vendas por marca
def get_sales_by_marca():
    return ("""
                    SELECT marca,
                    COUNT(1) AS total_linhas, 
                    SUM(qtd_venda) as total_qtd_venda
                    FROM tbl_vendas
                    GROUP BY 1
                    """)


# Consolidado de vendas por linha;
def get_sales_by_linha():
    return ("""
                    SELECT linha,
                    COUNT(1) AS total_linhas, 
                    SUM(qtd_venda) as total_qtd_venda
                    FROM tbl_vendas
                    GROUP BY 1
                    """)


# Consolidado de vendas por marca, ano e mês;
def get_sales_by_marca_ano_mes():
    return ("""
                    SELECT marca,
                    COUNT(1) AS total_linhas, 
                    EXTRACT(YEAR FROM data_venda) as ano,
                    EXTRACT(MONTH FROM data_venda) as mes,
                    SUM(qtd_venda) as total_qtd_venda
                    FROM tbl_vendas
                    GROUP BY data_venda, marca
                    """)


# Consolidado de vendas por linha, ano e mês;
def get_sales_by_linha_ano_mes():
    return ("""
                    SELECT linha,
                    COUNT(1) AS total_linhas, 
                    EXTRACT(YEAR FROM data_venda) as ano,
                    EXTRACT(MONTH FROM data_venda) as mes,
                    SUM(qtd_venda) as total_qtd_venda
                    FROM tbl_vendas
                    GROUP BY data_venda, linha
                    """)


def get_db_views():
    queries = [get_sales_by_year(), get_sales_by_month(), get_sales_by_marca(), get_sales_by_linha(),
               get_sales_by_marca_ano_mes(), get_sales_by_linha_ano_mes()]

    return queries
