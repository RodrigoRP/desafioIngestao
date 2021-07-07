def clean_colname(dataframe):
    # force column names to be lower case, no spaces, no dashes
    dataframe.columns = [
        x.lower().replace(" ", "_").replace("-", "_").replace(r"/", "_").replace("\\", "_").replace(".", "_").replace(
            "$", "").replace("%", "") for x in dataframe.columns]

    return dataframe.columns


def replace_dtype(dataframe):
    replacements = {
        'timedelta64[ns]': 'varchar',
        'object': 'varchar',
        'float64': 'float',
        'int64': 'int',
        'datetime64[ns]': 'date',
        'datetime64[ns, UTC]': 'date'
    }

    col_str = ", ".join(
        "{} {}".format(n, d) for (n, d) in zip(dataframe.columns, dataframe.dtypes.replace(replacements)))
    col_str = col_str.replace('data_venda varchar', 'data_venda date')

    return col_str
