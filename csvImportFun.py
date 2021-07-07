import os

import pandas as pd


def get_csv_files():
    # get names of only csv files
    csv_files = []
    for file in os.listdir(os.getcwd() + "/my_data"):
        if file.endswith(".csv"):
            csv_files.append(file)

    return csv_files


def string_to_date(df):
    df['data_venda'] = pd.to_datetime(df['data_venda'], format='%d/%m/%Y')
    return df


def create_df(dataset_dir, csv_files):
    data_path = os.getcwd() + '/' + dataset_dir + '/'
    # loop through the files and create the dataframe
    df = {}
    for file in csv_files:
        try:
            df[file] = pd.read_csv(data_path + file, delimiter=';')
        except UnicodeDecodeError:
            df[file] = pd.read_csv(data_path + file, delimiter=';', encoding="utf-8")  # if utf-8 encoding error
        print(file)

    return df


# import_csv_to_database(conn,dataframe,k, table_name)
def import_csv_to_database(conn, dataframe, file, tbl_name):
    cursor = conn.cursor()
    dataframe_columns = dataframe.columns
    dataframe.to_csv(file, header=dataframe_columns, index=False, encoding='utf-8')

    # open the csv file, save it as an object
    my_file = open(file)
    print('file opened in memory')
    # upload to db
    SQL_STATEMENT = """
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """
    cursor.copy_expert(sql=SQL_STATEMENT % tbl_name, file=my_file)
    print('file copied to db')
    conn.commit()
    cursor.close()
    print('table {0} imported to db completed'.format(tbl_name))
