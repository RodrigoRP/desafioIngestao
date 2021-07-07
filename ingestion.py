from cleaner import clean_colname, replace_dtype
from csvImportFun import get_csv_files, create_df, string_to_date, import_csv_to_database
from databaseFun import create_db_table, connect_database, authentication_db, create_db_view
from queryViews import get_sales_by_year, get_db_views


class DataIngestor:

    def __init__(self, table_name: str, dataset_dir) -> None:
        self.table_name = table_name,
        self.dataset_dir = dataset_dir
        self._import_csv_to_database = self._import_csv_to_database()

    def _import_csv_to_database(self):
        dataset_dir = self.dataset_dir
        table_name = self.table_name[0]
        # Read csv files
        csv_files = get_csv_files()

        # Create df
        df = create_df(dataset_dir, csv_files)
        dataframe = df[csv_files[2]]

        # Clean columns names
        dataframe.columns = clean_colname(dataframe)

        # Replace dtype csv
        col_str = replace_dtype(dataframe)

        # authentication_db
        param_disc = authentication_db()
        # Connect database
        conn = connect_database(param_disc)

        # Create table database
        create_db_table(col_str, conn, table_name)

        sql_views = get_db_views()

        i = 1
        for item in sql_views:
            # Create views
            create_db_view(conn, 'view' + str(i), item)
            i = i + 1

        # Upload Csvfiles to DB
        for k in csv_files:
            # call dataframe
            dataframe = df[k]

            # clean column names
            dataframe.columns = clean_colname(dataframe)
            # Format date to insert db
            string_to_date(dataframe)

            # Dataframe to CSV
            import_csv_to_database(conn, dataframe, k, table_name)

        conn.close()
