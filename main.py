# /usr/bin/python

from ingestion import DataIngestor


def main():
    table_name = "tbl_vendas"
    dataset_dir = "my_data"

    DataIngestor(
        table_name,
        dataset_dir
    )


if __name__ == "__main__":
    main()
