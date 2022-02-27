from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery.schema import SchemaField
import time
import datetime


class BigqueryOperator():
    def __init__(self, credentials_file_path):
        self.credentials = service_account.Credentials.from_service_account_file(credentials_file_path)
        self.client = bigquery.Client(credentials=self.credentials)
        self.table = None
        self.field_type = {
            str: 'STRING',
            bytes: 'BYTES',
            int: 'INTEGER',
            float: 'FLOAT',
            bool: 'BOOLEAN',
            datetime.datetime: 'DATETIME',
            datetime.date: 'DATE',
            datetime.time: 'TIME',
            dict: 'RECORD',
        }

    def _map_dict_to_bq_schema(self, schema_dict):
        # SchemaField list
        schema = []
        for key, value in schema_dict.items():
            if value == list:
                print("schema should not be list")
            schema_field = SchemaField(key, self.field_type[value])  # NULLABLE BY DEFAULT
            schema.append(schema_field)
        return schema

    def get_table(self, table_name, schema_dict: dict, partition_col: dict = None):
        bq_schema = self._map_dict_to_bq_schema(schema_dict)
        table = bigquery.Table(table_name, bq_schema)
        if partition_col:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=partition_col['type'],
                field=partition_col['field']  # name of column to use for partitioning
            )
        try:
            self.client.create_table(table)
        except:
            print(f'{table_name} table exist')
        self.table = table

    def load_data2bigquery(self, data: list = None, batch: int = None) -> list:
        table = self.table
        error_list = []
        n = 0
        while n <= len(data):
            errors = self.client.insert_rows(table, data[n:min(n + batch, len(data))])
            n += batch
            error_list.append(errors)
            print(n, min(n + batch, len(data)))
            time.sleep(1)
        print(f'upload {len(data)} row of data')
        return error_list
