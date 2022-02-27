from Operator import BigqueryOperator


class BigqueryLoaders:
    def __init__(self, credentials_file_path):
        self.credentials_file_path = credentials_file_path
        self.BigqeuryLoader = BigqueryOperator(credentials_file_path)

    def load(self, table_name, patition_col, table_schema, data, batch):
        self.BigqeuryLoader.get_table(table_name, table_schema, patition_col)
        self.BigqeuryLoader.load_data2bigquery(data, batch)
