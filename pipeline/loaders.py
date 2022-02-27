from pipeline.operators.BigqueryOperator import BigqueryOperator


class BigqueryLoaders:
    def __init__(self, credentials_file_path):
        self.credentials_file_path = credentials_file_path
        self.bigquery_operator = BigqueryOperator(credentials_file_path)

    def load(self, table_name, patition_col, table_schema, data, batch):
        self.bigquery_operator.get_table(table_name, table_schema, patition_col)
        self.bigquery_operator.load_data2bigquery(data, batch)
