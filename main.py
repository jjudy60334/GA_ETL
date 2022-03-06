from pipeline.pipelines import GApipeline
from config.schema_config import data_schema
from config.project_config import (
    credentials_file_path,
    file_path, dict_table_name,
    key_value_table_name, patition_col, forign_column, batch)
pipeline = GApipeline(data_schema, credentials_file_path)
pipeline.dict_col_execute(file_path, dict_table_name, patition_col, batch)
pipeline.key_value_col_execute(file_path, key_value_table_name, patition_col, forign_column, batch)
