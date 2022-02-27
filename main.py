from pipeline.pipelines import GApipeline
from ga_config import (dict_data_schema,
                       credentials_file_path,
                       file_path, dict_table_name,
                       key_value_table_name, patition_col, forign_column, batch)
pipeline = GApipeline(dict_data_schema, credentials_file_path)
pipeline.dict_col_execute(file_path, dict_table_name, patition_col, batch)
pipeline.key_value_col_execute(file_path, key_value_table_name, patition_col, forign_column, batch)
