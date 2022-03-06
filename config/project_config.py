
from google.cloud import bigquery
credentials_file_path = 'credentials_file_path'
file_path = 'file_path'
project_id = 'project_id'
destination_dataset = 'destination_dataset'
dict_table = 'dict_table'
dict_table_name = f'{project_id}.{destination_dataset}.{dict_table}'
key_value_table = 'key_value_table'
key_value_table_name = f'{project_id}.{destination_dataset}.{key_value_table}'
patition_col = {'field': 'event_date', 'type': bigquery.TimePartitioningType.DAY}
batch = 2000
forign_column = ['event_date', 'user_pseudo_id', 'event_timestamp', 'event_name', 'platform']
