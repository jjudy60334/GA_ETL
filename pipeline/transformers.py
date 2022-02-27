import datetime
flatten_list = []


class Transformer:
    def __init__(self, data_schema):
        self.data_schema = data_schema

    def flatten_dict_data(self, data: list):
        flatten_list = []
        for line in data:
            flatten_json = {}
            for column, value in line.items():
                if type(value) == dict:
                    for op_k, op_v in value.items():  # flatten_data
                        flatten_json[f"{column}_{op_k}"] = op_v
                else:
                    flatten_json[column] = line[column]

            flatten_list.append(flatten_json)
        return flatten_list

    def flatten_key_value_data(self, data: list, forign_column: list):
        flatten_k_v_list = []
        for line in data:
            for column, value in line.items():
                if type(value) == list:
                    for key_value in value:
                        event_key_value = {}
                        event_key_value['key_id'] = column
                        event_key_value['key'] = key_value['key']
                        for v_key, v_value in key_value['value'].items():
                            event_key_value[v_key] = v_value
                        for column_include in forign_column:
                            event_key_value[column_include] = line[column_include]
                        flatten_k_v_list.append(event_key_value)
        return flatten_k_v_list

    def clean_data_slice(self, column_type: dict, data: dict) -> dict:
        converted_data = {}
        for column, v_type in column_type.items():
            if column not in data or data[column] == "":
                data[column] = ""
            elif v_type == datetime.date:
                converted_data[column] = datetime.datetime.strptime(data[column], '%Y%m%d').date()
            elif v_type == datetime.datetime:
                converted_data[column] = datetime.datetime.fromtimestamp(int(int(data[column]) / 10**6))
            else:
                converted_data[column] = v_type(data[column])
        return converted_data

    def clean_data(self, column_type, data: list):
        convert_data_list = []
        for slice_data in data:
            convert_data_list.append(self.clean_data_slice(column_type, slice_data))
        return convert_data_list

    def _flatten_data_schema(self):
        flattened_data_schema = dict()
        for column, v_type in self.data_schema.items():
            if type(v_type) != dict:
                flattened_data_schema[column] = v_type
            else:
                for op_k, op_v in v_type.items():
                    flattened_data_schema[f"{column}_{op_k}"] = op_v
        return flattened_data_schema

    def transform_dict_data(self, data: list) -> list:
        flattened_data_list = self.flatten_dict_data(data)
        flattened_data_schema = self._flatten_data_schema()
        transformed_data = self.clean_data(flattened_data_schema, flattened_data_list)
        return flattened_data_schema, transformed_data

    def transform_key_value_data(self, data: list, forign_column) -> list:
        flattened_data_list = self.flatten_key_value_data(data, forign_column)
        flattened_data_schema = self._flatten_data_schema()
        transformed_data = self.clean_data(flattened_data_schema, flattened_data_list)
        return flattened_data_schema, transformed_data