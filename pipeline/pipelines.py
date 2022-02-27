from pipeline.extractors import JsonExtractor
from pipeline.transformers import Transformer
from pipeline.loaders import BigqueryLoaders
from pipeline.logger import LoggingMixin


class BasePipeline(LoggingMixin):
    def __init__(self):
        self._extractor = None
        self._validator = None
        self._loader = None
        self._transformer = None

    @property
    def extractor(self):
        return self._extractor

    @property
    def validator(self):
        return self._validator

    @property
    def transformer(self):
        return self._transformer

    @property
    def loader(self):
        return self._loader

    def execute(self, **kwargs):
        raise NotImplementedError


class GApipeline(BasePipeline):
    def __init__(self, data_schema: dict, credentials_file_path: str):
        super().__init__()
        self._data_schema = data_schema
        self._extractor = JsonExtractor()
        self._transformer = Transformer(self._data_schema)
        self._loader = BigqueryLoaders(credentials_file_path)

    def dict_col_execute(self, file_path, table_name, patition_col, batch):
        self.log.info("Extract data.")
        data = self._extractor.extract(file_path)

        self.log.info("Transform data.")
        table_schema, data = self._transformer.transform_dict_data(data)

        self.log.info("Load data to target location.")
        self._loader.load(table_name, patition_col, table_schema, data, batch)

    def key_value_col_execute(self, file_path, table_name, patition_col, forign_column, batch,):
        self.log.info("Extract data.")
        data = self._extractor.extract(file_path)

        self.log.info("Transform data.")
        table_schema, data = self._transformer.transform_key_value_data(data, forign_column)

        self.log.info("Load data to target location.")
        self._loader.load(table_name, patition_col, table_schema, data, batch)
