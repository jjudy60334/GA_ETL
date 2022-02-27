from extractors import JsonExtractor
from transformers import Transformer
from loaders import BigqueryLoaders


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


class GAPipeline(BasePipeline):
    def __init__(self, data_schema: dict, credentials_file_path: str):
        self._data_schema = data_schema
        self._extractor = JsonExtractor()
        self._tranforme = Transformer(self._data_schemaa)
        self._loader = BigqueryLoaders(credentials_file_path)
        super().__init__()

    def execute(self, file_path, table_name, patition_col, data, batch, **kwargs):
        self.log.info("Extract data.")
        df = self.extractor.extract(file_path)

        self.log.info("Transform data.")
        table_schema, data = self.transformer.transform_dict_data(data)

        self.log.info("Load data to target location.")
        self.loader.load(self, table_name, patition_col, table_schema, data, batch)
