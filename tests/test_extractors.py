from pipeline.extractors import JsonExtractor
import unittest


class TestJsonExtractor(unittest.TestCase):

    def setUp(self):
        self.file_path = 'tests/test_files/test_Json_extractor.json'

    def test_jsonextractor(self):
        json_extractor = JsonExtractor()

        expect_output = [{
            "schema": {
                "event": "str"
            }
        },
            {
            "schema": {
                "event": "str"
            }
        }]
        output = json_extractor.extract(self.file_path)
        self.assertListEqual(expect_output, output)
