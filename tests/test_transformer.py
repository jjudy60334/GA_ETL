from pipeline.transformers import Transformer
import unittest
import datetime


class TestTransformer(unittest.TestCase):
    def setUp(self):
        self.data = [{
            "event_date": '20220910',
            "geo":
            {"continent": "Asia", "metro": "(not set)"},
            "app_info":
            {"id": "com.taitung", "version": "4.0.22",
             }},
            {
            "event_date": '20220920',
            "geo":
            {"continent": "Asia", "country": "Taiwan", "region": "Taipei City"},
            "app_info":
            {"id": "com.taitung", "version": "4.0.23",
             }}]
        self.data_schema = {
            "event_date": datetime.date,
            "geo": {
                "continent": str,
                "country": str,
                "region": str,
                "city": str,
                "sub_continent": str,
                "metro": str
            },
            "app_info": {
                "id": str,
                "version": str,
                "firebase_app_id": str,
                "install_source": str
            }}

    def test_flatten_dict_data(self):
        expect_output = [
            {'event_date': '20220910', 'geo_continent': 'Asia',
             'geo_metro': '(not set)', 'app_info_id': 'com.taitung',
             'app_info_version': '4.0.22'
             },
            {'event_date': '20220920', 'geo_continent': 'Asia', 'geo_country': 'Taiwan',
             'geo_region': 'Taipei City',
             'app_info_id': 'com.taitung', 'app_info_version': '4.0.23',
             }]
        transformer = Transformer(self. data_schema)
        output = transformer.flatten_dict_data(self.data)
        self.assertListEqual(expect_output, output)
