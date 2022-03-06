from pipeline.transformers import Transformer
import unittest
import datetime


class TestTransformer(unittest.TestCase):
    def setUp(self):
        self.data = [{
            "event_date": '20220910', "event_timestamp": "1631285116932001",
            "geo":
            {"continent": "Asia", "metro": "(not set)"},
            "app_info":
            {"id": "com.taitung", "version": "4.0.22",
             }},
            {
            "event_date": '20220920', "event_timestamp": "1631285116932001",
            "geo":
            {"continent": "Asia", "country": "Taiwan", "region": "Taipei City"},
            "app_info":
            {"id": "com.taitung", "version": "4.0.23",
             }}]
        self.data_schema = {
            "event_date": datetime.date,
            "event_timestamp": datetime.datetime,
            "event_name": str,
            "geo": {
                "continent": str,
                "metro": str
            },
            "app_info": {
                "id": str,
                "version": str
            },
            "user_pseudo_id": str}
        self.flatten_data_schema = {
            "event_date": datetime.date,
            "event_name": str,
            "event_timestamp": datetime.datetime,
            'geo_continent': str,
            'geo_metro': str,
            'app_info_id': str,
            "user_pseudo_id": str,
            'app_info_version': str
        }
        self.test_input = [{
            "event_date": "20210910", "event_timestamp": "1631285116932001", "event_name": "screen_view",
            "event_params":
            [{"key": "firebase_event_origin", "value": {"string_value": "auto"}},
             {"key": "ga_session_id", "value": {"int_value": "1631285116"}}, ],
            "privacy_info": {"analytics_storage": "Yes"},
            "user_properties":
            [{"key": "ga_session_id",
              "value": {"int_value": "1631285116", "set_timestamp_micros": "1631285116852000"}},
             {"key": "first_open_time",
              "value": {"int_value": "1628326800000", "set_timestamp_micros": "1628325521168000"}}],
            "user_first_touch_timestamp": "1628325521168000"}]
        self.extra_forign_column = ['event_date', 'event_timestamp', 'event_name']
        self.transformer = Transformer(self. data_schema)

    def test_flatten_dict_data(self):
        expect_output = [{'event_date': '20220910', 'geo_continent': 'Asia',
                          "event_timestamp": "1631285116932001", 'geo_metro': '(not set)',
                          'app_info_id': 'com.taitung', 'app_info_version': '4.0.22'},
                         {'event_date': '20220920', 'geo_continent': 'Asia',
                          "event_timestamp": "1631285116932001", 'geo_country': 'Taiwan',
                          'geo_region': 'Taipei City', 'app_info_id': 'com.taitung',
                          'app_info_version': '4.0.23'}]

        output = self.transformer.flatten_dict_data(self.data)
        self.assertListEqual(expect_output, output)

    def test_flatten_key_value_data(self):

        expect_output = [
            {'key_id': 'event_params', 'key': 'firebase_event_origin', 'string_value': 'auto',
             'event_date': '20210910', 'event_timestamp': '1631285116932001', 'event_name': 'screen_view'},
            {'key_id': 'event_params', 'key': 'ga_session_id', 'int_value': '1631285116',
             'event_date': '20210910', 'event_timestamp': '1631285116932001', 'event_name': 'screen_view'},
            {'key_id': 'user_properties', 'key': 'ga_session_id', 'int_value': '1631285116',
             'set_timestamp_micros': '1631285116852000', 'event_date': '20210910',
             'event_timestamp': '1631285116932001', 'event_name': 'screen_view'},
            {'key_id': 'user_properties', 'key': 'first_open_time', 'int_value': '1628326800000',
             'set_timestamp_micros': '1628325521168000', 'event_date': '20210910',
             'event_timestamp': '1631285116932001', 'event_name': 'screen_view'}]
        output = self.transformer.flatten_key_value_data(
            self.test_input, forign_column=self.extra_forign_column)
        self.assertListEqual(expect_output, output)

    def test_convert_data_type_slice(self):

        test_input = {'event_date': '20220910', 'geo_continent': 'Asia',
                      "event_timestamp": "1631285116932001", 'geo_metro': '(not set)',
                      'app_info_id': 'com.taitung', 'app_info_version': '4.0.22'}
        expect_output = {
            'event_date': datetime.date(2022, 9, 10),
            'event_timestamp': datetime.datetime(2021, 9, 10, 14, 45, 16),
            'geo_continent': 'Asia', 'geo_metro': '(not set)', 'app_info_id': 'com.taitung',
            'app_info_version': '4.0.22', "user_pseudo_id": "", 'event_name': ""
        }
        output = self.transformer.convert_data_type_slice(self.flatten_data_schema, test_input)
        self.assertDictEqual(expect_output, output)

    def test_clean_data(self):
        test_input = [{'event_date': '20220910', 'geo_continent': 'Asia',
                      "event_timestamp": "1631285116932001", 'geo_metro': '(not set)',
                       'app_info_id': 'com.taitung', 'app_info_version': '4.0.22'},
                      {'event_date': '20220913', 'geo_continent': 'Asia',
                      "event_timestamp": "1631285116932001", 'geo_metro': '(not set)',
                       'app_info_id': 'com.taitung', 'app_info_version': '4.0.23'}]
        expect_output = [{
            'event_date': datetime.date(2022, 9, 10),
            'event_name': "",
            'event_timestamp': datetime.datetime(2021, 9, 10, 14, 45, 16),
            'geo_continent': 'Asia', 'geo_metro': '(not set)', 'app_info_id': 'com.taitung',
            'app_info_version': '4.0.22', "user_pseudo_id": ""
        }, {
            'event_date': datetime.date(2022, 9, 13),
            'event_name': "",
            'event_timestamp': datetime.datetime(2021, 9, 10, 14, 45, 16),
            'geo_continent': 'Asia', 'geo_metro': '(not set)', 'app_info_id': 'com.taitung',
            'app_info_version': '4.0.23', "user_pseudo_id": ""
        }]
        output = self.transformer.clean_data(self.flatten_data_schema, test_input)
        self.assertListEqual(expect_output, output)

    def test_flatten_data_schema(self):
        output = self.transformer._flatten_data_schema()
        self.assertDictEqual(self.flatten_data_schema, output)

    def test_transform_dict_data(self):
        expect_output = [
            {'event_date': datetime.date(2022, 9, 10),
             'event_timestamp': datetime.datetime(2021, 9, 10, 14, 45, 16),
             'geo_continent': 'Asia', 'geo_metro': '(not set)', 'app_info_id': 'com.taitung',
             'app_info_version': '4.0.22', 'user_pseudo_id': '', 'event_name': "", },
            {'event_date': datetime.date(2022, 9, 20),
             'event_timestamp': datetime.datetime(2021, 9, 10, 14, 45, 16),
             'geo_continent': 'Asia', 'geo_metro': '', 'app_info_id': 'com.taitung',
             'app_info_version': '4.0.23', 'user_pseudo_id': '', 'event_name': "", }]
        flatten_schema, output = self.transformer.transform_dict_data(self.data)
        self.assertListEqual(expect_output, output)

    def test_transform_key_value_data(self):
        expect_output = [
            {'key_id': 'event_params', 'key': 'firebase_event_origin', 'string_value': 'auto',
             'int_value': '', 'double_value': '', 'float_value': '', 'set_timestamp_micros': '',
             'event_date': datetime.date(2021, 9, 10),
             'event_timestamp': datetime.datetime(2021, 9, 10, 14, 45, 16),
             'event_name': 'screen_view'},
            {'key_id': 'event_params', 'key': 'ga_session_id', 'string_value': '', 'int_value': 1631285116,
             'double_value': '', 'float_value': '', 'set_timestamp_micros': '', 'event_date': datetime.date(
                 2021, 9, 10),
             'event_timestamp': datetime.datetime(2021, 9, 10, 14, 45, 16),
             'event_name': 'screen_view'},
            {'key_id': 'user_properties', 'key': 'ga_session_id', 'string_value': '', 'int_value': 1631285116,
             'double_value': '', 'float_value': '', 'set_timestamp_micros': 1631285116852000,
             'event_date': datetime.date(2021, 9, 10),
             'event_timestamp': datetime.datetime(2021, 9, 10, 14, 45, 16),
             'event_name': 'screen_view'},
            {'key_id': 'user_properties',
             'key': 'first_open_time',
             'string_value': '',
             'int_value': 1628326800000,
             'double_value': '',
             'float_value': '',
             'set_timestamp_micros': 1628325521168000,
             'event_date': datetime.date(2021, 9, 10),
             'event_timestamp': datetime.datetime(2021, 9, 10, 14, 45, 16),
             'event_name': 'screen_view'}]
        expect_data_schema = {'key_id': str,
                              'key': str,
                              'string_value': str,
                              "event_date": datetime.date,
                              "event_timestamp": datetime.datetime,
                              "event_name": str,
                              "int_value": int,
                              "float_value": float,
                              "double_value": float,
                              "set_timestamp_micros": int,
                              }
        schema, output = self.transformer.transform_key_value_data(self.test_input, self.extra_forign_column)
        self.assertListEqual(expect_output, output)
        self.assertDictEqual(schema, expect_data_schema)
