import pandas.core.frame
import pytest
from main import *


@pytest.fixture()
def get_list_files():
    list_of_files = ['example1.log', 'example2.log']
    return list_of_files


@pytest.fixture()
def get_dict_list():
    dict_list = [
        {"@timestamp": "2025-06-22T13:57:32+00:00",
         "status": 200, "url": "/api/context/...",
         "request_method": "GET",
         "response_time": 0.024,
         "http_user_agent": "..."
         },
        {"@timestamp": "2025-06-22T13:57:32+00:00",
         "status": 200, "url": "/api/context/...",
         "request_method": "GET",
         "response_time": 0.02,
         "http_user_agent": "..."
         }
    ]
    return dict_list


def test_json_handler(get_list_files):
    assert json_handler(get_list_files) is not None
    assert {
               "@timestamp": "2025-06-22T13:57:32+00:00",
               "status": 200, "url": "/api/context/...",
               "request_method": "GET",
               "response_time": 0.024,
               "http_user_agent": "..."
           } in json_handler(get_list_files)
    assert {
               "@timestamp": "2025-06-22T13:59:47+00:00",
               "status": 200, "url": "/api/homeworks/...",
               "request_method": "GET",
               "response_time": 0.032,
               "http_user_agent": "..."
           } in json_handler(get_list_files)


def test_json_handler_non_existing_logs():
    list_of_files = ['example1.log', 'example2.log', 'example3.log']
    assert json_handler(list_of_files) is not None
    assert {
               "@timestamp": "2025-06-22T13:57:32+00:00",
               "status": 200, "url": "/api/context/...",
               "request_method": "GET",
               "response_time": 0.024,
               "http_user_agent": "..."
           } in json_handler(list_of_files)
    assert {
               "@timestamp": "2025-06-22T13:59:47+00:00",
               "status": 200, "url": "/api/homeworks/...",
               "request_method": "GET",
               "response_time": 0.032,
               "http_user_agent": "..."
           } in json_handler(list_of_files)


def test_json_handler_invalid_name_logs():
    list_of_files = ['example1.log', 'exaasd123mple2.log']
    assert json_handler(list_of_files) is not None
    assert {
               "@timestamp": "2025-06-22T13:57:32+00:00",
               "status": 200, "url": "/api/context/...",
               "request_method": "GET",
               "response_time": 0.024,
               "http_user_agent": "..."
           } in json_handler(list_of_files)
    assert {
               "@timestamp": "2025-06-22T13:59:47+00:00",
               "status": 200, "url": "/api/homeworks/...",
               "request_method": "GET",
               "response_time": 0.032,
               "http_user_agent": "..."
           } not in json_handler(list_of_files)


def test_main_dataframe_handler(get_dict_list):
    main_df = main_dataframe_handler(get_dict_list)
    assert isinstance(main_df, pandas.core.frame.DataFrame)


def test_main_dataframe_handler_empty_list():
    dict_list = []
    main_df = main_dataframe_handler(dict_list)
    assert main_df is not None
    assert isinstance(main_df, pandas.core.frame.DataFrame)


def test_first_df_handler(get_dict_list):
    main_df = main_dataframe_handler(get_dict_list)
    first_df = first_df_handler(main_df)
    assert isinstance(first_df, pandas.core.frame.DataFrame)
    assert 'handler' in first_df
    assert 'total' in first_df


def test_second_df_handler(get_dict_list):
    main_df = main_dataframe_handler(get_dict_list)
    second_df = second_df_handler(main_df)
    assert isinstance(second_df, pandas.core.frame.DataFrame)
    assert 'handler' in second_df
    assert 'avg_response_time' in second_df


def test_df_merger(get_dict_list):
    main_df = main_dataframe_handler(get_dict_list)
    first_df = first_df_handler(main_df)
    second_df = second_df_handler(main_df)
    df_merged = pd.merge(first_df, second_df, on='handler')
    assert isinstance(df_merged, pandas.core.frame.DataFrame)
    assert 'handler' in df_merged
    assert 'total' in df_merged
    assert 'avg_response_time' in df_merged