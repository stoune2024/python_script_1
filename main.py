import argparse
import json
from json.decoder import JSONDecodeError
import pandas as pd


def args_giver():
    """
    Функция обработки параметров командной строки
    :return: Список строк, представляющих пути к лог-файлам
    """
    parser = argparse.ArgumentParser(description='Log analysis')
    parser.add_argument(
        '--file',
        type=str,
        nargs='+',
        help='list of log files to be analysed'
    )
    parser.add_argument(
        '--report',
        type=str,
        default='average',
        help='provide the name of the report (default: average)'
    )
    args = parser.parse_args()
    return args.file


def json_handler(args):
    """
    Функция обработки json-логов. Преобразует json-строки в питоновский список словарей для дальнейшей обработки.
    :param args: Список строк, представляющих пути к лог-файлам
    :return: Список словарей, представляющих преобразованные json-логи
    """
    list_of_dicts = []
    for i in args:
        try:
            with open(i, 'r') as file:
                big_str = file.read()
                json_string_list = big_str.split('\n')
                for i in json_string_list:
                    my_dict = json.loads(i)
                    list_of_dicts.append(my_dict)
        except JSONDecodeError:
            pass
        except FileNotFoundError:
            pass
    return list_of_dicts


def main_dataframe_handler(list_of_dicts):
    """
    Функция создания датафрейма из списка словарей
    :param list_of_dicts: Список словарей
    :return: Главный датафрейм (таблица), состоящая из исходных логов
    """
    df = pd.DataFrame.from_dict(list_of_dicts, orient='columns')
    main_df = df.rename(columns={'url': 'handler'})
    return main_df


def first_df_handler(main_df):
    """
    Функция создания датафрейма с колонкой подсчета количества записей
    :param main_df: Главный датафрейм
    :return: Датафрейм с отчетом по количеству записей
    """
    value_counted_df = main_df['handler'].value_counts().reset_index()
    value_counted_df.columns = ['handler', 'total']
    return value_counted_df


def second_df_handler(main_df):
    """
    Функция создания датафрейма с колонкой
    :param main_df: Главный датафрейм
    :return: Датафрейм с отчетом по среднему времени ответа
    """
    grouped_by_df = main_df.groupby('handler').agg({'response_time': 'mean'}).reset_index()
    grouped_by_df.columns = ['handler', 'avg_response_time']
    return grouped_by_df


def n_df_handler(main_df):
    """
    Аналогично предыдущим двум функциям можно создать новый отчет на основе главного датафрейма.
    Таких функций может быть сколько угодно.
    :param main_df: Главный датафрейм
    :return: Датафрейм с новым отчетом
    """
    pass


def df_merger(value_counted_df, grouped_by_df):
    """
    Функиця объединения n-количества датафреймов с целью создания одного общего отчета.
    :param value_counted_df: Первый датафрейм
    :param grouped_by_df: Второй датафрейм
    :return: Искомый отчет
    """
    final_df = pd.merge(value_counted_df, grouped_by_df, on='handler')
    return print(final_df)


if __name__ == '__main__':
    args = args_giver()
    list_of_dicts = json_handler(args_giver())
    main_df = main_dataframe_handler(json_handler(args_giver()))
    df_merger(
        first_df_handler(main_df),
        second_df_handler(main_df)
    )