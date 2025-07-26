import argparse
import json
from json.decoder import JSONDecodeError
import pandas as pd

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

print(args.file)
list_of_dicts = []

for i in args.file:
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
        print('Один из указанных файлов не существует!')

df = pd.DataFrame.from_dict(list_of_dicts, orient='columns')
df_renamed = df.rename(columns={'url': 'handler'})
df_changed = df_renamed['handler'].value_counts().reset_index()
df_changed.columns = ['handler', 'total']

groupby_df = df_renamed.groupby('handler').agg({'response_time': 'mean'}).reset_index()
groupby_df.columns = ['handler', 'avg_response_time']

final_df = pd.merge(df_changed, groupby_df, on='handler')

print(final_df)
