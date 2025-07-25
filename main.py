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

list_of_dicts = []

try:
    with open(f'{args.file[0]}', 'r') as file:
        big_str = file.read()
        json_string_list = big_str.split('\n')
        for i in json_string_list:
            my_dict = json.loads(i)
            list_of_dicts.append(my_dict)
except JSONDecodeError:
    pass

df = pd.DataFrame.from_dict(list_of_dicts, orient='columns')
df_renamed=df.rename(columns={'url':'handler'})
# print(df.head(3))
# print(df[['url']])
# print(df_renamed.head(3))
# df_renamed['total'] = df_renamed[['handler']].value_counts()
df_changed = df_renamed['handler'].value_counts().reset_index()
df_changed.columns = ['handler', 'total']
# final_df = df_changed.groupby('response_time').agg({'response_time': 'mean'})
# print(final_df)
# print(df_changed)

groupby_df = df_renamed.groupby('handler').agg({'response_time': 'mean'}).reset_index()
groupby_df.columns = ['handler', 'avg_response_time']
print(df_changed)
print(groupby_df)