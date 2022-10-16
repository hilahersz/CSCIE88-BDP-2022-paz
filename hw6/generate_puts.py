import dateutil.parser
import pandas as pd

COLUMNS = ['uuid', 'timestamp', 'url', 'userid', 'country', 'ua_browser', 'ua_os', 'response_status', 'TTFB']
HOUR_PATTERN = "%Y-%m-%d:%H"

dfs = []
for i in range(1, 5):
    dfs.append(pd.read_csv(f'sample_data/file-input{i}.csv', header=None, names=COLUMNS))
data = pd.concat(dfs)
data.columns = COLUMNS

data['timestamp'] = data['timestamp'].apply(dateutil.parser.parse).dt.strftime(HOUR_PATTERN)

# Query 1
q1 = data.groupby('timestamp', as_index=False).aggregate(
    unique_urls=("url", "nunique")
)
q1['print'] = q1.apply(lambda r: f"put 'lab6_q1', '{r['timestamp']}', 'quantities:unique_urls', '{r['unique_urls']}'",
                       axis=1)
print("create 'lab6_q1', 'quantities'")
[print(i) for i in q1['print'].iloc[:5].values]

# Query 2
q2 = data.groupby(['timestamp', 'url'], as_index=False).aggregate(
    unique_users=("userid", "nunique"),
    unique_uuid=("uuid", "nunique")
)

print("create 'lab6_q2', 'filters', 'quantities'")

q2['key'] = q2.apply(lambda r: r['timestamp'] + "," + r['url'], axis=1)
q2['print1'] = q2.apply(lambda r: f"put 'lab6_q2', '{r['key']}', "
                                  f"'filters:date_hour', "
                                  f"'{r['timestamp']}'", axis=1)
q2['print2'] = q2.apply(lambda r: f"put 'lab6_q2', '{r['key']}', "
                                  f"'filters:url', "
                                  f"'{r['url']}'", axis=1)
q2['print3'] = q2.apply(lambda r: f"put 'lab6_q2', '{r['key']}', "
                                  f"'quantities:unique_visitors', "
                                  f"'{r['unique_users']}'", axis=1)
q2['print4'] = q2.apply(lambda r: f"put 'lab6_q2', '{r['key']}', "
                                  f"'quantities:unique_uuid', "
                                  f"'{r['unique_uuid']}'", axis=1)

for _, r in q2[q2['url'].isin(['http://example.com/?url=001', 'http://example.com/?url=002'])].iloc[:5].iterrows():
    # print(r['print1'])
    print(r['print2'])
    print(r['print3'])
    print(r['print4'])

q4 = data.groupby(['timestamp', 'country'], as_index=False).aggregate(
    unique_urls=("url", "nunique")
)
q4['key'] = q4.apply(lambda r: r['timestamp'] + "," + r['country'], axis=1)

print("create 'lab6_q4', 'filters', 'quantities'")

q4['print1'] = q4.apply(lambda r: f"put 'lab6_q4', '{r['key']}', "
                                  f"'filters:country', "
                                  f"'{r['country']}'", axis=1)
q4['print2'] = q4.apply(lambda r: f"put 'lab6_q4', '{r['key']}', "
                                  f"'quantities:unique_urls', "
                                  f"'{r['unique_urls']}'", axis=1)

for _, r in q4[q4['country'].isin(['AD', 'AF'])].iloc[:5].iterrows():
    print(r['print1'])
    print(r['print2'])

# Q5

data['day'] = data['timestamp'].str[:10]
q5_info = data.groupby(['day', 'url'], as_index=False).aggregate(
    unique_urls=("TTFB", "mean")
)
q5_info = q5_info.sort_values(by='unique_urls')
q5 = q5_info.groupby('day', as_index=False).head(5).groupby('day', as_index=False).aggregate(
    url=("url", lambda x: list(x))
)

print("create 'lab6_q5', 'quantities'")

q5['print1'] = q5.apply(lambda r: f"put 'lab6_q5', '{r['day']}', "
                                  f"'quantities:top_urls', "
                                  f"'{', '.join(r['url'])}'", axis=1)

for _, r in q5.iloc[:5].iterrows():
    print(r['print1'])

"""
scan 'lab6_q2_q3', { FILTER => "SingleColumnValueFilter(">", '1', 'f1', 'col_a')" }
"""

"""
scan 'lab6_q2_q3', { FILTER => "SingleColumnValueFilter(">", '1', 'f1', 'col_a')" }
"""
"""
scan 't1', {date_hour => [2022-09-03:13, 2022-09-06:13]}
"""
"""
scan 'lab6_q2', {COLUMNS => 'quantities:unique_visitors',  FILTER => "ValueFilter(=, 'binaryprefix:http://example.com/?url=002')"}
scan 'lab6_q2', {FILTER => "ValueFilter(=, 'binaryprefix:http://example.com/?url=002')"}

scan 'lab6_q2' ,{ FILTER => "(SingleColumnValueFilter('filters','url',=, 'binary:http://example.com/?url=002')) }
scan 'lab6_q2' ,{ FILTER => "(ColumnValueFilter('filters','url',=, 'binary:http://example.com/?url=002')) }
"""

"""
FILTER => "ValueFilter( =, 'binaryprefix:<someValue.e.g. test1 AsDefinedInQuestion>' )"
"""


{}

for i in range(10):
    print('hi')
