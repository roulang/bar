# -*- coding: utf-8 -*-
import pandas as pd
import urllib.request
from datetime import timedelta
from datetime import datetime
from time import time
import pymongo

fmt = '%Y-%m-%d'
today = datetime.today()
params = [['3y', 3 * 365, 0.25, 0.3], ['2y', 2 * 365, 0.25, 0.25], ['1y', 1 * 365, 0.25, 0.2], ['6m', 6 * 30, 0.33, 0.15], ['3m', 3 * 30, 0.33, 0.1]]
max_pg = 50
ed = today.strftime(fmt)
ed = '2020-05-05'

'''
df_fin = pd.DataFrame()
for para in params:
    p = para[2]
    st = (today - timedelta(days=para[1])).strftime(fmt)
    print(st, ed)

    n = 0
    num = 0
    cnt = 0
    df_mid = pd.DataFrame()
    for i in range(1, max_pg + 1):
        url = "http://fund.eastmoney.com/data/rankhandler.aspx?op=dy&dt=kf&ft=hh&rs=&gs=0&sc=qjzf&st=desc&sd=" + st + "&ed=" + ed + "&es=1&qdii=&pi=" + str(i) + "&pn=50&dx=1"
        print(url)
        while True:
            try:
                r = urllib.request.urlopen(url, timeout=15)
            except Exception as exc:
                print('error %s, wait 5 seconds...' % exc)
                time.sleep(5)
            else:
                break
        content = r.read().decode('utf-8')
        content2 = content[15:-1]

        m = ['datas', 'allRecords', 'pageIndex', 'pageNum', 'allPages', 'allNum', 'gpNum', 'hhNum', 'zqNum', 'zsNum',
             'bbNum', 'qdiiNum', 'etfNum', 'lofNum', 'fofNum']

        for c in m:
            content2 = content2.replace(c, '"' + c + '"')

        d = {'allRecords': 0, 'pageIndex': 0, 'pageNum': 0, 'allPages': 0, 'allNum': 0, 'gpNum': 0, 'hhNum': 0,
             'zqNum': 0, 'zsNum': 0, 'bbNum': 0, 'qdiiNum': 0, 'etfNum': 0, 'lofNum': 0, 'fofNum': 0}

        try:
            d = eval(content2)
        except Exception:
            import traceback
            print ('generic exception: ' + traceback.format_exc())
            exit(1)

        df = pd.DataFrame.from_records(pd.Series(d['datas']).str.split(',').values)
        df2 = df[[0, 1, 2, 3]]
        df2.columns = ['code', 'name', 'name2', 'rose(' + para[0] + ')']
        df_mid = pd.concat([df_mid, df2], ignore_index=True)

        num = int(d['allRecords'])
        pg = d['pageNum']

        n = int(num * p)
        cnt = i * int(d['pageNum'])
        print('n=', n, 'cnt=', cnt)
        if cnt > n:
            break

    df_mid['rank(' + para[0] + ')'] = df_mid.index + 1
    df_mid['rank_r(' + para[0] + ')'] = df_mid['rank(' + para[0] + ')'].astype(str) + '/' + str(num)
    df_mid.set_index('code', inplace=True)

    df_fin = df_fin.combine_first(df_mid)

df_fin.dropna(inplace=True)
# df_fin.to_csv('out.csv', encoding='gbk', index=True)
df_fin.to_pickle('out_pickle_' + ed)
exit(0)

df_fin = pd.read_pickle('out_pickle_' + ed)

df_fin2 = pd.DataFrame()
for cd in df_fin.index:
    url = "http://fund.eastmoney.com/f10/" + cd + ".html"
    print(url)
    tables = pd.read_html(url)
    if len(tables) > 1:
        df = tables[1]
        df1 = df[[0, 1]]
        df2 = df[[2, 3]]
        df1.set_index(0, inplace=True)
        df2.set_index(2, inplace=True)
        df1 = df1.T
        df2 = df2.T
        df1['code'] = cd
        df2['code'] = cd
        df1.set_index('code', inplace=True)
        df2.set_index('code', inplace=True)
        df_mid = pd.concat([df1, df2], axis=1)
        df_fin2 = df_fin2.combine_first(df_mid)

    url2 = "http://fund.eastmoney.com/f10/tsdata_" + cd + ".html"
    print(url2)
    tables2 = pd.read_html(url2)
    if len(tables2) > 1:
        df = tables2[1]
        df['code'] = cd
        df.set_index('code', inplace=True)
        df.drop(u'基金风险指标', axis='columns', inplace=True)
        df2 = df[1:]
        df2.columns = u'夏普比率(' + df2.columns + ')'
        df_fin2 = df_fin2.combine_first(df2)

df_fin2.to_csv('out2.csv', encoding='gbk', index=True)
df_fin2.to_pickle('out_pickle2_' + ed)
exit(0)

df_fin2 = pd.read_pickle('out_pickle2_' + ed)

df_fin3 = df_fin.combine_first(df_fin2)
df_fin3.to_pickle('out_pickle3_' + ed)
exit(0)

df_fin3 = pd.read_pickle('out_pickle3_' + ed)

df_fin4 = df_fin3
df_fin4['rank'] = 0
# params2 = [['3y', 3 * 365, 0.15], ['2y', 2 * 365, 0.15], ['1y', 1 * 365, 0.15], ['6m', 6 * 30, 0.2], ['3m', 3 * 30, 0.2]]
for para in params:
    p = para[2]
    p2 = para[3]
    df_fin4['rank_r2(' + para[0] + ')'] = df_fin4['rank_r(' + para[0] + ')'].apply(lambda x: eval(x))
    df_fin4['rank'] = df_fin4['rank'] + df_fin4['rank_r2(' + para[0] + ')'] * p2
    df_fin4 = df_fin4[df_fin4['rank_r2(' + para[0] + ')'] < p]

df_fin4.sort_values(by='rank', inplace=True)
df_fin4.to_pickle('out_pickle4')
df_fin4.to_csv('out4.csv', encoding='gbk', index=True)
exit(0)

df_fin4 = pd.read_pickle('out_pickle4_' + ed)

pat = u'(.+?)亿元\（'
df_fin4['资产规模(亿)'] = df_fin4['资产规模'].str.extract(pat)
df_fin4['资产规模(亿)'] = df_fin4['资产规模(亿)'].apply(lambda x : eval(x))
df_fin4['业绩比较基准r'] = df_fin4['业绩比较基准'].str.extract(r'([0-9\u4e00-\u9fa5]+指数)')
df_fin5 = df_fin4[(~ df_fin4['name'].str.endswith('C')) & (df_fin4['资产规模(亿)'] > 1)]
df_fin5.to_csv('out5_' + ed + '.csv', encoding='gbk', index=True)
df_fin5.to_pickle('out_pickle5_' + ed)
exit(0)

df = pd.read_pickle('out_pickle5_' + ed)
df.sort_values(by='code', inplace=True)

"""数据插入到 Mongo 数据库中"""
print("writing", len(df.index), "records...")
start = time()

# 锁定集合，并创建索引
client = pymongo.MongoClient('localhost', 27017)
collection = client['fund']['fund_rank']
collection.create_index([('key', pymongo.ASCENDING)], unique=True)

for cd, row in df.iterrows():
    data = row.to_dict()
    data['code'] = cd
    data['key'] = cd + ' ' + ed
    # print(data['key'])
    flt = {'key': data['key']}
    collection.update_one(flt, {'$set': data}, upsert=True)

print(u'插入完毕，耗时：%s' % (time() - start))
exit(0)

df_fin5 = pd.read_pickle('out_pickle5_' + ed)
df_fin6 = df_fin5[:50]
df_fin6.sort_values(by=[u'夏普比率(近3年)', u'夏普比率(近2年)', u'夏普比率(近1年)'], ascending=False, inplace=True)
df_fin6.to_csv('out6_' + ed + '.csv', encoding='gbk', index=True)
df_fin6.to_pickle('out_pickle6_' + ed)
exit(0)
'''

# final
df_fin6 = pd.read_pickle('out_pickle6_' + ed)
# cols = ['基金代码', '基金简称', '基金管理人', '基金类型', '基金经理人',
#         'rank_r(1y)', 'rank_r(2y)', 'rank_r(3m)', 'rank_r(3y)', 'rank_r(6m)', 'rose(1y)', 'rose(2y)', 'rose(3m)', 'rose(3y)', 'rose(6m)',
#         '业绩比较基准', '份额规模', '发行日期', '夏普比率(近1年)', '夏普比率(近2年)', '夏普比率(近3年)', '跟踪标的',
#         'rank', '资产规模(亿)', '业绩比较基准r']
cols = ['基金代码', '基金简称', '基金管理人', '基金类型', '基金经理人',
        'rank_r(3y)', 'rank_r(2y)', 'rank_r(1y)', 'rank_r(6m)', 'rank_r(3m)',
        'rose(3y)', 'rose(2y)', 'rose(1y)', 'rose(6m)', 'rose(3m)',
        '夏普比率(近3年)', '夏普比率(近2年)', '夏普比率(近1年)', '资产规模(亿)']
df_fin7 = df_fin6[:20][cols]
df_fin7.to_csv('out7_' + ed + '.csv', encoding='gbk', index=False)
df_fin7.to_pickle('out_pickle7_' + ed)
print(df_fin7)
exit(0)

df = pd.read_pickle('out_pickle7_' + ed)
df.sort_values(by='code', inplace=True)

"""数据插入到 Mongo 数据库中"""
print("writing", len(df.index), "records...")
start = time()

# 锁定集合，并创建索引
client = pymongo.MongoClient('localhost', 27017)
collection = client['fund']['fund_rank']
collection.create_index([('key', pymongo.ASCENDING)], unique=True)

for cd, row in df.iterrows():
    data = dict()
    data[ed] = 1
    data['key'] = cd + ' ' + ed
    # print(data['key'])
    flt = {'key': data['key']}
    collection.update_one(flt, {'$set': data}, upsert=True)

print(u'插入完毕，耗时：%s' % (time() - start))
exit(0)
