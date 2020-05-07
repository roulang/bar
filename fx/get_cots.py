# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import pymongo
from time import time
import mpl_finance as mpf
from datetime import timedelta
from datetime import datetime
# import requests
# import re

target = 'EURUSD'
# target = 'USDJPY'
# target = 'GBPUSD'
# target = 'AUDUSD'
# target = 'NZDUSD'
# target = 'USDCAD'
# target = 'USDCHF'
sd = '2020'
ed = '2021'
fn = target + '1440.csv'
p0 = "['LL-LS','LL+LS']"
# p0 = "['NL-NS','NL+NS']"
# p0 = "['DL','DS']"
# p0 = "['OL','OS']"
# p0 = "['NL','NS']"
# p0 = "['AL','AS']"
# p1 = "['COT_NAME', 'DATE', 'LL', 'LS']"
p1 = "['COT_NAME', 'DATE', 'LL-LS','LL+LS']"
# p1 = "['COT_NAME', 'DATE', 'NL-NS', 'NL+NS']"
# p1 = "['COT_NAME', 'DATE', 'DL', 'DS']"
# p1 = "['COT_NAME', 'DATE', 'OL', 'OS']"
# p1 = "['COT_NAME', 'DATE', 'NL', 'NS']"
# p1 = "['COT_NAME', 'DATE', 'AL', 'AS']"

min_days = 11
max_days = 18
df_fin = None

url_new = 'https://www.cftc.gov/dea/newcot/FinFutWk.txt'
url_old = 'https://www.cftc.gov/files/dea/history/fut_fin_txt_2020.zip'
# url_new = 'FinFutWk.txt'
# url_old = 'FinFutYY_new2.txt'

titles = {
    "Market_and_Exchange_Names": "Market_and_Exchange_Names",
    "As_of_Date_In_Form_YYMMDD": "As_of_Date",
    "Report_Date_as_YYYY-MM-DD": "Report_Date",
    "CFTC_Contract_Market_Code": "Contract_Market_Code",
    "CFTC_Market_Code": "Market_Code",
    "CFTC_Region_Code": "Region_Code",
    "CFTC_Commodity_Code": "Commodity_Code",
    "Open_Interest_All": "OI",
    "Dealer_Positions_Long_All": "DL",
    "Dealer_Positions_Short_All": "DS",
    "Dealer_Positions_Spread_All": "DD",
    "Asset_Mgr_Positions_Long_All": "AL",
    "Asset_Mgr_Positions_Short_All": "AS",
    "Asset_Mgr_Positions_Spread_All": "AD",
    "Lev_Money_Positions_Long_All": "LL",
    "Lev_Money_Positions_Short_All": "LS",
    "Lev_Money_Positions_Spread_All": "LD",
    "Other_Rept_Positions_Long_All": "OL",
    "Other_Rept_Positions_Short_All": "OS",
    "Other_Rept_Positions_Spread_All": "OD",
    "Tot_Rept_Positions_Long_All": "TL",
    "Tot_Rept_Positions_Short_All": "TS",
    "NonRept_Positions_Long_All": "NL",
    "NonRept_Positions_Short_All": "NS",
}

items2 = {
    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "USDCAD",
    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE": "USDCHF",
    "BRITISH POUND STERLING - CHICAGO MERCANTILE EXCHANGE": "GBPUSD",
    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE": "USDJPY",
    "EURO FX - CHICAGO MERCANTILE EXCHANGE": "EURUSD",
    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "AUDUSD",
    "EURO FX/BRITISH POUND XRATE - CHICAGO MERCANTILE EXCHANGE": "",
    "RUSSIAN RUBLE - CHICAGO MERCANTILE EXCHANGE": "",
    "MEXICAN PESO - CHICAGO MERCANTILE EXCHANGE": "",
    "BRAZILIAN REAL - CHICAGO MERCANTILE EXCHANGE": "",
    "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE": "NZDUSD",
    "SOUTH AFRICAN RAND - CHICAGO MERCANTILE EXCHANGE": "",
    "DJIA Consolidated - CHICAGO BOARD OF TRADE": "",
    "DOW JONES INDUSTRIAL AVG- x $5 - CHICAGO BOARD OF TRADE": "",
    "DOW JONES U.S. REAL ESTATE IDX - CHICAGO BOARD OF TRADE": "",
    "S&P 500 Consolidated - CHICAGO MERCANTILE EXCHANGE": "",
    "S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE": "",
    "E-MINI S&P ENERGY INDEX - CHICAGO MERCANTILE EXCHANGE": "",
    "E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE": "",
    "NASDAQ-100 Consolidated - CHICAGO MERCANTILE EXCHANGE": "",
    "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE": "",
    "E-MINI RUSSELL 2000 INDEX - CHICAGO MERCANTILE EXCHANGE": "",
    "NIKKEI STOCK AVERAGE YEN DENOM - CHICAGO MERCANTILE EXCHANGE": "",
    "MSCI EAFE MINI INDEX - ICE FUTURES U.S.": "",
    "MSCI EMERGING MKTS MINI INDEX - ICE FUTURES U.S.": "",
    "E-MINI S&P 400 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE": "",
    "S&P 500 ANNUAL DIVIDEND INDEX - CHICAGO MERCANTILE EXCHANGE": "",
    "U.S. TREASURY BONDS - CHICAGO BOARD OF TRADE": "",
    "ULTRA U.S. TREASURY BONDS - CHICAGO BOARD OF TRADE": "",
    "2-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE": "",
    "10-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE": "",
    "ULTRA 10-YEAR U.S. T-NOTES - CHICAGO BOARD OF TRADE": "",
    "5-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE": "",
    "30-DAY FEDERAL FUNDS - CHICAGO BOARD OF TRADE": "",
    "3-MONTH EURODOLLARS - CHICAGO MERCANTILE EXCHANGE": "",
    "3-MONTH SOFR - CHICAGO MERCANTILE EXCHANGE": "",
    "1-MONTH SOFR - CHICAGO MERCANTILE EXCHANGE": "",
    "10 YEAR DELIVERABLE IR - CHICAGO BOARD OF TRADE": "",
    "BITCOIN - CHICAGO MERCANTILE EXCHANGE": "",
    "U.S. DOLLAR INDEX - ICE FUTURES U.S.": "USDX",
    "VIX FUTURES - CBOE FUTURES EXCHANGE": "",
    "BLOOMBERG COMMODITY INDEX - CHICAGO BOARD OF TRADE": "",
}

items = {
    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "USDCAD",
    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE": "USDCHF",
    "BRITISH POUND STERLING - CHICAGO MERCANTILE EXCHANGE": "GBPUSD",
    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE": "USDJPY",
    "EURO FX - CHICAGO MERCANTILE EXCHANGE": "EURUSD",
    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "AUDUSD",
    "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE": "NZDUSD",
    "U.S. DOLLAR INDEX - ICE FUTURES U.S.": "USDX",
}

curs = ['USDX', 'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'USDCAD', 'AUDUSD', 'NZDUSD']


def read_cot_his_from_db():
    print("Read old COTS from db, please wait...")

    client = pymongo.MongoClient('localhost', 27017)
    collection = client['fx']['cots']
    data_frame = pd.DataFrame(list(collection.find({"Report_Date_as_YYYY-MM-DD": {"$gte": sd}}, {"_id": 0})))
    # data_frame = pd.DataFrame(list(collection.find({"$and": [{"Report_Date_as_YYYY-MM-DD": {"$gte": sd}},
    #                                                          {"Report_Date_as_YYYY-MM-DD": {"$lte": ed}}]},
    #                                                {"_id": 0})))

    print("Read OK")

    return data_frame


def read_cot_his_from_web():
    print("Read old COTS from web, please wait...")

    data_frame = pd.read_csv(
        filepath_or_buffer=url_old,
    )

    print("Read OK")

    """数据插入到 Mongo 数据库中"""
    print("writing", len(data_frame.index), "records...")
    start = time()

    # 锁定集合，并创建索引
    client = pymongo.MongoClient('localhost', 27017)
    collection = client['fx']['cots']
    collection.create_index([('key', pymongo.ASCENDING)], unique=True)

    for i, row in data_frame.iterrows():
        data = row.to_dict()
        data['key'] = data['Market_and_Exchange_Names'] + ' ' + str(data['As_of_Date_In_Form_YYMMDD'])
        # print(data['key'])
        flt = {'key': data['key']}
        collection.update_one(flt, {'$set': data}, upsert=True)

    print(u'插入完毕，耗时：%s' % (time() - start))

    return data_frame


def read_cot_new_from_web():
    print("Read new COTS from web, please wait...")

    data_frame = pd.read_csv(
        # filepath_or_buffer='FinFutWk.txt',
        filepath_or_buffer=url_new,
        # sep=',',
        header=None,
        # skiprows=0,
        # nrows=10,
        # index_col=['DateTime'],
        # usecols=['Market_and_Exchange_Names', 'Report_Date_as_YYYY-MM-DD', 'CFTC_Contract_Market_Code'],
        # parse_dates=['Report_Date_as_YYYY-MM-DD'],
        # parse_dates=['DateTime'],
        # error_bad_lines=False,
        # na_values='NULL',
    )
    df_title = pd.read_csv(
        filepath_or_buffer='cot_var.txt',
        header=None,
        sep=' ',
    )
    data_frame.columns = df_title[1]

    print("Read OK")

    """数据插入到 Mongo 数据库中"""
    print("writing", len(data_frame.index), "records...")
    start = time()

    # 锁定集合，并创建索引
    client = pymongo.MongoClient('localhost', 27017)
    collection = client['fx']['cots']
    collection.create_index([('key', pymongo.ASCENDING)], unique=True)

    for i, row in data_frame.iterrows():
        data = row.to_dict()
        data['key'] = data['Market_and_Exchange_Names'] + ' ' + str(data['As_of_Date_In_Form_YYMMDD'])
        # print(data['key'])
        flt = {'key': data['key']}
        collection.update_one(flt, {'$set': data}, upsert=True)

    print(u'插入完毕，耗时：%s' % (time() - start))

    return data_frame


df = read_cot_his_from_db()

newest = df['Report_Date_as_YYYY-MM-DD'].max()
print('newest=', newest)
d = (datetime.now()-datetime.strptime(newest, '%Y-%m-%d')).days

if d > max_days:
    print('Too old. (', newest, ')')
    df = read_cot_his_from_web()

elif d > min_days:
    print('A bit old. (', newest, ')')
    df_new = read_cot_new_from_web()
    df = df.combine_first(df_new)

else:
    print('No old. (', newest, ')')

df2 = df[titles.keys()]
df3 = df2.rename(columns=titles)
df3['COT_NAME'] = df3['Market_and_Exchange_Names'].map(items)
df3['DATE'] = df3['Report_Date']
df3['LL-LS'] = df3['LL'] - df3['LS']
df3['LL+LS'] = df3['LL'] + df3['LS']
df3['NL-NS'] = df3['NL'] - df3['NS']
df3['NL+NS'] = df3['NL'] + df3['NS']
# df3['DATE'] = pd.to_datetime(df3['Report_Date'], format='%Y-%m-%d')
# df3['DATE'] = df3['DATE'] - timedelta(2)
df_fin = df3.dropna()
# print(df_fin)

df_cot = df_fin[df_fin['COT_NAME'] == target][eval(p1)]
# df_cot = df_fin.iloc[df_fin.index.get_level_values('COT_NAME') == 'EUR'][p[0]]
# df_cot.reset_index(['COT_NAME'], inplace=True)
# df_cot.drop('COT_NAME', axis=1, inplace=True)
# df_cot.index = pd.PeriodIndex(df_cot.index, freq='D')
df_cot_cut = df_cot[(df_cot['DATE'] >= sd) & (df_cot['DATE'] < ed)]
# print(df_cot_cut)
print(df_cot_cut['DATE'].max())
print(df_cot_cut['DATE'].min())

df = pd.read_csv(
    filepath_or_buffer=fn,
    header=None,
)
cols = ['D', 'T', 'Open', 'High', 'Low', 'Close', 'Volume']
df.columns = cols
df['DATE'] = df['D'].str.replace('.', '-')
df['COT_NAME'] = target
# df['DATE'] = pd.to_datetime(df['D'], format='%Y-%m-%d')
# df.set_index('DATE', inplace=True)
# df.index = pd.PeriodIndex(df.index, freq='D')
# df_eur_w1 = df.drop(['D', 'T', 'Volume'], axis=1)
# df_eur_w1_cut = df_eur_w1['2019':'2020']['Close']
df_d1 = df.drop(['D', 'T', 'Volume'], axis=1)
df_d1_cut = df_d1[(df_d1['DATE'] >= sd) & (df_d1['DATE'] < ed)]
print(df_d1_cut['DATE'].max())
print(df_d1_cut['DATE'].min())

# print(df_d1_cut)

df_merge = pd.merge(df_d1_cut, df_cot_cut, on=['COT_NAME', 'DATE'], how='outer')
df_merge.fillna(0, inplace=True)
# print(df_merge)

fig, axes = plt.subplots(3, 1, sharex=True, gridspec_kw={'height_ratios': [4, 1, 1]})
plt.subplots_adjust(wspace=0)

# axes[0].plot(df_eur_merge['DATE'].str[2:], df_eur_merge['Close'])
mpf.candlestick2_ochl(axes[0], df_merge['Open'], df_merge['Close'], df_merge['High'], df_merge['Low'],
                      width=0.5, colorup='g', colordown='r', alpha=0.6)

bar_width = 0.3
x1 = np.arange(len(df_merge['DATE']))
x2 = np.arange(len(df_merge['DATE']))
# x1 = np.arange(len(df_merge['DATE'])) - bar_width/2
# x2 = np.arange(len(df_merge['DATE'])) + bar_width/2
axes[1].bar(x1, df_merge[eval(p0)[0]], width=bar_width, color='g')
axes[2].bar(x2, df_merge[eval(p0)[1]], width=bar_width, color='b')
axes[2].set_xticks(range(0, len(df_merge['DATE'])))
axes[2].set_xticklabels(df_merge['DATE'].str[2:], rotation=90, fontsize='xx-small')
axes[0].set_title(target + ' ' + p0)

# axes[2].bar(df_eur_merge['DATE'].str[2:], df_eur_merge['LS'])
# df_eur_d1_cut.plot(ax=axes[0])
# df_eur_cot_cut.plot.bar(ax=axes[1])

# for tick in axes[1].get_xticklabels():
#     tick.set_rotation(90)
#     tick.set_fontsize('xx-small')

axes[0].grid(which='both')
axes[1].grid(which='both')
axes[2].grid(which='both')

fig.tight_layout()
plt.show()

'''
df_eur_merge = pd.merge(df_eur_w1_cut, df_eur_cot_cut, on='DATE', how='outer')
print(df_eur_merge)

apds = [mpf.make_addplot(df_eur_merge['LL'], panel='lower', color='g'),
        mpf.make_addplot(df_eur_merge['LS'], panel='lower', color='r')]
mpf.plot(df_eur_merge, type='candle', addplot=apds)
'''

'''
fig, axes = plt.subplots(2, 1, sharex=True)
# fig.subplots_adjust(bottom=0.2)
plt.subplots_adjust(wspace=0)

candlestick_ohlc(axes[0], zip(mdates.date2num(df_eur_w1_cut.index.to_pydatetime()),
                                 df_eur_w1_cut['Open'], df_eur_w1_cut['High'],
                                 df_eur_w1_cut['Low'], df_eur_w1_cut['Close']),
                 width=0.6)

df_eur_cot_cut.plot.bar(ax=axes[1])

# mondays = WeekdayLocator(MONDAY)        # major ticks on the mondays
# alldays = DayLocator()              # minor ticks on the days
# weekFormatter = DateFormatter('%b %d')  # e.g., Jan 12
# dayFormatter = DateFormatter('%d')      # e.g., 12

# axes[0].xaxis.set_major_locator(mondays)
# axes[0].xaxis.set_major_formatter(weekFormatter)
# axes[0].xaxis.set_minor_locator(alldays)
# axes[0].xaxis.set_minor_formatter(dayFormatter)

axes[1].set_title('EUR')
# axes[0].xaxis_date()
# axes[0].autoscale_view()
plt.setp(plt.gca().get_xticklabels(), rotation=90, horizontalalignment='right')

plt.show()
'''

'''
fig, axes = plt.subplots(2, 4, sharex=True, sharey=True)
plt.subplots_adjust(wspace=0)

for n in range(len(curs)):
    df_cur = df_fin.iloc[df_fin.index.get_level_values('COT_NAME') == curs[n]][p[0]]
    df_cur.reset_index(['COT_NAME'], inplace=True)
    df_cur.drop('COT_NAME', axis=1, inplace=True)
    df_cur.index = pd.PeriodIndex(df_cur.index, freq='D')
    # print(df_cur)
    axes[int(n / 4), n % 4].set_title(curs[n])
    # axes[int(n / 4), n % 4].format_xdata = mdates.DateFormatter('%Y-%m-%d')
    # axes[int(n / 4), n % 4].xaxis.set_minor_formatter(mdates.DateFormatter('%d'))
    df_cur.plot.bar(ax=axes[int(n / 4), n % 4])
    for tick in axes[int(n / 4), n % 4].get_xticklabels():
        tick.set_rotation(90)

plt.show()
'''
