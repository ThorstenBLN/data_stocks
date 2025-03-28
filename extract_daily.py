import yfinance as yf
import pandas as pd
import numpy as np
import time
import os
import openpyxl
import datetime as dt
import warnings
import functions as f
import re
warnings.simplefilter('ignore', 'FutureWarning')

PATH = "./data/"
FILE_SYMBOLS = "symbols.xlsx"
FILE_DATES = "dates.xlsx"
FILE_KGV = "kgv_5y.xlsx"
FILE_RESULT = "result.xlsx"
FILE_DATA = "data_all.xlsx"
FILE_RESULT_DAY  = "result_last_download.xlsx"

INDEX_SYMBOL = "^990100-USD-STRD" #"^GDAXI"
NA_PENALTY = -0.333
DAYS_THRES = 85

# 1. load base data ####################################################################
if not os.path.exists(PATH + FILE_RESULT): # for the first time there is no result file
    df_result_cur = pd.DataFrame()
else:
    df_result_cur = pd.read_excel(PATH + FILE_RESULT)
df_base_orig = pd.read_excel(PATH + FILE_SYMBOLS)
mask = (df_base_orig['data_all'] == 1) & (df_base_orig['symbol'].notna())
df_base = df_base_orig.loc[mask].copy()
df_dates = pd.read_excel(PATH+ FILE_DATES)

# 2. refresh financial dates ###########################################################
# 2.1 filter on symbols where last reporting date is older then x days (ca. 25 min / 1000 symbols)
if not df_result_cur.empty:
    mask_renew = (df_result_cur['days_passed'] >= DAYS_THRES) | (df_result_cur['days_passed'].isna())
    df_dates_renew = df_result_cur.loc[mask_renew][['symbol']]
    df_dates_renew = df_base.merge(df_dates_renew, on='symbol', how='inner')
    # 2.2 renew the dates for selected symbols
    df_dates_new = f.scrape_dates(df_dates_renew)
    df_dates_new['check'] = 1
    # 2.3 add the new dates to datesfiles and save file
    df_dates_update = df_dates.merge(df_dates_new[['symbol', 'check']], how='left')
    df_dates_update = pd.concat([df_dates_update.loc[df_dates_update['check'].isna()], df_dates_new])
    df_dates_update.drop(columns='check').to_excel(PATH + FILE_DATES, index=False)

# 3. load data for levermann formual
# 3.1 base data index and relevant dates #######################################################
dat_index = yf.Ticker(INDEX_SYMBOL)
df_index_hist = dat_index.history(period="2y").reset_index()
df_index_hist['Date'] = df_index_hist['Date'].dt.date
if df_index_hist.empty:
    print("error occured while loading dax")
    raise SystemExit("stopped due to dax download error")

DATE_CUR = df_index_hist['Date'].max() 
DATES = {'cur': DATE_CUR, 
         '3m': (DATE_CUR - pd.DateOffset(months=3)).date(), 
         '6m': (DATE_CUR - pd.DateOffset(months=6)).date(), 
         '12m': (DATE_CUR - pd.DateOffset(months=12)).date(), 
         '18m': (DATE_CUR - pd.DateOffset(months=18)).date()}
df_index_prices = f.get_hist_prices(df_index_hist, DATES)

# 3.2 get levermann data fram yfinance api (ca. 30 min / 1000 symbols)
# get the dates of the last Geschäftszahlen presentation
df_dates = pd.read_excel(PATH + FILE_DATES)
df_dates['time_delta'] = (df_dates['date'] - pd.to_datetime('today')).dt.days
df_dates['date'] = df_dates['date'].dt.date
df_dates_qrt = df_dates.loc[(df_dates['type'] == 'Quartalszahlen') & (df_dates['time_delta'] <= 0)].copy() 
df_dates_qrt_rel = df_dates_qrt.sort_values(['time_delta'], ascending=False).groupby(['symbol']).head(1).reset_index()
df_dates_jv = df_dates.loc[(df_dates['type'] == 'Hauptversammlung') & (df_dates['time_delta'] <= 0)].copy() 
df_dates_jv_rel = df_dates_jv.sort_values(['time_delta'], ascending=False).groupby(['symbol']).head(1).reset_index()
# download data
data = []
for row in df_base.iloc[:].itertuples():
    if row.Index % 100 == 0:
        print(row.Index, row.symbol)
    qrt_date = df_dates_qrt_rel.loc[df_dates_qrt_rel['symbol'] == row.symbol]['date']
    jv_date = df_dates_jv_rel.loc[df_dates_jv_rel['symbol'] == row.symbol]['date']
    data.append(f.get_levermann_data(row, df_index_hist, df_index_prices, DATES, qrt_date, jv_date))
    time.sleep(np.random.uniform(0.9, 1.4))
df_data = pd.DataFrame(data)
df_data['data_date'] = pd.to_datetime(df_data['data_date']).dt.date
df_data.to_excel(PATH + FILE_DATA, index=False)
print("code data levermann finished successfully")

# 4. calculate levermann score #############################################################
df_kgv = pd.read_excel(PATH + FILE_KGV)
# df_data = pd.read_excel(PATH + FILE_DATA)
df_data['forward_kgv'] = np.where(df_data['forward_kgv'] == "Infinity", np.inf, df_data['forward_kgv']).astype('float')
df_data_complete = df_data.merge(df_kgv, on='symbol', how='left')

df_result = f.add_levermann_score(df_data_complete, NA_PENALTY)
df_result_tot = pd.concat([df_result_cur, df_result], axis=0).reset_index(drop=True)
df_result.to_excel(PATH + FILE_RESULT_DAY, index=False)
df_result_tot.to_excel(PATH + FILE_RESULT, index=False)



