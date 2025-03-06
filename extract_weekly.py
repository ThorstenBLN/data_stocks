import yfinance as yf
import pandas as pd
import numpy as np
import time
import os
import openpyxl
import datetime as dt
import warnings
import functions as f
warnings.simplefilter('ignore', 'FutureWarning')


PATH = "./data/"
FILE_SYMBOLS = "symbols.xlsx"
FILE_DATES = "dates.xlsx"
FILE_KGV = "kgv_5y.xlsx"

# 1. load symbols
df_base = pd.read_excel(PATH + FILE_SYMBOLS)

# 2. finanzen.net: scrape data ############################################################
# 2.1 scrape termine scrapet the vergangenen Termine (ca. 20 min for 1000 symbols)
df_dates = f.scrape_dates(df_base.loc[df_base['data_all'] == 1].iloc[:])
df_dates.to_excel(PATH + FILE_DATES, index=False)

# 2.2 scrape old KGV (ca. 20 min for 1000 symbols)
kgv_real = []
REL_YEARS_REAL = ["2024", "2023", "2022", "2021"]
for row in df_base.loc[df_base['data_all'] == 1].iloc[:].itertuples():
    if row.Index % 100 == 0:
        print(row.Index, row.symbol)
    kgv_real = kgv_real + f.scrape_finanzen_kgv_real(row.symbol, row.kgv_old_url, REL_YEARS_REAL)
    time.sleep(np.random.uniform(0.3, 0.8))
df_kgv_real = pd.DataFrame(kgv_real)
df_kgv_real['kgv'] = np.where(df_kgv_real['kgv'] == '-', np.nan, df_kgv_real['kgv'])
df_kgv_real['kgv'] = df_kgv_real['kgv'].str.replace(".","").str.replace(",", ".").astype(float)
df_kgv_real_wide = df_kgv_real.loc[~df_kgv_real['kgv'].isna()].pivot(index='symbol', columns=['year'], values='kgv').reset_index()
# df_kgv_real_wide.to_excel(PATH + FILE_KGV_REAL, index=False)
print("code real_kgv finished successfully")

# 2.3 scrape estimated KGV (ca. 20 min for 1000 symbols)
kgv_est = []
REL_YEARS_EST = ["2024e", "2025e", "2026e", "2027e"]
for row in df_base.loc[df_base['data_all'] == 1].iloc[:].itertuples():
    if row.Index % 100 == 0:
        print(row.Index, row.symbol)
    kgv_est = kgv_est + f.scrape_finanzen_kgv_est(row.symbol, row.kgv_est_url, REL_YEARS_EST)
    time.sleep(np.random.uniform(0.3, 0.8))
df_kgv_est = pd.DataFrame(kgv_est)
df_kgv_est['kgv'] = np.where(df_kgv_est['kgv'] == "-", np.nan, df_kgv_est['kgv'])
df_kgv_est['kgv'] = df_kgv_est['kgv'].str.replace(".","").str.replace(",", ".").astype(float)
df_kgv_est_wide = df_kgv_est.loc[~df_kgv_est['kgv'].isna()].pivot(index='symbol', columns='year', values='kgv').reset_index()
# df_kgv_est_wide.to_excel(PATH + FILE_KGV_EST, index=False)
print("code est_kgv finished successfully")

# 2.4. add both data and calculate 5 years kgv
df_kgv = df_kgv_real_wide.merge(df_kgv_est_wide, on='symbol', how='outer')
prev_year = dt.datetime.now().year - 1
df_kgv['kgv_5y'] = np.where(df_kgv[str(prev_year)].notna(), df_kgv[[str(year) if year <= prev_year else str(year) + "e" for year in range(prev_year - 2, prev_year + 3)]].mean(axis=1, skipna=True), 
                            df_kgv[[str(year) if year < prev_year else str(year) + "e" for year in range(prev_year - 2, prev_year + 3)]].mean(axis=1, skipna=True))
df_kgv['kgv_5y'] = df_kgv['kgv_5y'].astype('float')
df_kgv['dwl_date_kgv'] = time.strftime("%Y%m%d")
df_kgv.to_excel(PATH + FILE_KGV, index=False)
    

