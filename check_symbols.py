import pandas as pd
import numpy as np
import time
import openpyxl
import datetime as dt
import warnings
import functions as f
import os
warnings.simplefilter('ignore', 'FutureWarning')

PATH = "./data/"

if not os.path.exists(PATH):
    os.makedirs(PATH)

# 1. download current symbols german and Nasdaq symbols #############################################
df_symbols_all = f.get_fmp_symbols(exchange_names=['NASDAQ', 'NYSE', 'XETRA'], sym_type='stock')
df_symbols_all['symbol'] = df_symbols_all['symbol'].astype(str)

# get only the original symbols
df_symbols_all['org'] = df_symbols_all['symbol'].apply(lambda x: "." not in x)
df_symbols = df_symbols_all.loc[df_symbols_all['org']].drop_duplicates('name').copy().reset_index()

# 2. check if data in yfinance (ca. 9 min for 1000 symb) ###########################################
# data_yf = [f.yf_data_available(row.symbol) for row in df_symbols.iloc[:].itertuples()]
data_yf = []
for row in df_symbols.iloc[:].itertuples():
    if row.Index % 100 == 0:
        print((row.Index, row.symbol))
    data_yf.append(f.yf_data_available(row.symbol))
    time.sleep(np.random.uniform(0.2, 0.5))
print("all data collected")
df_symbols['data_yf'] = data_yf
df_symbols.to_excel(PATH + "symbols.xlsx", index=False)

# 3. check valid symbols at finanzen.net (ca. 1h for each 1000 sybols)
# 3.1 get all relevnt links 
# df_symbols = pd.read_excel(PATH + "symbols.xlsx")
BASE_URL = f"https://www.finanzen.net/"
fin_links = []
for row in df_symbols.loc[df_symbols['data_yf']].iloc[:500].itertuples():
    if row.Index % 100 == 0:
        print(row.Index, row.symbol)
    fin_links.append(f.get_url_finanzen(row.symbol, row.name))
    time.sleep(np.random.uniform(0.3, 0.8))
df_fin_links = pd.DataFrame(fin_links)
# add links and check if symbols are identical 
df_fin_links['kgv_old_url'] = BASE_URL + "bilanz_guv/" + df_fin_links['name_finanzen']
df_fin_links['kgv_est_url'] = BASE_URL + "schaetzungen/" + df_fin_links['name_finanzen']
df_fin_links_final = df_fin_links.loc[df_fin_links['symbol'] == df_fin_links['symbol_finanzen']]
# df_fin_links_final.to_excel("./data/finanzen_links.xlsx", index=False)

# 4. merge all data and save final list
df_symbols_final = df_symbols.merge(df_fin_links_final[['symbol', 'name_finanzen', 'stock_url', 'termine_url', 'kgv_old_url', 'kgv_est_url']], on='symbol', how='left')
mask = (df_symbols_final['data_yf']) & (~df_symbols_final['name_finanzen'].isna())
df_symbols_final['data_all'] = np.where(mask, 1, 0)
df_symbols_final.to_excel(PATH + "symbols_fin.xlsx", index=False)