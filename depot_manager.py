import openpyxl
import pandas as pd
import numpy as np
import yfinance as yf
import warnings
import os
import time

warnings.simplefilter('ignore', 'FutureWarning')

PATH = "./data/"
FILE_RESULT_DAY  = "result_last_download.xlsx"
FILE_DEPOT = "depot.xlsx"
FILE_TRANSACTIONS = "transactions.xlsx"

L_SCORE_BUY = 9
MIN_L_SCORE = 7
STOP_LOSS_PC = 0.9
THRES_LEV_BETTER = 3
INVEST_VALUE = 2000
MIN_INVEST_VALUE = 1000


# 0. load relevant files
df_result = pd.read_excel(PATH + FILE_RESULT_DAY)
if not os.path.exists(PATH + FILE_DEPOT):
    df_depot = pd.DataFrame({"symbol":'bank', 'name':'account', 'buy_date':'2025-03-16','price_buy':1.00, 'amount':1, 'cur_date':'2025-03-17', 'price_cur':1.00, 'value':10000.00, 'stop_loss':0.00, 'return':0.00, 'lev_score': 100.00}, index=[0])
else:
    df_depot = pd.read_excel(PATH + FILE_DEPOT)
if not os.path.exists(PATH + FILE_TRANSACTIONS):
    df_transact = pd.DataFrame()
else:
    df_transact = pd.read_excel(PATH + FILE_TRANSACTIONS)

# 1. update the current values of the stocks
df_depot.reset_index(drop=True)
mask_bank = df_depot['symbol'] == 'bank'
for row in df_depot.loc[~mask_bank].itertuples():
    try:
        cur_price = yf.Ticker(row.symbol).info['regularMarketPrice']
        # cur_return = cur_price / row.price_buy - 1
        df_depot.at[row.Index, "price_cur"] = cur_price
        df_depot.at[row.Index, "cur_date"] = time.strftime("%Y-%m-%d")
        df_depot.at[row.Index, "value"] = cur_price * row.amount
        df_depot.at[row.Index, "return"] = cur_price / row.price_buy - 1
    except Exception as err:
        print("0", row.symbol, err)
df_depot = df_depot.drop(columns='lev_score').merge(df_result[['symbol', 'lev_score']], on='symbol', how='left')
df_depot.at[0, 'lev_score'] = 100

# 2. buy/sell stocks 
# 2.1 sell based on fixed values
mask_1 = df_depot['lev_score'] <= MIN_L_SCORE
mask_2 = df_depot['price_cur'] <= df_depot['stop_loss']
df_sales = df_depot.loc[mask_1 | mask_2].copy()
if not df_sales.empty:
    # add sales value to bank account
    df_depot.at[0, "value"] = df_depot.at[0, "value"] + df_sales['value'].sum()
    # add values to transitions
    cols = list(df_sales.columns)
    df_sales["type"] = "sell"
    df_transact = pd.concat([df_transact, df_sales[['type'] + cols]])
    # delete stocks from depot
    df_depot = df_depot.loc[~df_depot['symbol'].isin(df_sales['symbol'].unique())].reset_index(drop=True)

# 2.2 buy with bank money
# check alternatives
mask_cur = df_result['symbol'].isin(df_depot['symbol'].unique())
mask_buy_bank = df_result['lev_score'] >= L_SCORE_BUY
df_buy_opt = df_result.loc[mask_buy_bank & ~mask_cur].sort_values(['lev_score', 'market_cap'], ascending=[False, False])
# buy the best stocks from bank
for row in df_buy_opt.itertuples():
    mask_bank = df_depot['symbol'] == 'bank'
    # buy all for invest value and last one for min value
    if df_depot.loc[mask_bank]['value'].values[0] >= INVEST_VALUE:
        VALUE = INVEST_VALUE
    elif df_depot.loc[mask_bank]['value'].values[0] >= MIN_INVEST_VALUE:
        VALUE = MIN_INVEST_VALUE
    else:
        break
    try:
        # perform purchase and add to all files 
        cur_price = yf.Ticker(row.symbol).info['regularMarketPrice']
        amount = VALUE // cur_price
        df_temp = pd.DataFrame({"type":"buy", "symbol":row.symbol, 'name': row.name,'buy_date':time.strftime("%Y-%m-%d"), 'price_buy':cur_price, 'amount':amount, 'cur_date':time.strftime("%Y-%m-%d"), 'price_cur':cur_price, 'value':cur_price * amount, 'stop_loss':cur_price * STOP_LOSS_PC, 'return':0, 'lev_score': row.lev_score}, index=[0]) 
        df_transact = pd.concat([df_transact, df_temp]).reset_index(drop=True)
        df_depot = pd.concat([df_depot, df_temp.drop(columns=['type'])]).reset_index(drop=True)
        # reduce bank account value by purchase volume
        df_depot.at[0, 'value'] = df_depot.at[0, "value"] - cur_price * amount
    except Exception as err:
        print("1", row.symbol, err)
    
# 2.3. shift stocks to better options
# get possible stocks
mask_cur = df_result['symbol'].isin(df_depot['symbol'].unique())
mask_shift_stocks = df_result['lev_score'] >= df_depot['lev_score'].min() + THRES_LEV_BETTER
df_buy_opt = df_result.loc[mask_shift_stocks & ~mask_cur].sort_values(['lev_score', 'market_cap'], ascending=[False, False]).reset_index(drop=True)
# prepare depot dataframe
mask_bank = df_depot['symbol'] == 'bank'
df_sales = df_depot.loc[~mask_bank].sort_values('lev_score').copy().reset_index(drop=True)
symbols_sold = []
for row in df_sales.itertuples():
    if df_buy_opt.shape[0]  < row.Index + 1:
        break
    if row.lev_score + THRES_LEV_BETTER < df_buy_opt.at[row.Index, 'lev_score']: 
        try: 
            # perform purchase and add to all files 
            cur_price = yf.Ticker(df_buy_opt.at[row.Index, 'symbol']).info['regularMarketPrice']
            # add sale value to bank
            df_depot.at[0, "value"] = df_depot.at[0, "value"] + df_sales.iloc[row.Index]['value']
            # add sale values to transitions
            cols = list(df_sales.columns)
            df_temp = df_sales.iloc[row.Index].copy()
            df_temp["type"] = "sell"
            df_transact = pd.concat([df_transact, df_temp[['type'] + cols]])
            symbols_sold.append(row.symbol)
            # buy other article
            if df_depot.loc[mask_bank]['value'].values[0] >= INVEST_VALUE:
                VALUE = INVEST_VALUE
            elif df_depot.loc[mask_bank]['value'].values[0] >= MIN_INVEST_VALUE:
                VALUE = MIN_INVEST_VALUE
            amount = VALUE // cur_price
            df_temp = pd.DataFrame({"type":"buy", "symbol":df_buy_opt.at[row.Index, 'symbol'], 'name': df_buy_opt.at[row.Index, 'name'],'buy_date':time.strftime("%Y-%m-%d"), 'price_buy':cur_price, 'amount':amount, 'cur_date':time.strftime("%Y-%m-%d"), 'price_cur':cur_price, 'value':cur_price * amount, 'stop_loss':cur_price * STOP_LOSS_PC, 'return':0, 'lev_score': df_buy_opt.at[row.Index, 'lev_score']}, index=[0]) 
            df_transact = pd.concat([df_transact, df_temp]).reset_index(drop=True)
            df_depot = pd.concat([df_depot, df_temp.drop(columns=['type'])]).reset_index(drop=True)
            # reduce bank account value by purchase volume
            df_depot.at[0, 'value'] = df_depot.at[0, "value"] - cur_price * amount    
        except Exception as err:
            print("2", row.symbol, err)
# delete stocks from depot
df_depot = df_depot.loc[~df_depot['symbol'].isin(symbols_sold)].reset_index(drop=True)

# 3. set new stop loss
df_depot['stop_loss'] = np.where(df_depot['price_cur'] * STOP_LOSS_PC > df_depot['stop_loss'], df_depot['price_cur'] * STOP_LOSS_PC, df_depot['stop_loss']) 

# 4. save Files
df_depot.to_excel(PATH + FILE_DEPOT, index=False)
df_transact.to_excel(PATH + FILE_TRANSACTIONS, index=False)