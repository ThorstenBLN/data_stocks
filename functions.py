import requests
import random
import time
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import yfinance as yf
import re
import janitor
import os

FMP_KEY = os.getenv("FMP_KEY")
if FMP_KEY is None:
    from credentials import FMP_KEY

user_agent_list = [
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0"
    ]


def scrape_yahoo_url(url):
    cookies = {"Cookie": "A1=d=AQABBEiFp2cCEFCzb_m_-DndGR1baoxeXu0FEgABCAHSqGfaZ-A9b2UBAiAAAAcIRYWnZztNAcA&S=AQAAArexugCmp9Gf7XSFWHqNQtg; GUC=AQABCAFnqNJn2kIc-QQs&s=AQAAANF8CSel&g=Z6eFUg; A3=d=AQABBEiFp2cCEFCzb_m_-DndGR1baoxeXu0FEgABCAHSqGfaZ-A9b2UBAiAAAAcIRYWnZztNAcA&S=AQAAArexugCmp9Gf7XSFWHqNQtg; cmp=t=1739809550&j=1&u=1---&v=67; PRF=t%3DDBK.DE%252BBAS.F%252BPNE3.DE%252BEOAN.DE%252BDR0.DE%252BNEM.DE%252BPSG.DE%252BVOW3.DE%252BMED.DE%252BPSG.F%252BNXU.DE%252BWAC.DE%252BSIE.DE%252BMUX.DE%252BBIKE.F; dflow=418; A1S=d=AQABBEiFp2cCEFCzb_m_-DndGR1baoxeXu0FEgABCAHSqGfaZ-A9b2UBAiAAAAcIRYWnZztNAcA&S=AQAAArexugCmp9Gf7XSFWHqNQtg; EuConsent=CQMgjEAQMgjEAAOACKDEBdFgAAAAAAAAACiQAAAAAAAA"}
    headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.5",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",    
    "User-Agent": random.choice(user_agent_list),
    # "Referer": referer
    }
    num_retries = 16
    for x in range(0, num_retries):
        try:
            page = requests.get(url, headers=headers, cookies=cookies)
            soup = BeautifulSoup(page.content, 'html.parser', from_encoding="utf-8")
            str_error = None

        except Exception as e:
            str_error = str(e)
            print("error", str_error, " trying again for the ", x+1, " time")
        if str_error:
                time.sleep(0.8 + random.random())
        else:
            break
    return soup


def scrape_finanzen_url(url):
    cookies = {"Cookie": "A1=d=AQABBEiFp2cCEFCzb_m_-DndGR1baoxeXu0FEgABCAHSqGfaZ-A9b2UBAiAAAAcIRYWnZztNAcA&S=AQAAArexugCmp9Gf7XSFWHqNQtg; GUC=AQABCAFnqNJn2kIc-QQs&s=AQAAANF8CSel&g=Z6eFUg; A3=d=AQABBEiFp2cCEFCzb_m_-DndGR1baoxeXu0FEgABCAHSqGfaZ-A9b2UBAiAAAAcIRYWnZztNAcA&S=AQAAArexugCmp9Gf7XSFWHqNQtg; cmp=t=1739809550&j=1&u=1---&v=67; PRF=t%3DDBK.DE%252BBAS.F%252BPNE3.DE%252BEOAN.DE%252BDR0.DE%252BNEM.DE%252BPSG.DE%252BVOW3.DE%252BMED.DE%252BPSG.F%252BNXU.DE%252BWAC.DE%252BSIE.DE%252BMUX.DE%252BBIKE.F; dflow=418; A1S=d=AQABBEiFp2cCEFCzb_m_-DndGR1baoxeXu0FEgABCAHSqGfaZ-A9b2UBAiAAAAcIRYWnZztNAcA&S=AQAAArexugCmp9Gf7XSFWHqNQtg; EuConsent=CQMgjEAQMgjEAAOACKDEBdFgAAAAAAAAACiQAAAAAAAA"}
    headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.5",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",    
    "User-Agent": random.choice(user_agent_list),
    # "Referer": referer
    }
    num_retries = 16
    for x in range(0, num_retries):
        try:
            page = requests.get(url, headers=headers)# , cookies=cookies)
            soup = BeautifulSoup(page.content, 'html.parser', from_encoding="utf-8")
            str_error = None

        except Exception as e:
            str_error = str(e)
            print("error", str_error, " trying again for the ", x+1, " time")
        if str_error:
                time.sleep(0.8 + random.random())
        else:
            break
    return soup


# functions
def get_hist_prices(data: pd.DataFrame, dates:dict):
    # get historic prices of the date (try except if there was no trade day)
    df_prices = pd.DataFrame()
    for per, date in dates.items():
        for i in range(4):
            check_date = (date - pd.DateOffset(days=i)).date()
            try:
                df_prices = pd.concat([df_prices, pd.DataFrame({'Date': check_date, 'Close':data.loc[data['Date'] == check_date]['Close'].values[0]}, index=[per])])            
                break
            except:
                # print(f"no data found at {check_date}")
                continue
            # price = df_hist.loc[df_hist['Date'] == date]['Close'].values[0]
    return df_prices

def get_fmp_symbols(exchange_names:list=None, sym_type=None):
    '''get list of dictionaries of all stocks of the chosen trade exchange with symbol and name info'''
    url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={FMP_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        symbols = response.json()
        if exchange_names is None:
            return pd.DataFrame([symbol for symbol in symbols if symbol['type'] == sym_type])
        return pd.DataFrame([symbol for symbol in symbols if symbol['exchangeShortName'] in exchange_names and symbol['type'] == sym_type])
    else:
        print(response.content)
    return pd.DataFrame()
    

def add_isin(df_symb:pd.DataFrame):
    '''add isin to the list of symbols and returns a dataframe with company info'''
    df_symb['isin'] = np.nan
    for i, row in df_symb.iterrows():
        print(i)
        url = f"https://financialmodelingprep.com/api/v3/profile/{row['symbol']}?apikey={FMP_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            response = response.json()
            try: 
                df_symb.at[i]['isin'] = response[0]['isin']
            except:
                continue
        else:
            print(response.content)
        return df_symb
    return None

def yf_data_available(symbol):
    '''checks data availability of a certain symbol in yfinance'''
    if yf.Ticker(symbol).history(period="1d").empty:
        return 0
    return 1

def get_levermann_data(row, df_dax_hist, df_dax_prices, dates, qrt_date, jv_date):
    '''get all data needed to calculate the levermann formula'''
    FINANCE = ['Capital Markets','Credit Services','Financial Conglomerates', 'Financial Data & Stock Exchanges', 'Banks - Diversified', 'Banks - Regional', 'Mortgage Finance']
    MAX_QRT_DAY_DISTANCE = 200
    FIELDS = ['data_date', 'industry', 'finance', 'cap_size', 'market_cap', 'eigenkapital_rendite', 'ebit_marge', 'ek_quote', 'forward_kgv','reaktion_qrt', 'gewinnrevision', 
              'up_6m', 'up_12m', 'kursmomentum', 'up_vs_dax_3m', 'up_vs_dax_6m', 'cur_gewinnwachstum', 'strongBuy', 'buy', 'hold', 'sell', 'strongSell']
    # predefine results dict
    result_temp = {'symbol':row.symbol, 'name':row.name, 'download_date':time.strftime("%Y%m%d")}
    # add dates and timediff and check which data is relevant for reaktion auf geschäftszahlen
    result_temp['rel_financials_date'] = None
    if jv_date.empty:
        result_temp['jv_date'] = np.nan
    else:
        result_temp['jv_date'] = jv_date.values[0]
        result_temp['rel_financials_date'] = result_temp['jv_date']
    if qrt_date.empty:
        result_temp['qrt_date'] = np.nan
    else:
        result_temp['qrt_date'] = qrt_date.values[0]
        if not result_temp['rel_financials_date']:
            result_temp['rel_financials_date'] = result_temp['qrt_date']
        else:
            result_temp['rel_financials_date'] = max(result_temp['qrt_date'], result_temp['jv_date'])
    if  result_temp['rel_financials_date']:
        result_temp['days_passed'] = (pd.to_datetime("today") - pd.to_datetime(result_temp['rel_financials_date'])).days
    # setup rest of dict
    for key in FIELDS:
        result_temp[key] = np.nan
    # get stock data
    dat = yf.Ticker(row.symbol)
    # try to get the historic data for 2 years. if not available take max timeframe
    try:
        df_hist = dat.history(period="2y").reset_index()
        df_hist['Date'] = df_hist['Date'].dt.date
        df_prices = get_hist_prices(df_hist, dates)
        result_temp['data_date'] = df_hist['Date'].max()
    except:
        try:
            df_hist = dat.history(period=dat.get_history_metadata()['validRanges'][-1]).reset_index()
            df_hist['Date'] = df_hist['Date'].dt.date
            df_prices = get_hist_prices(df_hist, dates)
            result_temp['data_date'] = df_hist['Date'].max()
        except:
            df_hist = pd.DataFrame()
            df_prices = pd.DataFrame()
            result_temp['data_date'] = np.nan
    df_bs = dat.balance_sheet
    df_is = dat.income_stmt
    df_eps = dat.eps_trend
    # error handling
    if any([df.empty or df is None for df in [df_hist, df_prices, df_bs, df_is, df_eps]]): 
        print("no valid data -> skipping")
        return result_temp
    try:
        result_temp['industry'] = dat.info['industry']
        result_temp['market_cap'] = int(dat.info['marketCap'])
    except:
        result_temp['industry'] = np.nan
        result_temp['market_cap'] = 0
    result_temp['cap_size'] = np.where(result_temp['market_cap'] < 2000000000, "small", np.where(result_temp['market_cap'] < 5000000000, "mid", "big"))
    result_temp['finance'] = 0
    if result_temp['industry'] in FINANCE:
        result_temp['finance'] = 1
    # 2. Eigenkapitalrendite
    try:
        result_temp['eigenkapital_rendite'] = df_is.loc['Net Income'].iloc[0] / df_bs.loc['Total Equity Gross Minority Interest'].iloc[0]
    except:
        result_temp['eigenkapital_rendite'] = np.nan
    # 3. EBIT-Marge
    try:
        result_temp['ebit_marge'] = df_is.at['EBIT', df_is.columns[0]] / df_is.at['Total Revenue', df_is.columns[0]]
    except:
        result_temp['ebit_marge'] = np.nan
    # 4. EK-Quote
    try:
        result_temp['ek_quote'] = df_bs.at['Total Equity Gross Minority Interest', df_bs.columns[0]] / df_bs.at['Total Assets', df_bs.columns[0]]
    except:
        result_temp['ek_quote'] = np.nan
   
    # 6. forward P/E (KGV)
    try:
        result_temp['forward_kgv'] = dat.info['forwardPE'] # nicht 100% sicher
        if result_temp['forward_kgv'] == "Infinity":
            result_temp['forward_kgv'] = np.inf
    except:
        result_temp['forward_kgv'] = np.nan
    # 7. Analystenmeinung
    try:
        df_analyst = dat.recommendations
        if not df_analyst.empty:
          for key in df_analyst.columns[1:]:
            result_temp[key] =  df_analyst.at[0, key]
    except:
        pass
    # 8. Reaktion auf Geschäftszahlen 
    # check which date to use (JV oder QRT)
    if not result_temp['rel_financials_date']:
        print("no_qrt_date")
        result_temp['reaktion_qrt'] = np.nan
        result_temp['rel_financials_date'] = np.nan
    # calculate the time difference in order to chose day before and after or pass if wrt_Date too much in the past
    else:
        df_hist['date_diff'] = (pd.to_datetime(df_hist['Date']) - pd.to_datetime(result_temp['rel_financials_date'])).dt.days
        df_dax_hist['date_diff'] = (pd.to_datetime(df_dax_hist['Date']) - pd.to_datetime(result_temp['rel_financials_date'])).dt.days
        # check if there is a the window of +/- 1 day in the data (1. qrt date too old, 2. all data newer than wrt date, 3. qrt date newer than any data 
        if df_hist['date_diff'].max() > MAX_QRT_DAY_DISTANCE or df_hist['date_diff'].min() > 0 or df_hist['date_diff'].max() < 0:
            result_temp['reaktion_qrt'] = np.nan
            print("no valid qrt_date")
        else:
            # calculate the values for dax
            min_later_dax = df_dax_hist.loc[df_dax_hist['date_diff'] > 0]['date_diff'].min()
            if df_dax_hist['date_diff'].max() == 0: # on the day of the qrt release, take this day
                min_later_dax = df_dax_hist.loc[df_dax_hist['date_diff'] == 0]['date_diff'].min()
            price_dax_next = df_dax_hist.loc[df_dax_hist['date_diff'] == min_later_dax]['Close'].values[0]
            min_before_dax = df_dax_hist.loc[df_dax_hist['date_diff'] < 0]['date_diff'].max()
            price_dax_before = df_dax_hist.loc[df_dax_hist['date_diff'] == min_before_dax]['Close'].values[0]
            # caluclate the value for our stock
            min_later = df_hist.loc[df_hist['date_diff'] > 0]['date_diff'].min()
            if df_hist['date_diff'].max() == 0: # on the day of the qrt release, take this day
                min_later = df_hist.loc[df_hist['date_diff'] == 0]['date_diff'].min()
            price_next = df_hist.loc[df_hist['date_diff'] == min_later]['Close'].values[0]
            min_before = df_hist.loc[df_hist['date_diff'] < 0]['date_diff'].max()
            price_before = df_hist.loc[df_hist['date_diff'] == min_before]['Close'].values[0]
            result_temp['reaktion_qrt']  = (price_next / price_before) / (price_dax_next / price_dax_before) - 1
    # 9 Gewinnrevision (erwartung des EPS heute vs. vor 30 Tagen)
    try:
        result_temp['gewinnrevision'] = df_eps.at['0y', 'current'] / df_eps.at['0y', '7daysAgo'] - 1
    except:
        result_temp['gewinnrevision'] = np.nan 

    # 10. Kurs heute vs. 6 M
    try:
        result_temp['up_6m'] = (df_prices.at['cur', 'Close'] / df_prices.at['6m', 'Close']) - 1
    except:
        result_temp['up_6m'] = np.nan

    # 11. Kurs heute vs. 12 M
    try:
        result_temp['up_12m'] = (df_prices.at['cur', 'Close'] / df_prices.at['12m', 'Close']) - 1
    except:
        result_temp['up_12m'] = np.nan
    
    # 12. Kursmomentum
    try:
        result_temp['kursmomentum'] = (df_prices.at['cur', 'Close'] / df_prices.at['6m', 'Close']) / (df_prices.at['cur', 'Close'] / df_prices.at['12m', 'Close']) - 1
    except:
        result_temp['kursmomentum'] = np.nan

    # 13 Entwicklung letzte 3 Monate ggü. Index (DAX)  6Mx / 6Mp – 1
    try:
        result_temp['up_vs_dax_3m'] = (df_prices.at['cur', 'Close'] / df_prices.at['3m', 'Close']) / (df_dax_prices.at['cur', 'Close'] / df_dax_prices.at['3m', 'Close']) - 1
    except:
        result_temp['up_vs_dax_3m'] = np.nan
    try:
        result_temp['up_vs_dax_6m'] = (df_prices.at['cur', 'Close'] / df_prices.at['6m', 'Close']) / (df_dax_prices.at['cur', 'Close'] / df_dax_prices.at['6m', 'Close']) - 1
    except:
        result_temp['up_vs_dax_6m'] = np.nan

    # 14. gewinn-wachstum    
    try:
        result_temp['cur_gewinnwachstum'] = df_eps.at['+1y', 'current'] / df_eps.at['0y', 'current']  - 1 
    except:
        result_temp['cur_gewinnwachstum'] = np.nan
    # create and return datafrane
    return result_temp

def add_levermann_score(df_data, na_penalty):
    '''add all levemannscores to the dataframe'''
    df_valid = df_data.clean_names(strip_underscores=True).copy()
    analyst_start = list(df_valid.columns).index('strongbuy')
    df_valid['n_rec'] = df_valid.iloc[:, analyst_start:analyst_start + 5].sum(axis=1)
    df_valid['analyst_metric'] = (df_valid['strongbuy'] + df_valid['buy'] + 2 * df_valid['hold'] + 3 * (df_valid['sell'] + df_valid['strongsell'])) / df_valid['n_rec']
    # score_start = len(df_valid.columns)
    df_valid['lev_ekr'] = np.where(df_valid['eigenkapital_rendite'].isna(), na_penalty, np.where(df_valid['eigenkapital_rendite'] > 0.2, 1, np.where(df_valid['eigenkapital_rendite'] >= 0.1, 0, -1)))
    df_valid['lev_ebitm'] = np.where(df_valid['ebit_marge'].isna(), na_penalty, np.where(df_valid['ebit_marge'] > 0.12, 1, np.where(df_valid['ebit_marge'] >= 0.06, 0, -1)))
    df_valid['lev_ekq'] = np.where(df_valid['ek_quote'].isna(), na_penalty, np.where(df_valid['ek_quote'] > 0.25, 1, np.where(df_valid['ek_quote'] >= 0.15, 0, -1)))
    mask_fin = df_valid['finance'] == 1
    df_valid['lev_ekq'] = np.where(df_valid['ek_quote'].isna(), na_penalty, np.where(mask_fin & (df_valid['ek_quote'] > 0.10), 1, np.where(mask_fin & (df_valid['ek_quote'] >= 0.05), 0, np.where(mask_fin & (df_valid['ek_quote'] < 0.05), -1, df_valid['lev_ekq']))))
    df_valid['lev_kgv5y'] = np.where(df_valid['kgv_5y'].isna(), na_penalty, np.where(df_valid['kgv_5y'] < 0, -1, np.where(df_valid['kgv_5y'] < 12, 1, np.where(df_valid['kgv_5y'] <= 16, 0, -1))))
    df_valid['lev_fkgv'] = np.where(df_valid['forward_kgv'].isna(), na_penalty, np.where(df_valid['forward_kgv'] < 0, -1, np.where(df_valid['forward_kgv'] < 12, 1, np.where(df_valid['forward_kgv'] <= 16, 0, -1))))
    df_valid['lev_anam'] = np.where(df_valid['analyst_metric'].isna(), na_penalty, np.where(df_valid['analyst_metric'] > 2.5, 1, np.where(df_valid['analyst_metric'] >= 1.5, 0, -1)))
    mask_sc  = (df_valid['cap_size'] == "small") & (df_valid['n_rec'] <= 5)
    df_valid['lev_anam'] = np.where(df_valid['analyst_metric'].isna(), na_penalty, np.where((df_valid['analyst_metric'] < 1.5) & mask_sc, 1, np.where((df_valid['analyst_metric'] <= 2.5)  & mask_sc, 0, np.where((df_valid['analyst_metric'] > 2.5) & mask_sc, -1, df_valid['lev_anam']))))
    df_valid['lev_rqrt'] = np.where(df_valid['reaktion_qrt'].isna(), na_penalty, np.where(df_valid['reaktion_qrt'] > 0.01, 1, np.where(df_valid['reaktion_qrt'] >= -0.01, 0, -1)))
    df_valid['lev_gewr'] = np.where(df_valid['gewinnrevision'].isna(), na_penalty, np.where(df_valid['gewinnrevision'] > 0.05, 1, np.where(df_valid['gewinnrevision'] >= -0.05, 0, -1)))
    df_valid['lev_kurs6m'] = np.where(df_valid['up_6m'].isna(), na_penalty, np.where(df_valid['up_6m'] > 0.05, 1, np.where(df_valid['up_6m'] >= -0.05, 0, -1)))
    df_valid['lev_kurs12m'] = np.where(df_valid['up_12m'].isna(), na_penalty, np.where(df_valid['up_12m'] > 0.05, 1, np.where(df_valid['up_12m'] >= -0.05, 0, -1)))
    df_valid['lev_kmom'] = np.where(df_valid['kursmomentum'].isna(), na_penalty, np.where(df_valid['kursmomentum'] > 0.02, 1, np.where(df_valid['kursmomentum'] >= -0.02, 0, -1)))
    df_valid['lev_dmr'] = np.where(df_valid['up_vs_dax_3m'].isna(), na_penalty, np.where(df_valid['up_vs_dax_3m'] > 0.02, 1, np.where(df_valid['up_vs_dax_3m'] >= -0.02, 0, -1)))
    df_valid['lev_gw'] = np.where(df_valid['cur_gewinnwachstum'].isna(), na_penalty, np.where(df_valid['cur_gewinnwachstum'] > 0.05, 1, np.where(df_valid['up_vs_dax_3m'] >= -0.05, 0, -1)))
    score_start = list(df_valid.columns).index('lev_ekr')
    df_valid['lev_score'] = df_valid.iloc[:, score_start:].sum(axis=1)
    return df_valid

def get_url_finanzen(symbol, name):
    '''scrapes the finanzen page to get the url for termine and the name used by finanzen for the given symbol'''
    BASE_URL = f"https://www.finanzen.net/"
    df_dates = pd.DataFrame()
    # find the website by symbol
    url = f"https://www.finanzen.net/suchergebnis.asp?_search={symbol}"
    soup = scrape_finanzen_url(url)
    # assure that the Aktien tab has been chosen
    try:
        stock_menu_url = BASE_URL + soup.find("a", 'tab__item', string=lambda text: text and "Aktien" in text)['href']    
    except:
        print("error 1: no stock menu")
        return {'symbol': symbol, 'name': name, 'symbol_finanzen':np.nan, 'name_finanzen':np.nan, 'stock_url':np.nan, 'termine_url':np.nan}
    soup_2 = scrape_finanzen_url(stock_menu_url)
    try:
        # verify if the name of the company appears similarily in finanzen.net
        check_name = re.sub('[!-,\'&.\\/]', ' ', name.lower()).split()
        if check_name[0] == "the" and len(check_name) > 1:
            check_name = check_name[1]
        else: 
            check_name = check_name[0]
        stock_url = BASE_URL + soup_2.find(attrs={'class':"horizontal-scrolling"}).find('a', href=True, string=lambda text: text and check_name in text.lower())['href']
    except:
        print("error 2: no name matching")
        return {'symbol': symbol, 'name': name, 'symbol_finanzen':np.nan, 'name_finanzen':np.nan, 'stock_url':np.nan, 'termine_url':np.nan}
    # get the termine website from menu
    soup_3 = scrape_finanzen_url(stock_url)
    try:
        termine_url = BASE_URL + soup_3.find("a", "details-navigation__item-label", string="Termine", href=True)['href']
    except:
        print("error 3: no termine url")
        return {'symbol': symbol, 'name': name, 'symbol_finanzen':np.nan, 'name_finanzen':np.nan, 'stock_url':stock_url, 'termine_url':np.nan}
    name_finanzen = termine_url.replace("https://www.finanzen.net//termine/", "")    
    # get the symbol of finanzen
    try:
        symbol_finanzen = soup_3.find("em", "badge__key", string="Symbol").find_next_sibling("span").text 
    except:
        return {'symbol': symbol, 'name': name, 'symbol_finanzen':np.nan, 'name_finanzen':name_finanzen, 'stock_url':stock_url, 'termine_url':termine_url}
    return {'symbol': symbol, 'name': name, 'symbol_finanzen':symbol_finanzen, 'name_finanzen':name_finanzen, 'stock_url':stock_url, 'termine_url':termine_url}

def scrape_finanzen_termine(symbol, termine_url):
    '''scrapes the termine table of finanzen'''
    soup = scrape_finanzen_url(termine_url)
    try:
        dates_table = soup.find_all("tbody")[2] #, "page-content__item page-content__item--space"
    except:
        print("error 1: no tabel")
        return [{'symbol': symbol, 'termine_url':termine_url, 'type':np.nan, 'info':np.nan, 'date':np.nan}]
    if "keine" in dates_table.text.lower().strip():
        print("error 2: no dates")
        return [{'symbol': symbol, 'termine_url':termine_url, 'type':np.nan, 'info':np.nan, 'date':np.nan}]
    else:
        count = 0
        dates = []
        # if there is table scrape the first 5 dates
        for tr in dates_table.find_all('tr')[0:5]:
            count += 1
            date = tr.find_all('td')
            dates = dates + [{'symbol': symbol, 'termine_url':termine_url, 'type':date[0].text.strip(), 'info':date[2].text.strip(), 'date':date[3].text.strip()}]
        return dates
    
def scrape_finanzen_kgv_real(symbol, kgv_old_url, rel_years):
    '''finds table of Unternehmenskennzahlen and scrapes th unverwässerte kgv for rel_years'''
    kgv_real = []
    soup_kgv = scrape_finanzen_url(kgv_old_url)
    try:
        table = soup_kgv.find("h2", string=lambda text: text and "unternehmenskennzahlen" in text.lower()).parent
        years = table.find_all("th")
        cur_kgv = table.find("label", "checkbox__label", string=lambda text: text and "kgv" in text.lower() and "unver" in text.lower()).parent.parent#, string=lambda text: text and "kgv" in text.lower())
    except:
        print("error 1: no Unternehmenskennzahlen Tabelle or KGV KPI")
        return [{'symbol':symbol, 'year':np.nan, 'kgv':np.nan}]
    for year in years:
        if year.text in rel_years:
            kgv_real.append({'symbol':symbol, 'year':year.text, 'kgv':cur_kgv.text})
        cur_kgv = cur_kgv.next_sibling
    return kgv_real

def scrape_finanzen_kgv_est(symbol, kgv_est_url, rel_years):
    '''finds table of Unternehmenskennzahlen and scrapes the estimate kgv for rel_years'''
    kgv_est = []
    soup_kgv = scrape_finanzen_url(kgv_est_url)
    try:
        table = soup_kgv.find("h1", string=lambda text: text and "schätzungen* zu" in text.lower()).parent
        years = table.find_all("th")
        cur_kgv = table.find("td", "table__td", string=lambda text: text and "kgv" in text.lower())
    except:
        print("error 1: no Unternehmenskennzahlen Tabelle or KGV KPI")
        return [{'symbol':symbol, 'year':rel_years[0], 'kgv':np.nan}]
        # get how many years to scrape (2023 and 2024)
    for year in years:
        if year.text in rel_years:
            kgv_est.append({'symbol':symbol, 'year':year.text, 'kgv':cur_kgv.text})
        cur_kgv = cur_kgv.next_sibling
    return kgv_est

def scrape_dates(df):
    '''scrapes all the past dates from finanzen.net for a certain symbol and termine_url given in a dataframe'''
    dates = []
    for row in df.itertuples():
        if row.Index % 100 == 0:
            print(row.Index, row.symbol)
        dates += scrape_finanzen_termine(row.symbol, row.termine_url)
        time.sleep(np.random.uniform(0.3, 0.8))
    df_dates = pd.DataFrame(dates)
    df_dates = pd.concat([df_dates.drop(columns=['date'].copy()), df_dates['date'].str.split(n=2, expand=True)], axis= 1)
    if 1 in df_dates.columns:
        df_dates.rename(columns={0:'date', 1:'estimate'}, inplace=True)
    else:
        df_dates.rename(columns={0:'date'}, inplace=True)
        df_dates['estimate'] = np.nan
    df_dates['date'] = pd.to_datetime(df_dates['date'], format="%d.%m.%Y")
    df_dates['estimate'] = np.where(df_dates['estimate'].isna(), 0, 1)
    print("code termine finished successfully")
    return df_dates