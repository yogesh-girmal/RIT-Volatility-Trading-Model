"""
Volatility Support File 3
Rotman BMO Finance Research and Trading Lab, Uniersity of Toronto (C)
All rights reserved.
"""
import warnings
import signal
import requests
from time import sleep
import pandas as pd
import numpy as np
#black scholes libraries
from py_vollib.black_scholes import black_scholes as bs
from py_vollib.black_scholes.greeks.analytical import delta
import py_vollib.black.implied_volatility as iv
#graphs
import plotext as plt

# Define variables 
# risk free rate r
# Stock price s
# strike price k
# time remaining (in years)
#ESTIMATE YOUR VOLATILITY:
vol = 0.18

#class that passes error message, ends the program
class ApiException(Exception):
    pass

#code that lets us shut down if CTRL C is pressed
def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True
    
API_KEY = {'X-API-Key': 'rotman'}
shutdown = False
session = requests.Session()
session.headers.update(API_KEY)
    
#code that gets the current tick
def get_tick(session):
    resp = session.get('http://localhost:9999/v1/case')
    if resp.ok:
        case = resp.json()
        return case['tick'] + (case['period'] - 1) * 300
    raise ApiException('fail - cant get tick')

#code that gets the securities via json  
def get_s(session):
    price_act = session.get('http://localhost:9999/v1/securities')
    if price_act.ok:
        prices = price_act.json()
        return prices
    raise ApiException('fail - cant get securities')

def years_r(mat, tick):
    yr = (mat - tick)/3600 
    return yr
    
def main():
    with requests.Session() as session:
        session.headers.update(API_KEY)
        while get_tick(session) < 600 and not shutdown:
            assets = pd.DataFrame(get_s(session))
            assets2 = assets.drop(columns=['vwap', 'nlv', 'bid_size', 'ask_size', 'volume', 'realized', 'unrealized', 'currency', 
                                           'total_volume', 'limits', 'is_tradeable', 'is_shortable', 'interest_rate', 'start_period', 'stop_period', 'unit_multiplier', 
                                           'description', 'unit_multiplier', 'display_unit', 'min_price', 'max_price', 'start_price', 'quoted_decimals', 'trading_fee', 'limit_order_rebate',
                                           'min_trade_size', 'max_trade_size', 'required_tickers', 'underlying_tickers', 'bond_coupon', 'interest_payments_per_period', 'base_security', 'fixing_ticker',
                                           'api_orders_per_second', 'execution_delay_ms', 'interest_rate_ticker', 'otc_price_range'])
            helper = pd.DataFrame(index = range(1),columns = ['share_exposure', 'required_hedge', 'must_be_traded', 'current_pos', 'required_pos', 'SAME?'])
            assets2['delta'] = np.nan
            assets2['i_vol'] = np.nan
            assets2['bsprice'] = np.nan
            assets2['diffcom'] = np.nan
            assets2['abs_val'] = np.nan
            assets2['decision'] = np.nan
            for row in assets2.index.values:
                if 'P' in assets2['ticker'].iloc[row]:
                    assets2['type'].iloc[row] = 'PUT'
                    if '1' in assets2['ticker'].iloc[row][3] and get_tick(session) < 300:
                        assets2['delta'].iloc[row] = delta('p', assets2['last'].iloc[0], float(assets2['ticker'].iloc[row][-2:]), 
                                                           years_r(300, get_tick(session)), 0, vol)
                        assets2['bsprice'].iloc[row] = bs('p', assets2['last'].iloc[0], float(assets2['ticker'].iloc[row][-2:]), 
                                                           years_r(300, get_tick(session)), 0, vol)
                        #assets2['i_vol'].iloc[row] = iv.implied_volatility(assets2['last'].iloc[row], assets2['last'].iloc[0],
                        #                                                   float(assets2['ticker'].iloc[row][-2:]), 0, years_r(300, get_tick(session)),
                        #                                                   'p')
                    elif '2' in assets2['ticker'].iloc[row][3]:
                        assets2['delta'].iloc[row] = delta('p', assets2['last'].iloc[0], float(assets2['ticker'].iloc[row][-2:]), 
                                                           years_r(600, get_tick(session)), 0, vol)
                        assets2['bsprice'].iloc[row] = bs('p', assets2['last'].iloc[0], float(assets2['ticker'].iloc[row][-2:]), 
                                                           years_r(600, get_tick(session)), 0, vol)
                        #assets2['i_vol'].iloc[row] = iv.implied_volatility(assets2['last'].iloc[row], assets2['last'].iloc[0],
                        #                                                   float(assets2['ticker'].iloc[row][-2:]), 0, years_r(600, get_tick(session)),
                        #                                                   'p')
                elif 'C' in assets2['ticker'].iloc[row]:
                    assets2['type'].iloc[row] = 'CALL'
                    if '1' in assets2['ticker'].iloc[row][3] and get_tick(session) < 300:
                        assets2['delta'].iloc[row] = delta('c', assets2['last'].iloc[0], float(assets2['ticker'].iloc[row][-2:]), 
                                                           years_r(300, get_tick(session)), 0, vol)
                        assets2['bsprice'].iloc[row] = bs('c', assets2['last'].iloc[0], float(assets2['ticker'].iloc[row][-2:]), 
                                                           years_r(300, get_tick(session)), 0, vol)
                        #assets2['i_vol'].iloc[row] = iv.implied_volatility(assets2['last'].iloc[row], assets2['last'].iloc[0],
                        #                                                   float(assets2['ticker'].iloc[row][-2:]), 0, years_r(300, get_tick(session)),
                        #                                                   'c')
                    elif '2' in assets2['ticker'].iloc[row][3]:
                        assets2['delta'].iloc[row] = delta('c', assets2['last'].iloc[0], float(assets2['ticker'].iloc[row][-2:]), 
                                                           years_r(600, get_tick(session)), 0, vol)
                        assets2['bsprice'].iloc[row] = bs('c', assets2['last'].iloc[0], float(assets2['ticker'].iloc[row][-2:]), 
                                                           years_r(600, get_tick(session)), 0, vol)
                        #assets2['i_vol'].iloc[row] = iv.implied_volatility(assets2['last'].iloc[row], assets2['last'].iloc[0],
                        #                                                   float(assets2['ticker'].iloc[row][-2:]), 0, years_r(600, get_tick(session)),
                        #                                                   'c')
                if assets2['last'].iloc[row] - assets2['bsprice'].iloc[row] > 0:
                    assets2['diffcom'].iloc[row] = assets2['last'].iloc[row] - assets2['bsprice'].iloc[row] - 0.02
                    assets2['abs_val'].iloc[row] = abs(assets2['diffcom'].iloc[row])
                elif assets2['last'].iloc[row] - assets2['bsprice'].iloc[row] < 0:
                    assets2['diffcom'].iloc[row] = assets2['last'].iloc[row] - assets2['bsprice'].iloc[row] + 0.02
                    assets2['abs_val'].iloc[row] = abs(assets2['diffcom'].iloc[row])
                if assets2['diffcom'].iloc[row] > 0.02:
                    assets2['decision'].iloc[row] = 'SELL'
                elif assets2['diffcom'].iloc[row] < -0.02:
                    assets2['decision'].iloc[row] = 'BUY'
                else:
                    assets2['decision'].iloc[row] = 'NO DECISION'
                warnings.filterwarnings('ignore')
                
            a1 = np.array(assets2['position'].iloc[1:])
            a2 = np.array(assets2['size'].iloc[1:])
            a3 = np.array(assets2['delta'].iloc[1:])
            
            helper['share_exposure'] = np.nansum(a1 * a2 * a3)
            helper['required_hedge'] = helper['share_exposure'].iloc[0] * -1
            helper['must_be_traded'] = helper['required_hedge']/assets2['position'].iloc[0] - assets2['position'].iloc[0]
            if assets2['position'].iloc[0] > 0:
                helper['current_pos'] = 'LONG'
            elif assets2['position'].iloc[0] < 0:
                helper['current_pos'] = 'SHORT'
            else:
                helper['current_pos'] = 'NO POSITION'
            if helper['required_hedge'].iloc[0] > 0:
                helper['required_pos'] = 'LONG'
            elif helper['required_hedge'].iloc[0] < 0:
                helper['required_pos'] = 'SHORT'
            else:
                helper['required_pos'] = 'NO POSITION'
            helper['SAME?'] = (helper['required_pos'] == helper['current_pos'])
            print(assets2.to_markdown(), end='\n'*2)
            print(helper.to_markdown(), end='\n'*2)
            #y = assets2['last']
            #plt.plot(y)
            #plt.plotsize(50, 30)
            sleep(0.5)
if __name__ == '__main__':
        main()