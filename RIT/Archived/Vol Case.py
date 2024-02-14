"""
Volatility Support File 3
Rotman BMO Finance Research and Trading Lab, Uniersity of Toronto (C)
"""

import signal
import requests
from time import sleep
import pandas as pd
import numpy as np


# class that passes error message, ends the program
class ApiException(Exception):
    pass


# code that lets us shut down if CTRL C is pressed
def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True


API_KEY = {'X-API-Key': '7C6O3FKD'}
shutdown = False
session = requests.Session()
session.headers.update(API_KEY)


# code that gets the current tick
def get_tick(session):
    resp = session.get('http://localhost:9999/v1/case')
    if resp.ok:
        case = resp.json()
        return case['tick'] + (case['period'] - 1) * 300
    raise ApiException('fail - cant get tick')


# code that gets the securities via json
def get_s(session):
    price_act = session.get('http://localhost:9999/v1/securities')
    if price_act.ok:
        prices = price_act.json()
        return prices
    raise ApiException('fail - cant get securities')

def get_news(session):
    news_book = session.get('http://localhost:9999/v1/news')
    if news_book.ok:
        news = news_book.json()
        news_update = pd.DataFrame(news)
        news_update['Continuous_tick'] = (news_update['period']-1)*300+news_update['tick']
        news_update['Risk Free Rate (%)'] = np.nan
        news_update['Annualized Volatility (%)'] = np.nan
        news_update['Trading Days'] = np.nan
        news_update['Delta Limit'] = np.nan
        news_update['Penalty Percentage (%)'] = np.nan
        news_update['Drift (%)'] = np.nan

        # Define regex patterns for each metric
        patterns = {
            'Risk Free Rate (%)': r'risk free rate is (\d+)%',
            'Annualized Volatility (%)': r'annualized volatility is (\d+)%|expected to be between (\d+)% ~ (\d+)%|latest annualized volatility of RTM is (\d+)%',
            'Trading Days': r'consists of (\d+) trading days',
            'Delta Limit': r'delta limit for this heat is ([\d,]+)',
            'Penalty Percentage (%)': r'penalty percentage is (\d+)%',
            'Drift (%)': r'annual drift to be between (\d+)% ~ (\d+)%|annual drift is (\d+)%'
        }

        for col, pattern in patterns.items():
            # Extract data based on pattern
            extracted_data = news_update['body'].str.extract(pattern)

            # Handle multiple capture groups for cases like ranges or multiple matches
            if extracted_data.shape[1] > 1:
                # Combine extracted data into a single column, handling NaN values
                extracted_data = extracted_data.apply(lambda x: ' ~ '.join(x.dropna()), axis=1)

            # Assign extracted data to the DataFrame
            news_update[col] = extracted_data

        columns_to_standardize = ['Risk Free Rate (%)', 'Trading Days', 'Delta Limit', 'Penalty Percentage (%)']

        # Loop through each column and fill future rows with the first non-null value
        for col in columns_to_standardize:
            # Find the first non-null value
            first_non_null_value = news_update[col].dropna().iloc[0] if not news_update[col].dropna().empty else np.nan
            # Fill NaN values in the column with the first non-null value
            news_update[col] = news_update[col].fillna(first_non_null_value)

        new_columns = [
            'Realized Annualized Volatility(%)', 'Realized Drift(%)',
            'Minimum Implied Annual Volatility', 'Maximum Implied Annual Volatility',
            'Minimum Implied Drift', 'Maximum Implied Drift'
        ]
        news_update = news_update.reindex(columns=news_update.columns.tolist() + new_columns, fill_value='')

        # Separate single values and ranges
        for index, row in news_update.iterrows():
            for col in ['Annualized Volatility (%)', 'Drift (%)']:
                if '~' in row[col]:
                    # Split the range and assign to respective columns
                    min_val, max_val = row[col].split(' ~ ')
                    if col == 'Annualized Volatility (%)':
                        news_update.at[index, 'Minimum Implied Annual Volatility'] = min_val
                        news_update.at[index, 'Maximum Implied Annual Volatility'] = max_val
                    else:  # Drift (%)
                        news_update.at[index, 'Minimum Implied Drift'] = min_val
                        news_update.at[index, 'Maximum Implied Drift'] = max_val
                else:
                    # Assign single values to 'Realized' columns
                    if col == 'Annualized Volatility (%)' and row[col] != '':
                        news_update.at[index, 'Realized Annualized Volatility(%)'] = row[col]
                    elif col == 'Drift (%)' and row[col] != '':
                        news_update.at[index, 'Realized Drift(%)'] = row[col]

        columns_to_nos = ['Risk Free Rate (%)', 'Trading Days', 'Penalty Percentage (%)', 'Realized Annualized Volatility(%)', 'Realized Drift(%)', 'Minimum Implied Annual Volatility', 'Maximum Implied Annual Volatility', 'Minimum Implied Drift', 'Maximum Implied Drift']
        news_update[columns_to_nos] = news_update[columns_to_nos].apply(pd.to_numeric, errors='coerce')
        news_update['Delta Limit'] = news_update['Delta Limit'].str.replace(',', '').astype(int)

        #news_update = news_update.astype({'Risk Free Rate (%)':'float', 'Trading Days':'float', 'Delta Limit':'float', 'Penalty Percentage (%)':'float', 'Realized Annualized Volatility(%)':'float', 'Realized Drift(%)':'float', 'Minimum Implied Annual Volatility':'float', 'Maximum Implied Annual Volatility':'float', 'Minimum Implied Drift':'float', 'Maximum Implied Drift':'float'})

        news_update.to_excel("news_update_3.xlsx")
        return news_update
    raise ApiException('fail - cant get securities')


def main():
    with requests.Session() as session:
        session.headers.update(API_KEY)
        pd.set_option('chained_assignment', None)
        while get_tick(session) < 600 and not shutdown:
            years_remaining = (600 - get_tick(session)) / 3600
            news = get_news(session)
            assets = pd.DataFrame(get_s(session))
            assets2 = assets.drop(
                columns=['vwap', 'nlv', 'bid_size', 'ask_size', 'volume', 'realized', 'unrealized', 'currency',
                         'total_volume', 'limits', 'is_tradeable', 'is_shortable', 'interest_rate', 'start_period',
                         'stop_period', 'unit_multiplier',
                         'description', 'unit_multiplier', 'display_unit', 'min_price', 'max_price', 'start_price',
                         'quoted_decimals', 'trading_fee', 'limit_order_rebate',
                         'min_trade_size', 'max_trade_size', 'required_tickers', 'underlying_tickers', 'bond_coupon',
                         'interest_payments_per_period', 'base_security', 'fixing_ticker',
                         'api_orders_per_second', 'execution_delay_ms', 'interest_rate_ticker', 'otc_price_range'])
            for row in assets2.index.values:
                if 'P' in assets2['ticker'].iloc[row]:
                    assets2['type'].iloc[row] = 'PUT'
                elif 'C' in assets2['ticker'].iloc[row]:
                    assets2['type'].iloc[row] = 'CALL'

            strike_values = [None] + [value for value in range(45, 55) for _ in range(2)] * 2  # Repeat the pattern twice

            # Adding the 'Strike' column to the DataFrame
            assets2['Strike'] = strike_values


            # Check if tick_value matches any cell in the 'Continuous_tick' column
            assets2.to_excel("assets.xlsx")
            #print(news_update_2)

            print(assets2.to_markdown(), end='\n' * 2)
            sleep(1)


if __name__ == '__main__':
    main()