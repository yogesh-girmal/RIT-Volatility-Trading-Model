import signal
import requests
from time import sleep
import pandas as pd
import numpy as np

        
def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True

async def get_tick(session):
    async with session.get('http://localhost:9999/v1/case') as resp:
        if resp.status == 200:
            case = await resp.json()
            return case['tick']
        
async def get_period(session):
    async with session.get('http://localhost:9999/v1/case') as resp:
        if resp.status == 200:
            case = await resp.json()
            return case['period']

async def get_status(session):
    async with session.get('http://localhost:9999/v1/case') as resp:
        if resp.status == 200:
            case = await resp.json()
            return case['status']

# code that gets the securities via json
async def get_s(session):
    async with session.get('http://localhost:9999/v1/securities') as price_act:
        if price_act.status == 200:
            prices = await price_act.json()
            return [price['last'] for price in prices], [price['ticker'] for price in prices]

async def get_news(session):
    async with session.get('http://localhost:9999/v1/news') as news_book:
        if news_book.status == 200:
            news_data = await news_book.json()
            news_tick = news_data['tick']
            news_ticker = news_data['ticker']


async def get_news(session):
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