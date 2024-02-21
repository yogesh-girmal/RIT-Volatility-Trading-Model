import signal
import asyncio
import aiohttp
import pandas as pd
from utils import *

API_KEY = {'X-API-Key': 'SLMQ8O0P'}
shutdown = False

async def main():
    try:
        async with aiohttp.ClientSession(headers=API_KEY) as session:
            pd.set_option('chained_assignment', None)
            tick = await get_tick(session)

            while tick == 0:
                # await asyncio.sleep(0.5)
                  
                tick = await get_tick(session)
                print("wating fot tick to start, current tick is ", tick)

            values = tick
            securities_count = 41  # Number of securities
            data = [[] for _ in range(securities_count)]
            tick_count = values
            period = await get_period(session)
            sub_heats = period #default
            real_ticks = []

            while not shutdown and sub_heats <= 2:
                tick = await get_tick(session)
                period = await get_period(session)
                if tick == tick_count:
                    securities_data, securities_names = await get_s(session)
                    # Append data for each security to the respective list
                    for i, price in enumerate(securities_data):
                        data[i].append(price)

                    print(f"Data captured for tick: {tick_count} and real tick is : {tick} for period {period}")
                    real_ticks.append(tick)
                    tick_count += 1

                    if tick == 300 and sub_heats == 1:
                        tick_count = 1
                        sub_heats +=1
                        print (f'sub_heats is {sub_heats}')
                    elif tick == 300 and sub_heats == 2:
                        sub_heats +=1
                        print (f'sub_heats is {sub_heats}')
                        
                    elif sub_heats == 3 and period == 2:
                        print('breaking out of loop')
                        break

            await asyncio.sleep(0.5)

            # Create DataFrame from collected data
            df = pd.DataFrame(data)

            news = await get_news(session)  # Fetch news data
            news_df = pd.DataFrame(news)
            news_df = news_df.set_index('news_id')

            df.index = securities_names 

            df.columns = [f"{tick}" for tick in real_ticks]
            print('tick data is' , df)
            print('news data is' , news_df)
            
            # Export DataFrame and news DataFrame to Excel
            with pd.ExcelWriter('news_ticks_securities_data.xlsx') as writer:
                df.to_excel(writer, sheet_name='Asset Prices')
                news_df.to_excel(writer, sheet_name='News')
            
            print(f"Data captured and saved to securities_data.xlsx")
    except Exception as e:
        print ("exception is", e)
        raise 


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    asyncio.run(main())