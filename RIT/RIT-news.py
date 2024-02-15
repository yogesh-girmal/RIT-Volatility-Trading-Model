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

            while not shutdown and tick == 0:
                await asyncio.sleep(0.5)
                  
                tick = await get_tick(session)
                print("wating fot tick to start, current tick is ", tick)

            tick_count = tick
            real_tick_count = tick_count
            securities_count = 41  # Number of securities
            data = [[] for _ in range(securities_count)]
            _total_tick_count = 1
            sub_heats = 1 #default

            # Wait until the tick becomes 1
            tick = await get_tick(session)
            period = await get_period(session)
            status= await get_status(session)

            while tick not in {tick_count} or period not in {sub_heats} and status not in {"ACTIVE"}:
                await asyncio.sleep(0.5)
                tick = await get_tick(session)
                period = await get_period(session)
                status= await get_status(session)
                print(f'waiting for tick to become {tick_count}, current tick value is {tick}, current period is {period}, current status is {status}')
        
            while not shutdown and sub_heats <= 2:
                tick = await get_tick(session)
                period = await get_period(session)
                if tick == tick_count:
                    securities_data, securities_names = await get_s(session)
                    # Append data for each security to the respective list
                    for i, price in enumerate(securities_data):
                        data[i].append(price)

                    print(f"Data captured for tick: {tick_count} and real tick is : {tick}")
                    print(f"total_tick_count is {_total_tick_count}")
                    tick_count += 1
                    _total_tick_count += 1
                    print(f"changing total_tick_count to {_total_tick_count}")

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
            
            df.index = securities_names 

            df.columns = [f"Tick_{i + 1}" for i in range(_total_tick_count)]
            print(df)

            # Export DataFrame to Excel
            df.to_excel("securities_data.xlsx")
            
            print(f"Data captured for from {real_tick_count} to {_total_tick_count} ticks and saved to securities_data.xlsx")
    except Exception as e:
        print ("exception is ", e)
        raise 


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    asyncio.run(main())
