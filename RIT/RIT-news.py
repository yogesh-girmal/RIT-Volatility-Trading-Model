import signal
import asyncio
import aiohttp
import pandas as pd

API_KEY = {'X-API-Key': 'SLMQ8O0P'}
shutdown = False

# code that gets the current tick
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


async def main():
    try:
        async with aiohttp.ClientSession(headers=API_KEY) as session:
            pd.set_option('chained_assignment', None)
            values = 200
            securities_count = 41  # Number of securities
            data = [[] for _ in range(securities_count)]
            _total_tick_count = 0
            tick_count = values
            sub_heats = 1 #default

            # Wait until the tick becomes 1
            tick = await get_tick(session)
            period = await get_period(session)
            status= await get_status(session)

            while tick not in {values} or period not in {sub_heats} and status not in {"ACTIVE"}:
                await asyncio.sleep(0.5)
                tick = await get_tick(session)
                period = await get_period(session)
                status= await get_status(session)
                print(f'waiting for tick to become {values}, current tick value is {tick}, current period is {period}, current status is {status}')
        
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
            
            print(f"Data captured for total {_total_tick_count} ticks and saved to securities_data.xlsx")
    except Exception as e:
        print ("exception is ", e)
        raise 

def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    asyncio.run(main())
