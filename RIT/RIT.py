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


# code that gets the securities via json
async def get_s(session):
    async with session.get('http://localhost:9999/v1/securities') as price_act:
        if price_act.status == 200:
            prices = await price_act.json()
            return [price['last'] for price in prices], [price['ticker'] for price in prices]


async def main():
    async with aiohttp.ClientSession(headers=API_KEY) as session:
        pd.set_option('chained_assignment', None)

        securities_count = 41  # Number of securities
        data = [[] for _ in range(securities_count)]  # Initialize empty list for each security
        tick_count = 1
        delta = 1
 
        # Wait until the tick becomes 1
        tick = await get_tick(session)
        while tick not in {1}:
            await asyncio.sleep(0.2)
            tick = await get_tick(session)
            print(f'waiting for tick to become 1, current tick value is {tick}')

        while not shutdown and delta <= 2:
            tick = await get_tick(session)

            if tick is not None:
                if tick == tick_count:
                    securities_data, securities_names = await get_s(session)
                    # Append data for each security to the respective list
                    for i, price in enumerate(securities_data):
                        data[i].append(price)

                    print(f"Data captured for tick: {tick_count}")
                    tick_count += 1
                    
                    print(f"real tick is {tick}")

                    if tick == 300 and delta == 1:
                        tick_count = 1
                        delta +=1
                # else:
                #     print('passing for next tick')
                #     pass

            # await asyncio.sleep(0.3)
        
        # Create DataFrame from collected data
        df = pd.DataFrame(data)
        
        df.index = securities_names 

        df.columns = [f"Tick_{i + 1}" for i in range(tick_count)]
        
        # Export DataFrame to Excel
        df.to_excel("securities_data.xlsx")
        
        print(f"Data captured for {tick_count} ticks and saved to securities_data.xlsx")


def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    asyncio.run(main())
