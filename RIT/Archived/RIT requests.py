import signal
import requests
from time import sleep


# This class definition allows us to print error messages and stop the program when needed
class ApiException(Exception):
    pass

# This signal handler allows for a graceful shutdown when CTRL+C is pressed
def signal_handler(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True

API_KEY = {'X-API-Key': 'SLMQ8O0P'}
shutdown = False

# This helper method returns the current "tick" of the running case
def get_tick(session):
    resp = session.get("http://localhost:9999/v1/case")
    if resp.ok:
        case = resp.json()
        return case["tick"]
    raise ApiException("Authorization error. Please check API key.")

# This helper method returns the bid and ask for a given security
def ticker_bid_ask(session, ticker):
    payload = {'ticker': ticker}
    resp = session.get("http://localhost:9999/v1/securities/book", params=payload)
    if resp.ok:
        book = resp.json()
        print(book)
        return book['bids'][0]['price'], book['asks'][0]['price']
    raise ApiException("Authorization error. Please check API key.")

def main():
    with requests.Session() as s:
        s.headers.update(API_KEY)
        tick = get_tick(s)
        while tick >= 5 and tick < 295 and not shutdown:
            RTM_bid, RTM_ask = ticker_bid_ask(s, "RTM")
            print('RTM_bid:',RTM_bid)
            sleep(1)
            print('RTM_ask:',RTM_ask)
            sleep(1)
            # if RTM_bid > RTM_ask:
            #     s.post('http://localhost:9999/v1/orders', params={"ticker": "CRZY_A", "type": "MARKET","quantity": 1000,"action": "BUY"})

            # IMPORTANT to update the tick at the end of the loop to check that the algorithm should still run or not
            tick = get_tick(s)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main()

