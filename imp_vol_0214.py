from scipy.stats import norm
from scipy.optimize import brentq
import numpy as np

#comments for next steps

#1 sigma in the below is the annualized volatility given at the start of the simulation. scrap that and plug in here. 
#2 S and K is the asset price and K is the strike price for respective option assets
#3 plug those values (S, K, T, r =0, sigma (from #3)) from the real time prices, so we will have implied volatility for each option asset at each time step 
#4 calculate those implied volatility for similar dataframe and paste in the new sheet 
#5 so we will have asset prices in sheet1, news in sheet2, and impl volatility in sheet3

def black_scholes_call(S, K, T, r, sigma):
    """
    Price a European call option using the Black-Scholes formula.
    
    Parameters:
    S -- Current price of the underlying asset
    K -- Strike price of the option
    T -- Time to expiration in years
    r -- Risk-free interest rate
    sigma -- Volatility of the underlying asset
    """
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)

def implied_volatility(option_market_price, S, K, T, r):
    """
    Calculate the implied volatility of a European option using the Black-Scholes model.
    
    Parameters:
    option_market_price -- Current market price of the option
    S -- Current price of the underlying asset
    K -- Strike price of the option
    T -- Time to expiration in years
    r -- Risk-free interest rate
    """
    def objective_function(sigma):
        return black_scholes_call(S, K, T, r, sigma) - option_market_price
    
    sigma_guess = 0.2
    sigma_bounds = (1e-5, 1)
    
    implied_vol = brentq(objective_function, sigma_bounds[0], sigma_bounds[1], xtol=1e-20)
    
    return implied_vol

def calculate_time_to_expiration(current_tick, total_ticks):
    """
    Calculate the time to expiration dynamically based on the current tick.
    
    Parameters:
    current_tick -- The current tick in the simulation
    total_ticks -- The total number of ticks for the option's expiration period
    """
    remaining_ticks = total_ticks - current_tick
    # Assuming 3600 ticks represent 12 months (1 year), calculate the years remaining
    return remaining_ticks / 3600

## sample code for review 

# Simulation parameters
S = 100  # Current price of the underlying asset
K = 100  # Strike price
r = 0    # Risk-free interest rate

# Option market prices (example)
option_market_price_1_month = 10
option_market_price_2_months = 15

# Current tick in the simulation
current_tick = 25

# Calculate time to expiration dynamically
T_1_month = calculate_time_to_expiration(current_tick, 300)  # 1-month expiry
T_2_months = calculate_time_to_expiration(current_tick, 600)  # 2-month expiry

# Calculate implied volatility for both options
iv_1_month = implied_volatility(option_market_price_1_month, S, K, T_1_month, r)
iv_2_months = implied_volatility(option_market_price_2_months, S, K, T_2_months, r)

print(f"Implied Volatility for 1-month expiry: {iv_1_month * 100:.2f}%")
print(f"Implied Volatility for 2-month expiry: {iv_2_months * 100:.2f}%")
