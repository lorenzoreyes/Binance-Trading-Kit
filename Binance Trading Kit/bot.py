from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
import pandas as pd, yfinance as yahoo, os
import numpy as np, datetime as dt
import matplotlib.pyplot as plt
from pylab import mpl
from controlpanel import *
from api import *
mpl.rcParams['font.family'] = 'serif'
plt.style.use('fivethirtyeight')
pd.options.display.width = 0
pd.options.display.float_format = '{:,.4f}'.format
pd.options.display.max_rows


client = Client(API_KEY,API_SECRET)

# alarm sound for each buy/sell due signal break
duration, freq = 1, 440

def repo():
    direction = market()[0]
    for i in range(len(direction)):
        client.order_market_buy(
        symbol=str(direction.symbol.values[i]),
        quoteOrderQty=10.0
        )

def are_you_invested():
  account = pd.DataFrame(client.get_account()['balances'])
  account[['free','locked']] = account[['free','locked']].astype(float)
  holding = account[account['free']!=0.00000000]
  if len(holding) > 2:
    print("Portfolio is active, now lets monitor its performance")
  else:
    print("Starting the engines. Get ready for crypto")
    repo()

are_you_invested()

holding = account()[0]

# Set stop as a global variable   
stop = holding[['symbol','firstPrice']]
stop = stop.T
stop.columns = stop.iloc[0,:].to_list()
stop = stop.drop('symbol')
stop *= 0.9

# get records orders from currents positions to track PnL
#client.get_all_orders(symbol='BURGERUSDT', limit=10)
#pd.DataFrame(dict.values(),index=dict.keys()).T

# check market direction
def trending():
    df = yahoo.download('BTC-USD',period='30d',interval='2m')['Adj Close'].fillna(method='ffill')
    df = pd.DataFrame(df.values,columns=['Bitcoin'],index=df.index)
    df['SMA'] = df['Bitcoin'].rolling(round(len(df)*.1),min_periods=1).mean()
    trend = 'Short' if df['Bitcoin'].tail(1).values < df['SMA'].tail(1).values else 'Long'
    return trend

trend = trending()


symbols = sorted(holding.symbol.to_list())
pairs = sorted(holding.symbol.to_list())

# Gather info of pairs to have history to track
for i in range(len(symbols)):
    klines = client.get_historical_klines(symbols[i],\
                                      Client.KLINE_INTERVAL_1MINUTE,\
                                          "5 days ago UTC")
    close = [float(k[4]) for k in klines]
    timestamp = [int(k[0]) for k in klines]
    for j in range(len(timestamp)):
      timestamp[j] = dt.datetime.fromtimestamp(timestamp[j]/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
    
    symbols[i] = pd.DataFrame(close,columns=[f'{symbols[i]}'],index=timestamp)
    
data = symbols[0].tail(7000)
for i in range(1,len(symbols)):
  data[f'{symbols[i].columns[0]}'] = symbols[i].tail(7000).values

twm = ThreadedWebsocketManager(API_KEY, API_SECRET)
twm.start()

# dict coin and its quantity
nominal = holding[~(holding.symbol.str.contains('USDT')) & ~(holding.symbol.str.contains('BNB'))]
nominal = dict(zip(holding.symbol.to_list(), holding.nominalNow.to_list()))
lifespan = data.columns.to_list()

# main function with stop loss incorporated
def handle_socket_message(response):
        global data
        discarded = []
        lifespan = data.columns.to_list()
        spot = response['data']['s']
        close = float(response['data']['c'])      
        timestamp = dt.datetime.fromtimestamp(int(response['data']['E'])/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
        msg = pd.DataFrame(close,columns=[spot],index=[timestamp])
        data = pd.concat([data,msg]).sort_index().fillna(method='ffill')
        sma = data.rolling(round(len(data)*0.25)).min()*1.01
        tail = data.tail(1)-sma.tail(1).values
        alarm = tail[tail<0].dropna(axis=1) #if trend == 'Short' else tail[tail<0].dropna(axis=1)
        alarm = alarm.columns.to_list()
        #to sell dict(zip(alarm.asset, alarm.free))
        print(tail)
        if len(alarm) > 0:
            for i in alarm:
                if i in lifespan:
                    lifespan.remove(i)
                    #i = i.replace('USDT','')
                    ammount = nominal[i]
                    price = data[i].tail(1).values[0]  
                    val = int(price * ammount)
                    try: 
                        client.order_market_sell(
                            symbol=i,
                            quoteOrderQty=val
                            )
                        os.system('play -nq -t alsa synth {} sine {}'.format(duration, freq))
                        print(f"\n\n\n\tSelling {i} at {price} total obtained {val}\n\n\n")
                    except:
                        client.order_market_sell(
                            symbol=i,
                            quoteOrderQty=val-1
                            )
                        os.system('play -nq -t alsa synth {} sine {}'.format(duration, freq))
                        print(f"\n\n\n\tSelling {i} at {price} total obtained {val}\n\n\n")
                else:
                    pass
                               
            if lifespan == 0:
                print('\n\n\n\tGAME OVER\n\n\n')
                twm.stop()
            


streams = [i.lower() + '@miniTicker' for i in pairs]
twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)
twm.join()
