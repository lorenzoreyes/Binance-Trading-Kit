'''

Landscape functions to
read holdings, market status, trending data
in order to trigger or not
functions to enable trend-following logic

'''
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
import pandas as pd, yfinance as yahoo
import datetime as dt
from api import *

client = Client(API_KEY,API_SECRET)

def repo():
    direction = market()[0]
    for i in range(len(direction)):
        client.order_market_buy(
        symbol=str(direction.symbol.values[i]),
        quoteOrderQty=50.0
        )


def trending():
    df = yahoo.download('BTC-USD',period='30d',interval='2m')['Adj Close'].fillna(method='ffill')
    df = pd.DataFrame(df.values,columns=['Bitcoin'],index=df.index)
    df['SMA'] = df['Bitcoin'].rolling(round(len(df)*.1),min_periods=1).mean()
    trend = 'Short' if df['Bitcoin'].tail(1).values < df['SMA'].tail(1).values else 'Long'
    return trend


def research():
    x = pd.DataFrame(client.get_ticker())
    y =  x[x.symbol.str.contains('USDT')]
    z = y[~(y.symbol.str.contains('BULL')) & ~(y.symbol.str.contains('BEAR'))]
    z = z[~(z.symbol.str.contains('UP')) & ~(z.symbol.str.contains('DOWN'))]
    z = z[z.symbol.apply(lambda x: ('USDT' in x[-4:]))]
    z = z[z.lastPrice.astype(float)!=0]
    final = z[['symbol','lastPrice']]
    final = final.sort_values('symbol',ascending=True)
    final['lastPrice'] = final['lastPrice'].astype(float)
    symbols = final.symbol.to_list()
    for i in range(len(symbols)):
        klines = client.get_historical_klines(symbols[i],\
                                      Client.KLINE_INTERVAL_1DAY,\
                                          "30 days ago UTC")
        close = [float(k[4]) for k in klines]
        timestamp = [int(k[0]) for k in klines]
        for j in range(len(timestamp)):
            timestamp[j] = dt.datetime.fromtimestamp(timestamp[j]/1000).strftime('%Y-%m-%d %H:%M:%S.%f')

        symbols[i] = pd.DataFrame(close,columns=[f'{symbols[i]}'],index=timestamp)

    data = symbols[0].tail(30)
    for i in range(1,len(symbols)):
        data[f'{symbols[i].columns[0]}'] = symbols[i].fillna(method='ffill')

    final['MonthAgo'] = data.head(1).T.values
    final['Trend'] = ((final['lastPrice'] / final['MonthAgo']) - 1.0) * 100.0
    final = final.sort_values('Trend',ascending=True)
    final = final.dropna()
    return final


def market():
    df = yahoo.download('BTC-USD',period='30d',interval='2m')['Adj Close'].fillna(method='ffill')
    df = pd.DataFrame(df.values,columns=['Bitcoin'],index=df.index)
    df['SMA'] = df['Bitcoin'].rolling(round(len(df)*.1),min_periods=1).mean()
    trend = 'Short' if df['Bitcoin'].tail(1).values < df['SMA'].tail(1).values else 'Long'
    x = pd.DataFrame(client.get_ticker())
    y =  x[x.symbol.str.contains('USDT')]
    z = y[~(y.symbol.str.contains('BULL')) & ~(y.symbol.str.contains('BEAR'))]
    z = z[z.symbol.apply(lambda x: ('USDT' in x[-4:]))]
    z = z[z.lastPrice.astype(float)!=0]
    final = z[['symbol','lastPrice','priceChangePercent']]
    final[['lastPrice','priceChangePercent']] = final[['lastPrice','priceChangePercent']].astype(float)
    final = final.sort_values('priceChangePercent',ascending=False)
    up = final.sort_values('priceChangePercent',ascending=False).head(3)
    top = final[(final.symbol.str.contains('BTCDOWNUSDT'))].append(final[(final.symbol.str.contains('ETHUSDT'))]).append(final[(final.symbol.str.contains('ADAUSDT'))])
    up = up.append(top)
    down = final[final.symbol.str.contains('DOWN')].sort_values('priceChangePercent',ascending=False).head(5)
    down = down.append(final[(final.symbol.str.contains('BURGERUSDT'))])
    direction = down if trend=='Short' else up
    return [direction,up,down]


def account():
    x = pd.DataFrame(client.get_ticker())
    account = pd.DataFrame(client.get_account()['balances'])
    account[['free','locked']] = account[['free','locked']].astype(float)
    holding = account[account['free']!=0.00000000]
    cash = holding[(holding.asset.str.contains('BNB'))].append(holding[(holding.asset.str.contains('USDT'))])
    holding = holding[~(holding.asset.str.contains('BNB')) & ~(holding.asset.str.contains('USDT'))]
    holding = holding[holding.asset.values!='BNB']
    holding = holding[holding.asset.values!='USDT']
    holding['symbol'] = [i + 'USDT' for i in holding.asset.to_list()]
    holding['lastPrice'] = holding.merge(x,how='inner',on=['symbol'])['lastPrice'].values
    holding[['free','lastPrice']] = holding[['free','lastPrice']].astype(float)
    holding.index = range(len(holding))
    del holding['locked']
    del holding['asset']
    holding['value'] = holding['lastPrice'] * holding['free']
    holding.columns = ['free', 'symbol', 'Price', 'value']
    holding = holding[['symbol', 'free', 'Price', 'value']]
    holding['Posture'] = ['Short' if 'DOWN' in i else 'Long' for i in holding.symbol.to_list()]
    holding = holding.sort_values('symbol',ascending=True)
    orders = [client.get_all_orders(symbol=i, limit=1) for i in holding.symbol.to_list()]
    book = pd.DataFrame(orders[0][-1].values(),index=orders[0][-1].keys()).T[['symbol','origQty','cummulativeQuoteQty','updateTime','side']]
    for i in range(1,len(orders)):
      book = book.append(pd.DataFrame(orders[i][-1].values(),index=orders[i][-1].keys()).T[['symbol','origQty','cummulativeQuoteQty','updateTime','side']])
      
    book['updateTime'] = [dt.datetime.fromtimestamp(i/1000).strftime('%Y-%m-%d %H:%M:%S.%f') for i in book['updateTime'].to_list()]
    book[['origQty','cummulativeQuoteQty']]= book[['origQty','cummulativeQuoteQty']].astype(float)
    book['Price'] = book['cummulativeQuoteQty'] / book['origQty']
    book = book[['updateTime','symbol','cummulativeQuoteQty','origQty','Price']]
    book.columns = ['time','symbol','money','quant','price']
    holding = pd.merge(holding,book,how='inner',on=['symbol'])   
    holding.columns = ['symbol', 'nominalNow', 'lastPrice', 'value', 'Posture', 'time', 'invested', 'nominalStart','firstPrice']
    holding['PnL'] = (holding['lastPrice'] / holding['firstPrice']) - 1.0
    cash = cash[['asset','free']]
    cash.columns = ['symbol','value']
    cash = cash.append(holding[['symbol','value']])
    holding = holding.append(cash)
    return [holding.dropna(),cash]

