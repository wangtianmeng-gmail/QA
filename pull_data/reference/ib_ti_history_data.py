# -*- coding: utf-8 -*-
"""
IBAPI - Getting Contract info

@author: Mayank Rasu (http://rasuquant.com/wp/)
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import time
import pandas as pd
from decimal import Decimal
import numpy as np


class TradingApp(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self,wrapper = self)
        self.data = {}
        self.order_df = pd.DataFrame(columns=['PermId', 'ClientId', 'OrderId',
                                              'Account', 'Symbol', 'SecType',
                                              'Exchange', 'Action', 'OrderType',
                                              'TotalQty', 'CashQty', 'LmtPrice',
                                              'AuxPrice', 'Status'])
        
    def error(self, reqId,errorTime, errorCode, errorString, advancedOrderRejectJson):
        print("Error reqId: {}, errorTime:{}, errorCode: {}, errorString: {}, advancedOrderRejectJson: {}".format(reqId,errorTime,errorCode,errorString,advancedOrderRejectJson))
        
    def contractDetails(self, reqId, contractDetails):
        print("redID: {}, contract:{}".format(reqId,contractDetails))

    def historicalData(self, reqId, bar):
        # print("HistoricalData. ReqId:", reqId, "BarData.", bar)
        if reqId not in self.data:
            self.data[reqId] = pd.DataFrame([{"Date":bar.date,"Open":bar.open,"High":bar.high,"Low":bar.low,"Close":bar.close,"Volume":bar.volume,"WAP":bar.wap,"BarCount":bar.barCount}])
        else:
            self.data[reqId] = pd.concat((self.data[reqId],pd.DataFrame([{"Date":bar.date,"Open":bar.open,"High":bar.high,"Low":bar.low,"Close":bar.close,"Volume":bar.volume,"WAP":bar.wap,"BarCount":bar.barCount}])))
            #self.data[reqId].append({"Date":bar.date,"Open":bar.open,"High":bar.high,"Low":bar.low,"Close":bar.close,"Volume":bar.volume,"WAP":bar.wap,"BarCount":bar.barCount})

    def nextValidId(self, orderId):
        # super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        #print("NextValidId:", orderId)

    def openOrder(self, orderId, contract, order, orderState):
        #print(orderId, contract, order, orderState)
        dictionary = {"PermId":order.permId, "ClientId": order.clientId, "OrderId": orderId, 
                      "Account": order.account, "Symbol": contract.symbol, "SecType": contract.secType,
                      "Exchange": contract.exchange, "Action": order.action, "OrderType": order.orderType,
                      "TotalQty": order.totalQuantity, "CashQty": order.cashQty, 
                      "LmtPrice": order.lmtPrice, "AuxPrice": order.auxPrice, "Status": orderState.status}
        # Convert dictionary to DataFrame
        new_row_df = pd.DataFrame([dictionary])
        # Concatenate the new row with the existing DataFrame
        self.order_df = pd.concat([self.order_df, new_row_df], ignore_index=True)
        # print(self.order_df)

    def position(self, account: str, contract: Contract, position: Decimal, avgCost: float):
        print("Position.", "Account:", account, "Contract:", contract, "Position:", position, "Avg cost:", avgCost)

    def accountSummary(self, reqId: int, account: str, tag: str, value: str,currency: str):
        print("AccountSummary. ReqId:", reqId, "Account:", account,"Tag: ", tag, "Value:", value, "Currency:", currency)
    
    def accountSummaryEnd(self, reqId: int):
        print("AccountSummaryEnd. ReqId:", reqId)


#creating object of the Contract class - will be used as a parameter for other function calls
def usTechStk(symbol = "TSLA",sec_type="STK",currency="USD",exchange="SMART"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    return contract 

#creating object of the limit order class - will be used as a parameter for other function calls
def limitOrder(direction,quantity,lmt_price):
    order = Order()
    order.action = direction
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = lmt_price
    return order

#reqeust historical data
def histData(req_num,contract,duration,candle_size):
    app.reqHistoricalData(reqId=req_num, 
                          contract=contract,
                          endDateTime='',
                          durationStr=duration,
                          barSizeSetting=candle_size,
                          whatToShow='TRADES',
                          useRTH=1,
                          formatDate=1,
                          keepUpToDate=0,
                          chartOptions=[])	 # EClient function to request contract details


###################storing trade app object in dataframe#######################
def dataDataframe(TradeApp_obj,symbols):
    "returns extracted historical data in dataframe format"
    df_data = {}
    for symbol in symbols:
        df_data[symbol] = pd.DataFrame(TradeApp_obj.data[symbols.index(symbol)])
        df_data[symbol].set_index("Date",inplace=True)
    TradeApp_obj.data = {}
    return df_data


def websocket_con():
    print("Initiating the app")
    app.run()
    print("app.run() ends")


app = TradingApp()  
app.connect("127.0.0.1", 7497, clientId=1)

# starting a separate daemon thread to execute the websocket connection
con_thread = threading.Thread(target=websocket_con, daemon=True)
con_thread.start()  
time.sleep(0.2) # some latency added to ensure that the connection is established
print("check connection: {}".format(app.isConnected()))



# get history data
tickers = ["AMZN","AAPL","QQQ"]
# tickers = ["AMZN"]

for ticker in tickers:
    histData(req_num = tickers.index(ticker),contract= usTechStk(symbol = ticker),duration = '5 D', candle_size='1 min')
    time.sleep(1)  # some latency added to ensure that the contract details request has been processed
    #print(app.data)
#extract and store historical data in dataframe
historicalData = dataDataframe(app,tickers)
# print(historicalData)


# define function of MACD
def MACD(DF,a=12,b=26,c=9):
    """function to calculate MACD
       typical values a(fast moving average) = 12; 
                      b(slow moving average) =26; 
                      c(signal line ma window) =9"""
    df = DF.copy()
    df["MA_Fast"]=df["Close"].ewm(span=a,min_periods=a).mean()
    df["MA_Slow"]=df["Close"].ewm(span=b,min_periods=b).mean()
    df["MACD"]=df["MA_Fast"]-df["MA_Slow"]
    df["Signal"]=df["MACD"].ewm(span=c,min_periods=c).mean()
    df["OSMA"]=df["MACD"]-df["Signal"]
    df.dropna(inplace=False)
    return df
# calculate and store MACD values
macdDF = {}
for ticker in tickers:
    macdDF[ticker] = MACD(historicalData[ticker],12,26,9)
# # save df to csv
# print(macdDF)
# macdDF["AMZN"].to_csv('output.csv', index=True)  # Save as CSV


# define function of ATR
def atr(DF,n):
    "function to calculate True Range and Average True Range"
    df = DF.copy()
    df['H-L']=abs(df['High']-df['Low'])
    df['H-PC']=abs(df['High']-df['Close'].shift(1))
    df['L-PC']=abs(df['Low']-df['Close'].shift(1))
    df['TR']=df[['H-L','H-PC','L-PC']].max(axis=1,skipna=False)
    #df['ATR'] = df['TR'].rolling(n).mean()
    df['ATR'] = df['TR'].ewm(com=n,min_periods=n).mean()
    return df
#calculate and store ATR values
TI_dict = {}
for ticker in tickers:
    TI_dict[ticker] = atr(historicalData[ticker],20)
# # save df to csv
# print(TI_dict)
# TI_dict["AMZN"].to_csv('output.csv', index=True)  # Save as CSV


# define function of Bollinger Bands
def bollBnd(DF,n=20):
    "function to calculate Bollinger Band"
    df = DF.copy()
    #df["MA"] = df['close'].rolling(n).mean()
    df["MA"] = df['Close'].ewm(span=n,min_periods=n).mean()
    df["BB_up"] = df["MA"] + 2*df['Close'].rolling(n).std(ddof=0) #ddof=0 is required since we want to take the standard deviation of the population and not sample
    df["BB_dn"] = df["MA"] - 2*df['Close'].rolling(n).std(ddof=0) #ddof=0 is required since we want to take the standard deviation of the population and not sample
    df["BB_width"] = df["BB_up"] - df["BB_dn"]
    df.dropna(inplace=False)
    return df
#calculate and store Bollinger Bands values
TI_dict = {}
for ticker in tickers:
    TI_dict[ticker] = bollBnd(historicalData[ticker],20)
# # save df to csv
# print(TI_dict)
# TI_dict["AMZN"].to_csv('output.csv', index=True)  # Save as CSV


# define function of RSI
def rsi(DF,n=20):
    "function to calculate RSI"
    df = DF.copy()
    df['delta']=df['Close'] - df['Close'].shift(1)
    df['gain']=np.where(df['delta']>=0,df['delta'],0)
    df['loss']=np.where(df['delta']<0,abs(df['delta']),0)
    avg_gain = []
    avg_loss = []
    gain = df['gain'].tolist()
    loss = df['loss'].tolist()
    for i in range(len(df)):
        if i < n:
            avg_gain.append(np.nan)
            avg_loss.append(np.nan)
        elif i == n:
            avg_gain.append(df['gain'].rolling(n).mean()[n])
            avg_loss.append(df['loss'].rolling(n).mean()[n])
        elif i > n:
            avg_gain.append(((n-1)*avg_gain[i-1] + gain[i])/n)
            avg_loss.append(((n-1)*avg_loss[i-1] + loss[i])/n)
    df['avg_gain']=np.array(avg_gain)
    df['avg_loss']=np.array(avg_loss)
    df['RS'] = df['avg_gain']/df['avg_loss']
    df['RSI'] = 100 - (100/(1+df['RS']))
    return df
#calculate and store RSI values
TI_dict = {}
for ticker in tickers:
    TI_dict[ticker] = rsi(historicalData[ticker],20)
# save df to csv
print(TI_dict)
TI_dict["AMZN"].to_csv('output.csv', index=True)  # Save as CSV




print("Closing the connection")
app.disconnect()

time.sleep(0.2)
print("check connection: {}".format(app.isConnected()))

    