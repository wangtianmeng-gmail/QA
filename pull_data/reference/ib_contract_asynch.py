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


class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self,self)
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





# # creating object of the Contract class - will be used as a parameter for other function calls
# contract = usTechStk(symbol = "TSLA")
# app.reqContractDetails(reqId = 100, contract) # EClient function to request contract details
# time.sleep(10) # some latency added to ensure that the contract details request has been processed

# get history data
tickers = ["AMZN","AAPL","QQQ"]
for ticker in tickers:
    histData(req_num = tickers.index(ticker),contract= usTechStk(symbol = ticker),duration = '1 D', candle_size='1 hour')
    time.sleep(3)  # some latency added to ensure that the contract details request has been processed
    #print(app.data)
historicalData = dataDataframe(app,tickers)
print(historicalData)


# ## place limit order
# app.reqIds(-1)
# time.sleep(1)
# order_id = app.nextValidOrderId
# print(order_id)
# # app.placeOrder(order_id,usTechStk("AMZN"),limitOrder("BUY",1,181)) # EClient function to request contract details
# # time.sleep(2) # some latency added to ensure that the contract details request has been processed
# # app.cancelOrder(6,"")
# # time.sleep(2)


# # check open order
# app.order_df = pd.DataFrame(columns=['PermId', 'ClientId', 'OrderId',
#                                      'Account', 'Symbol', 'SecType',
#                                      'Exchange', 'Action', 'OrderType',
#                                      'TotalQty', 'CashQty', 'LmtPrice',
#                                      'AuxPrice', 'Status'])
# app.reqOpenOrders()
# time.sleep(1)
# order_df = app.order_df
# print(order_df)
# time.sleep(0.1)

# # check positions
# app.reqPositions()
# time.sleep(0.1)


# # check account summary
# app.reqAccountSummary(9001, "All", 'NetLiquidation')
# time.sleep(1)
# app.cancelAccountSummary(9001)
# time.sleep(1)



print("Closing the connection")
app.disconnect()

time.sleep(0.2)
print("check connection: {}".format(app.isConnected()))

    