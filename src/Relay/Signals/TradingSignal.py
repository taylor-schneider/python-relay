import json


class TradingSignal(dict):

    def __init__(self, ticker, price, action):
#        dict.__init__(self, ticker=ticker, price=price, action=action)
        self.action = action
        self.ticker = ticker
        self.price = price


    def __str__(self):
        return json.dumps(self.__dict__)