from enum import Enum
import json


class TradingActionJsonEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__

class TradingAction(str, Enum):
    NONE = 0
    HOLD = 1,
    BUY = 2,
    SELL = 3

    def __str__(self):
        return self.name
