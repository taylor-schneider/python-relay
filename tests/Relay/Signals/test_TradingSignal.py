import json
from unittest import TestCase
from Relay.Signals.TradingSignal import TradingSignal
from Relay.Signals.TradingAction import TradingAction

class test_TradingSignal(TestCase):

    def test_success__to_string(self):
        ts = TradingSignal("aaba", 5.0, TradingAction.SELL)
        s = str(ts)
        self.assertEqual('{"ticker": "aaba", "price": 5.0, "action": "3"}', s)

    def test_success__to_json(self):
        ts = TradingSignal("aaba", 5.0, TradingAction.SELL)
        s = json.dumps(ts)
        self.assertEqual('{"ticker": "aaba", "price": 5.0, "action": "3"}', s)

    def test_success__from_json(self):
        ts = TradingSignal("aaba", 5.0, TradingAction.SELL)
        o = json.loads('{"ticker": "aaba", "price": 5.0, "action": "3"}')
        self.assertNotEqual(o.__dict__, ts.__dict__)

    def test_success__from_different_json(self):
        ts = TradingSignal("aaba", 5.0, TradingAction.SELL)
        raw_json = '{"ticker": "aaba", "price": 5.0, "action": "2"}'
        o = json.loads(raw_json, object_hook=lambda d: TradingSignal(**d))
        self.assertNotEqual(o.__dict__, ts.__dict__)
        self.assertNotEqual(o.action, ts.action)
        self.assertEqual(o.ticker, ts.ticker)

    def test_success__from_string(self):
        s = TradingAction['BUY']
        self.assertEqual(TradingAction.BUY, s)
