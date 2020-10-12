import json
from unittest import TestCase
from Relay.Signals.TradingAction import TradingAction


class test_TradingAction(TestCase):

    def test_success__to_string(self):
        a = str(TradingAction.BUY)
        self.assertEqual(a, "BUY")

    def test_success__json_dumps(self):
        a = json.dumps(TradingAction.BUY)
        self.assertEqual(a, '"2"')

    def test_success__from_string(self):
        a = TradingAction['BUY']
        self.assertEqual(a, TradingAction.BUY)

