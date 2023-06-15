import json
import unittest

import server


class TestServer(unittest.TestCase):
    def setUp(self):
        server.app.testing = True
        self.app = server.app.test_client()

    def test_get_transaction(self):
        response = self.app.get('/api/v1/data/transactions/31373')
        self.assertEqual(response.status_code, 200)
        self.assertIn({'user_id': 31373, 'amount_in_dollars': 750, 'datetime': '2023-03-25T11:31:26+00:00', 'merchant_type_code': 5310}, 
                      json.loads(response.data))

    def test_get_transaction_with_invalid_user_id(self):
        response = self.app.get('/api/v1/data/transactions/123abc')
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json, {'error': 'userId must be an integer'})

    def test_get_net_return(self):
        response = self.app.get('/api/v1/data/netAmount/5732')
        self.assertEqual(response.status_code, 200)
        self.assertIn({'merchant_type_code': 5732, 'net_amount_in_dollars': 1047, 'date': '2023-03-30'}, 
                      json.loads(response.data))

    def test_get_net_return_with_invalid_merchant_type_code(self):
        response = self.app.get('/api/v1/data/netAmount/123abc')
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json, {'error': 'merchantTypeCode must be an integer'})