from unittest import TestCase
from unittest.mock import patch, MagicMock
from utils.DataConnector import DataConnector
from pyspark.sql import SparkSession
import requests

class TestDataConnector(TestCase):

    def setUp(self):
        self.connector = DataConnector()

    def tearDown(self):
        self.connector.close()

    @patch('utils.DataConnector.DataConnector.requests.get')
    def test_check_api_success(self, mock_get):
        mock_get.return_value.raise_for_status.return_value = None
        self.assertTrue(self.connector.check_api())
        mock_get.assert_called_once_with(self.connector.PING_API, headers=self.connector.HEADERS)
    

    @patch('utils.DataConnector.DataConnector.requests.get')
    def test_check_api_failure(self, mock_get):
        mock_get.side_effect = requests.RequestException("API is down")
        self.assertFalse(self.connector.check_api())
        mock_get.assert_called_once_with(self.connector.PING_API, headers=self.connector.HEADERS)

    def test_read_exchanges(self):
        with patch.object(self.connector, '_get_exchanges') as mock_get_exchanges, \
             patch.object(self.connector, 'spark') as mock_spark:
            mock_get_exchanges.return_value = None
            mock_spark.createDataFrame.return_value = MagicMock()
            self.connector.exchanges_list = ['bitso']
            result = self.connector.read_exchanges()
            self.assertIsNotNone(result)

    def test_read_btc_volume(self):
        with patch.object(self.connector, 'spark') as mock_spark:
            mock_spark.createDataFrame.return_value = MagicMock()
            self.connector.exchanges_list = ['bitso']
            result = self.connector.read_btc_volume()
            self.assertIsNotNone(result)

    def test_read_market_usd_volume(self):
        with patch.object(self.connector, 'spark') as mock_spark:
            mock_spark.createDataFrame.return_value = MagicMock()
            coin_id_list = [{'base_id': 'bitcoin', 'base': 'BTC'}]
            result = self.connector.read_market_usd_volume(coin_id_list)
            self.assertIsNotNone(result)

    def test_close(self):
        with patch.object(SparkSession, 'stop') as mock_stop:
            self.connector.close()
            mock_stop.assert_called_once()
        