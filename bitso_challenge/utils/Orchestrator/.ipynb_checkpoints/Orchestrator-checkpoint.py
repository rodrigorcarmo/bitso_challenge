from utils.DataConnector import DataConnector
from utils.Transformer import Transformer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Orchestrator:
    def __init__(self):
        self.data_connector = DataConnector()
        self.transformer = Transformer()

    def fetch_data(self) -> None:
        try:
            if not self.data_connector.check_api():
                raise Exception("API is down")
            self.exchanges = self.data_connector.read_exchanges()
            self.btc_volume = self.data_connector.read_btc_volume()
            logger.info("Successfully fetched all data")
        except Exception as e:
            logger.error("Error fetching data: %s", e)

    def transform_data(self) -> None:
        try:
            self.exchanges = self.transformer.transform_exchanges(self.exchanges)
            self.bitso = self.transformer.transform_bitso_data(self.exchanges)
            self.shared_markets = self.transformer.transform_shared_markets(self.exchanges, self.bitso)
            self.btc_volume = self.transformer.transform_volume(self.btc_volume, "btc")
            self.usd_volume = self.data_connector.read_market_usd_volume(list(map(lambda row: row.asDict(), self.bitso.select("base_id","base").collect())))
            self.usd_volume = self.transformer.transform_volume(self.usd_volume, "usd")
            self.exchanges = self.exchanges.drop("tickers")
            logger.info("Successfully transformed all data")
        except Exception as e:
            logger.error("Error transforming data: %s", e)

    def write_data(self) -> None:
        try:
            self.data_connector.write_to_postgres(self.exchanges, "exchanges")
            self.data_connector.write_to_postgres(self.shared_markets, "shared_markets")
            self.data_connector.write_to_postgres(self.btc_volume, "historical_btc_volume")
            self.data_connector.write_to_postgres(self.usd_volume, "historical_usd_volume")
            logger.info("Successfully wrote all data")
            self.data_connector.close()
            logger.info("Successfully closed connection")
        except Exception as e:
            logger.error("Error writing data: %s", e)

    def run(self) -> None:
        try:
            self.fetch_data()
            self.transform_data()
            self.write_data()
            logger.info("Orchestrator run completed successfully")
        except Exception as e:
            logger.error("Error running orchestrator: %s", e)