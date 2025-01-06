from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
import requests
from concurrent.futures import ThreadPoolExecutor
from functools import reduce
from typing import List, Dict, Any, Optional
import json
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataConnector:
    def __init__(self):
        self.spark = SparkSession.builder \
            .config("spark.jars", "/usr/local/postgresql-42.2.5.jar")\
            .appName("bitso") \
            .master("local[1]") \
            .getOrCreate()
        self.sc = self.spark.sparkContext
        self.HEADERS = {
            "accept": "application/json",
            "x-cg-demo-api-key": "CG-Jzz57Ym47ZTDMQGYZ6nJorcW",
            'User-agent': 'student 1.0'
        }
        self.EXCHANGES_URL =  "https://api.coingecko.com/api/v3/exchanges?per_page=10&page=1"
        self.EXCHANGES_DATA = "https://api.coingecko.com/api/v3/exchanges/"
        self.BTC_VOLUME = "https://api.coingecko.com/api/v3/exchanges/id/volume_chart?days=90"
        self.COIN_DATA = "https://api.coingecko.com/api/v3/coins/id/market_chart?vs_currency=usd&days=90&interval=daily"
        self.PING_API = "https://api.coingecko.com/api/v3/ping"

    def check_api(self) -> bool:
        logger.info("Checking API status")
        try:
            response = requests.get(self.PING_API, headers=self.HEADERS)
            response.raise_for_status()
            logger.info("API is up and running")
            return True
        except requests.RequestException as e:
            logger.error("API is down: %s", e)
            return False
        except Exception as e:
            logger.error("Unexpected error: %s", e)
            return False
        
    def _get_exchanges(self) -> None:
        logger.info("Fetching exchanges data from %s", self.EXCHANGES_URL)
        try:
            response = requests.get(self.EXCHANGES_URL, headers=self.HEADERS)
            response.raise_for_status()
            data = response.json()
            logger.info("Successfully fetched exchanges data")
            logger.info("Setting exchanges list")
            df =  self.spark.createDataFrame(data)
            self.exchanges_list = df.select("id").rdd.flatMap(lambda x: x).collect()
            self.exchanges_list.append("bitso")
            logger.info("Successfully set exchanges list")
        except requests.RequestException as e:
            logger.error("Error fetching exchanges data: %s", e)
            return None
        except Exception as e:
            logger.error("Unexpected error: %s", e)
            return None
            
    def read_exchanges(self) -> Optional[DataFrame]:
        def fetch_data(exchange_id: str) -> Dict[str, Any]:
            try:
                url = self.EXCHANGES_DATA + exchange_id
                response = requests.get(url, headers=self.HEADERS)
                response.raise_for_status()
                data = response.json()
                data['exchange_id'] = exchange_id  # Include the exchange_id in the data
                return data
            except requests.RequestException as e:
                print(f"Error fetching shared markets data for {exchange_id}: {e}")
                return {}
            except Exception as e:
                print(f"Unexpected error for {exchange_id}: {e}")
                return {}

        try:
            self._get_exchanges()
            with ThreadPoolExecutor() as executor:
                data = list(executor.map(fetch_data, self.exchanges_list))
            return self.spark.createDataFrame(data)
        except Exception as e:
            print(f"Error creating DataFrame for shared markets: {e}")
            return None

    def read_btc_volume(self) -> Optional[DataFrame]:
        logger.info("Fetching BTC volume data for exchange IDs: %s", self.exchanges_list)
        #time.sleep(30)
        def fetch_data(exchange_id: str) -> Optional[DataFrame]:
            try:
                url = self.BTC_VOLUME.replace("id", exchange_id)
                response = requests.get(url, headers=self.HEADERS)
                response.raise_for_status()
                data = response.json()
                cols = ["timestamp", "volume"]
                df = self.spark.createDataFrame(data).toDF(*cols)
                df = df.withColumn("exchange_id", F.lit(exchange_id))
                logger.info("Successfully fetched BTC volume data for %s", exchange_id)
                return df
            except requests.RequestException as e:
                logger.error("Error fetching BTC volume data for %s: %s", exchange_id, e)
                return None
            except Exception as e:
                logger.error("Unexpected error for %s: %s", exchange_id, e)
                return None

        try:
            with ThreadPoolExecutor() as executor:
                df_list = list(executor.map(fetch_data, self.exchanges_list))
            df_all = df_list[0]
            for df in df_list[1:]:
                df_all = df_all.unionByName(df)
            logger.info("Successfully created DataFrame for BTC volume data")
            return df_all
        except Exception as e:
            logger.error("Error creating DataFrame for BTC volume: %s", e)
            return None
        
    def read_market_usd_volume(self, coin_id_list: list) -> Optional[DataFrame]:
        def fetch_data(coin: dict) -> Optional[DataFrame]:
            try:
                url = self.COIN_DATA.replace("id", coin["base_id"])
                response = requests.get(url, headers=self.HEADERS)
                response.raise_for_status()
                data = response.json()['total_volumes']
                cols = ["timestamp", "volume"]
                df = self.spark.createDataFrame(data).toDF(*cols)
                df = df.withColumn("market_id", F.concat(F.lit(coin["base"]),F.lit("-USD")))
                logger.info("Successfully fetched USD volume data for %s", coin["base_id"])
                time.sleep(20)
                return df
            except requests.RequestException as e:
                logger.error("Error fetching USD volume data for %s: %s", coin["base_id"], e)
                return None
            except Exception as e:
                logger.error("Unexpected error for %s: %s", coin["base_id"], e)
                return None

        try:
            with ThreadPoolExecutor() as executor:
                df_list = list(executor.map(fetch_data, coin_id_list))
            df_all = df_list[0]
            for df in df_list[1:]:
                df_all = df_all.unionByName(df)
            logger.info("Successfully created DataFrame for USD volume data")
            return df_all
        except Exception as e:
            logger.error("Error creating DataFrame for USD volume: %s", e)
            return None
        
    def write_to_postgres(self, df: DataFrame, table_name: str) -> None:
        logger.info("Writing data to Postgres table %s", table_name)
        try:
            df.write.format("jdbc") \
                .option("url", "jdbc:postgresql://db:5432/bitso") \
                .option("driver", "org.postgresql.Driver")\
                .option("dbtable", table_name) \
                .option("user", "bitso") \
                .option("password", "bitso") \
                .mode("overwrite") \
                .save()
            logger.info("Successfully wrote data to Postgres table %s", table_name)
        except Exception as e:
            logger.error("Error writing data to Postgres table %s: %s", table_name, e)

    def close(self) -> None:
        self.spark.stop()
        logger.info("Spark session stopped")