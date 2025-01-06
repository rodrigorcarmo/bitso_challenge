from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as T
from typing import List, Dict, Any, Optional
from utils.DataConnector import DataConnector
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Transformer:

    def transform_exchanges(self, exchanges: DataFrame) -> Optional[DataFrame]:
        try:
            exchanges = exchanges.select(["country", "exchange_id", "trust_score", "trust_score_rank", "year_established", "name", "tickers"])
            exchanges = exchanges.withColumnRenamed("name", "exchange_name")
            logger.info("Successfully transformed exchanges data")
            return exchanges
        except Exception as e:
            logger.error("Error transforming exchanges data: %s", e)
            return None
        
    def transform_bitso_data(self, exchanges: DataFrame) -> Optional[DataFrame]:
        try:
            bitso = exchanges.where(exchanges.exchange_id == "bitso")
            bitso = bitso.withColumn("ticker_values", F.explode("tickers"))
            bitso_df = bitso.select("exchange_id", F.col("ticker_values.base").alias("base"), 
                                                   F.col("ticker_values.target").alias("target"), 
                                                   F.col("ticker_values.coin_id").alias("base_id"))
            bitso_df = bitso_df.withColumn("bitso_market", F.concat(F.col("base"), F.lit("-"), F.col("target")))
            bitso_df = bitso_df.where(bitso_df.target == "USD")
            logger.info("Successfully transformed Bitso data")
            return bitso_df
        except Exception as e:
            logger.error("Error transforming Bitso data: %s", e)
            return None

    def transform_shared_markets(self, exchanges: DataFrame, bitso: DataFrame) -> Optional[DataFrame]:
        try:
            markets_df = exchanges.select("exchange_id", F.explode("tickers").alias("ticker_values"), "exchange_name") \
                                 .select("exchange_id", F.col("ticker_values.base").alias("base"), F.col("ticker_values.target").alias("target"), F.col("exchange_name").alias("name"))
            markets_df = markets_df.withColumn("market_id", F.concat(F.col("base"), F.lit("-"), F.col("target")))
            markets_df = markets_df.join(bitso.select("bitso_market"), markets_df.market_id == bitso.bitso_market, "inner")
            markets_df = markets_df.select(markets_df.exchange_id, markets_df.market_id,markets_df.base, markets_df.target, markets_df.name)
            
            logger.info("Successfully transformed shared markets data")
            return markets_df
        except Exception as e:
            logger.error("Error transforming shared markets data: %s", e)
            return None

    def transform_volume(self, volume: DataFrame, identifier:str) -> Optional[DataFrame]:
        try:
            volume = volume.withColumn("timestamp", volume.timestamp.cast(T.DecimalType(14)).cast("string")) \
                                   .withColumn("volume", volume.volume.cast("float"))
            volume = volume.withColumn("timestamp", F.substring(volume.timestamp, 0, 10))
            volume.createOrReplaceTempView(f"{identifier}_volume")
            id = "exchange_id"
            if identifier == "usd":
                id = "market_id"
            logger.info(f"Transforming {identifier} volume data")
            volume = volume.withColumn("sum_volume", 
                F.sum("volume").over(
                    Window.partitionBy(id)
                    .orderBy("timestamp")
                    .rowsBetween(-30, 0)
                ))
            volume = volume.withColumn("date",F.from_unixtime(F.col("timestamp"), "yyyy-MM-dd"))
            volume = volume.groupBy(id,"date")\
                           .agg(F.max(F.col("sum_volume")).alias(f"volume_{identifier}"))\
                           .select(F.col(id),
                                   F.col("date"),
                                   F.col(f"volume_{identifier}"))
            logger.info(f"Successfully transformed {identifier} volume data")
            return volume
        except Exception as e:
            logger.error(f"Error transforming {identifier} volume data: %s", e)
            return None