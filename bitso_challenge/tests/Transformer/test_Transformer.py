import pytest
from pyspark.sql import SparkSession
from utils.Transformer.Transformer import Transformer

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

@pytest.fixture
def transformer():
    return Transformer()

def test_transform_exchanges(spark, transformer):
    data = [("USA", "bitso", 10, 1, 2014, "Bitso", [{"base": "BTC", "target": "USD", "coin_id": "bitcoin"}])]
    schema = ["country", "exchange_id", "trust_score", "trust_score_rank", "year_established", "name", "tickers"]
    df = spark.createDataFrame(data, schema)
    
    result = transformer.transform_exchanges(df)
    
    assert result is not None
    assert "exchange_name" in result.columns

def test_transform_bitso_data(spark, transformer):
    data = [("USA", "bitso", 10, 1, 2014, "Bitso", [{"base": "BTC", "target": "USD", "coin_id": "bitcoin"}])]
    schema = ["country", "exchange_id", "trust_score", "trust_score_rank", "year_established", "exchange_name", "tickers"]
    df = spark.createDataFrame(data, schema)
    
    result = transformer.transform_bitso_data(df)
    
    assert result is not None
    assert "bitso_market" in result.columns

def test_transform_shared_markets(spark, transformer):
    exchanges_data = [("USA", "bitso", 10, 1, 2014, "Bitso", [{"base": "BTC", "target": "USD", "coin_id": "bitcoin"}])]
    exchanges_schema = ["country", "exchange_id", "trust_score", "trust_score_rank", "year_established", "exchange_name", "tickers"]
    exchanges_df = spark.createDataFrame(exchanges_data, exchanges_schema)
    
    bitso_data = [("bitso", "BTC", "USD", "bitcoin", "BTC-USD")]
    bitso_schema = ["exchange_id", "base", "target", "base_id", "bitso_market"]
    bitso_df = spark.createDataFrame(bitso_data, bitso_schema)
    
    result = transformer.transform_shared_markets(exchanges_df, bitso_df)
    
    assert result is not None
    assert "market_id" in result.columns

def test_transform_volume(spark, transformer):
    volume_data = [(1638316800, 100.0,"BTC-USD")]
    volume_schema = ["timestamp", "volume","market_id"]
    volume_df = spark.createDataFrame(volume_data, volume_schema)
    
    result = transformer.transform_volume(volume_df, "usd")
    
    assert result is not None
    assert "volume_usd" in result.columns