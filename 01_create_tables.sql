\c bitso;

CREATE TABLE "exchanges" (
    "exchange_id" VARCHAR(10) NOT NULL,
    "exchange_name" VARCHAR(20) NOT NULL,
    "year_established" INT NOT NULL,
    "country" TEXT NOT NULL,
    "trust_score" INT NOT NULL,
    "trust_score_rank" INT NOT NULL,
    CONSTRAINT "PK_Exchanges" PRIMARY KEY ("exchange_id")
);

CREATE TABLE "shared_markets" (
    "exchange_id" VARCHAR(10) NOT NULL,
    "market_id" VARCHAR(10) NOT NULL,
    "base" VARCHAR(20) NOT NULL,
    "target" VARCHAR(20) NOT NULL,
    "name" VARCHAR(20) NOT NULL,
    CONSTRAINT "PK_SharedMarkets" PRIMARY KEY ("exchange_id","market_id")
);

CREATE TABLE "historical_usd_volume" (
    "market_id" VARCHAR(10) NOT NULL,
    "date" VARCHAR(10) NOT NULL,
    "volume_usd" FLOAT NOT NULL,
    CONSTRAINT "PK_HistoricalUSDVolume" PRIMARY KEY ("market_id", "date")
);

CREATE TABLE "historical_btc_volume" (
    "exchange_id" VARCHAR(10) NOT NULL,
    "date" VARCHAR(10) NOT NULL,
    "volume_btc" FLOAT NOT NULL,
    CONSTRAINT "PK_HistoricalBTCVolume" PRIMARY KEY ("exchange_id", "date")
);