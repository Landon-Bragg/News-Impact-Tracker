
import os, json, sys, re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json, col
from dotenv import load_dotenv

# Lazy VADER init inside UDF
_vader = None
def sentiment_score(text: str) -> float:
    global _vader
    if _vader is None:
        import nltk
        try:
            nltk.data.find('sentiment/vader_lexicon')
        except LookupError:
            nltk.download('vader_lexicon')
        from nltk.sentiment import SentimentIntensityAnalyzer
        _vader = SentimentIntensityAnalyzer()
    if not text:
        return 0.0
    return float(_vader.polarity_scores(text)['compound'])

def main():
    load_dotenv()
    broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    topic_news = os.getenv("KAFKA_TOPIC_NEWS", "news_headlines")
    topic_prices = os.getenv("KAFKA_TOPIC_PRICES", "price_ticks")
    topic_out = os.getenv("KAFKA_TOPIC_IMPACT", "impact_scores")
    symbols_env = os.getenv("SYMBOLS", "")
    universe = set(s.strip().upper() for s in symbols_env.split(",") if s.strip())

    spark = (SparkSession.builder
             .appName("impact-scoring")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Define schemas
    news_schema = StructType([
        StructField("id", StringType()),
        StructField("timestamp", StringType()),
        StructField("source", StringType()),
        StructField("headline", StringType()),
        StructField("tickers", ArrayType(StringType()))
    ])

    price_schema = StructType([
        StructField("symbol", StringType()),
        StructField("timestamp", StringType()),
        StructField("price", DoubleType())
    ])

    # Read Kafka streams
    news_raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic_news)
        .option("startingOffsets", "latest")
        .load())

    prices_raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic_prices)
        .option("startingOffsets", "latest")
        .load())

    news = (news_raw.selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), news_schema).alias("d"))
            .select("d.*")
            .withColumn("event_time", to_timestamp("timestamp"))
            .withWatermark("event_time", "10 minutes")
            )

    # explode tickers so we can join per-symbol
    if universe:
        news = news.withColumn("tickers", expr(f"filter(tickers, x -> x in ({','.join([f'\"{s}\"' for s in universe])}))"))
    news_exp = (news
                .withColumn("symbol", explode_outer("tickers"))
                .dropna(subset=["symbol"]))

    # Sentiment UDF
    sent_udf = udf(sentiment_score, DoubleType())
    news_sent = news_exp.withColumn("sentiment", sent_udf(col("headline")))

    prices = (prices_raw.selectExpr("CAST(value AS STRING) as json")
              .select(from_json(col("json"), price_schema).alias("d"))
              .select("d.*")
              .withColumn("event_time", to_timestamp("timestamp"))
              .withWatermark("event_time", "10 minutes")
              )

    # Compute pct change over a 10-minute window per symbol using last and first
    price_window = (prices
        .withColumn("price_ts", col("event_time"))
        .groupBy(window(col("event_time"), "10 minutes", "1 minute"), col("symbol"))
        .agg(
            first("price").alias("price_first"),
            last("price").alias("price_last"),
        )
        .withColumn("pct_change", when(col("price_first") > 0, (col("price_last")/col("price_first") - 1.0) * 100.0).otherwise(lit(0.0)))
    )

    # Join news with nearest price window by symbol and time overlap
    joined = (news_sent
        .join(price_window, on=[col("symbol") == col("symbol")], how="left")
        .withColumn("impact_score", col("sentiment") * col("pct_change"))
        .select(
            col("symbol"),
            lit(600).alias("window_sec"),
            col("sentiment"),
            col("pct_change"),
            col("impact_score"),
            col("id").alias("headline_id"),
            col("headline"),
            date_format(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX").alias("event_time")
        )
    )

    # Write to console for dev
    console_q = (joined.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start())

    # Also write back to Kafka as JSON
    out_df = joined.selectExpr("to_json(named_struct('symbol', symbol, 'window_sec', window_sec, 'sentiment', sentiment, 'pct_change', pct_change, 'impact_score', impact_score, 'headline_id', headline_id, 'headline', headline, 'event_time', event_time)) AS value")

    kafka_q = (out_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("topic", topic_out)
        .option("checkpointLocation", "/tmp/impact_scores_ckpt")
        .outputMode("append")
        .start())

    console_q.awaitTermination()
    kafka_q.awaitTermination()

if __name__ == "__main__":
    main()
