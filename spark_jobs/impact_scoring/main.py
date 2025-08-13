# spark_jobs/impact_scoring/main.py

import os
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, from_json, to_timestamp, expr, explode_outer, first, last,
    when, lit, date_format, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, DoubleType
)
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# --------------------------
# Lazy VADER sentiment UDF
# --------------------------
_vader = None
def _sentiment_score(text: str) -> float:
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

sent_udf = udf(_sentiment_score, DoubleType())


def main():
    # --------------------------
    # Env + Windows Hadoop setup
    # --------------------------
    load_dotenv()

    # Kafka topics / symbols
    broker       = os.getenv("KAFKA_BROKER", "localhost:9092")
    topic_news   = os.getenv("KAFKA_TOPIC_NEWS",   "news_headlines")
    topic_prices = os.getenv("KAFKA_TOPIC_PRICES", "price_ticks")
    topic_out    = os.getenv("KAFKA_TOPIC_IMPACT", "impact_scores")

    symbols_env = os.getenv("SYMBOLS", "")
    universe = {s.strip().upper() for s in symbols_env.split(",") if s.strip()}

    # Windows + Hadoop specifics (prevents NativeIO/perm issues)
    os.environ["HADOOP_HOME"]     = r"C:\hadoop"
    os.environ["hadoop.home.dir"] = r"C:\hadoop"
    os.environ["PATH"]            = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
    os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.io.tmpdir=C:\\spark-tmp"

    # Use Windows-safe absolute paths and file:// URIs
    local_tmp      = r"C:\spark-tmp"
    warehouse_uri  = r"file:///C:/spark-warehouse"
    checkpoint_uri = r"file:///C:/spark-checkpoints/impact_scores"

    # --------------------------
    # Spark session
    # --------------------------
    spark = (
        SparkSession.builder
        .appName("impact-scoring")
        .config("spark.master", "local[*]")
        .config("spark.local.dir", local_tmp)
        .config("spark.sql.warehouse.dir", warehouse_uri)
        .config("spark.hadoop.tmp.dir", "file:///C:/spark-tmp")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=C:\\hadoop\\bin -Dhadoop.home.dir=C:\\hadoop -Djava.io.tmpdir=C:\\spark-tmp"
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Djava.library.path=C:\\hadoop\\bin -Dhadoop.home.dir=C:\\hadoop -Djava.io.tmpdir=C:\\spark-tmp"
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --------------------------
    # Schemas
    # --------------------------
    news_schema = StructType([
        StructField("id",       StringType()),
        StructField("timestamp",StringType()),
        StructField("source",   StringType()),
        StructField("headline", StringType()),
        StructField("tickers",  ArrayType(StringType()))
    ])

    price_schema = StructType([
        StructField("symbol",   StringType()),
        StructField("timestamp",StringType()),
        StructField("price",    DoubleType())
    ])

    # --------------------------
    # Kafka sources (streams)
    # --------------------------
    news_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic_news)
        .option("startingOffsets", "latest")
        .load()
    )

    prices_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic_prices)
        .option("startingOffsets", "latest")
        .load()
    )

    # --------------------------
    # Parse + watermark
    # --------------------------
    news = (
        news_raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), news_schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp("timestamp"))
        .withWatermark("event_time", "10 minutes")
    )

    # optional: filter to universe if provided, then explode symbols
    if universe:
        syms = ", ".join([f"'{s}'" for s in sorted(universe)])
        news = news.withColumn("tickers", expr(f"filter(tickers, x -> x in ({syms}))"))

    news_exp = (
        news.withColumn("symbol", explode_outer("tickers"))
            .dropna(subset=["symbol"])
    )

    # Sentiment
    news_sent = news_exp.withColumn("sentiment", sent_udf(col("headline")))

    # Prices with watermark
    prices = (
        prices_raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), price_schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp("timestamp"))
        .withWatermark("event_time", "10 minutes")
    )

    # --------------------------
    # Price window agg (10m window, 1m slide)
    # --------------------------
    price_window = (
        prices
        .groupBy(window(col("event_time"), "10 minutes", "1 minute"), col("symbol"))
        .agg(
            first("price", ignorenulls=True).alias("price_first"),
            last("price",  ignorenulls=True).alias("price_last"),
        )
        .withColumn(
            "pct_change",
            when(col("price_first") > 0,
                 (col("price_last")/col("price_first") - 1.0) * 100.0
            ).otherwise(lit(0.0))
        )
    )

    # --------------------------
    # Window the news the same way to satisfy stream-stream join
    # --------------------------
    news_win = news_sent.withColumn("window", window(col("event_time"), "10 minutes", "1 minute"))

    # --------------------------
    # Join on (symbol, window)
    # --------------------------
    joined = (
        news_win.alias("n")
        .join(
            price_window.alias("p"),
            on=[col("n.symbol") == col("p.symbol"),
                col("n.window") == col("p.window")],
            how="left"
        )
        .withColumn("impact_score", col("n.sentiment") * col("p.pct_change"))
        .select(
            col("n.symbol").alias("symbol"),
            lit(600).alias("window_sec"),
            col("n.sentiment").alias("sentiment"),
            col("p.pct_change").alias("pct_change"),
            col("impact_score"),
            col("n.id").alias("headline_id"),
            col("n.headline").alias("headline"),
            date_format(col("n.event_time"), "yyyy-MM-dd'T'HH:mm:ssX").alias("event_time"),
        )
    )

    # --------------------------
    # Console sink (for dev)
    # --------------------------
    console_q = (
        joined.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
    )

    # --------------------------
    # Kafka sink (JSON) with Windows-safe checkpoint
    # --------------------------
    out_df = joined.selectExpr(
        "to_json(named_struct("
        "'symbol', symbol, "
        "'window_sec', window_sec, "
        "'sentiment', sentiment, "
        "'pct_change', pct_change, "
        "'impact_score', impact_score, "
        "'headline_id', headline_id, "
        "'headline', headline, "
        "'event_time', event_time"
        ")) AS value"
    )

    kafka_q = (
        out_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("topic", topic_out)
        .option("checkpointLocation", checkpoint_uri)  # file:///C:/...
        .outputMode("append")
        .start()
    )

    console_q.awaitTermination()
    kafka_q.awaitTermination()


if __name__ == "__main__":
    main()
