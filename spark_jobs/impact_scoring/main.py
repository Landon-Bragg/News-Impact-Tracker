import os
import platform
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
            print("Downloading VADER lexicon...")
            nltk.download('vader_lexicon')
        from nltk.sentiment import SentimentIntensityAnalyzer
        _vader = SentimentIntensityAnalyzer()
    if not text:
        return 0.0
    return float(_vader.polarity_scores(text)['compound'])

sent_udf = udf(_sentiment_score, DoubleType())

def setup_spark_config():
    """Setup Spark configuration based on platform"""
    is_windows = platform.system() == "Windows"
    
    if is_windows:
        # Windows-specific setup
        hadoop_home = r"C:\hadoop"
        spark_tmp = r"C:\spark-tmp"
        warehouse_dir = "file:///C:/spark-warehouse"
        checkpoint_dir = "file:///C:/spark-checkpoints"
        
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["hadoop.home.dir"] = hadoop_home
        os.environ["PATH"] = f"{hadoop_home}\\bin;" + os.environ.get("PATH", "")
        os.environ["JAVA_TOOL_OPTIONS"] = f"-Djava.io.tmpdir={spark_tmp}"
        
        # Create directories if they don't exist
        for dir_path in [spark_tmp, r"C:\spark-warehouse", r"C:\spark-checkpoints"]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            
        spark_config = {
            "spark.sql.warehouse.dir": warehouse_dir,
            "spark.hadoop.tmp.dir": "file:///C:/spark-tmp",
            "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.RawLocalFileSystem",
            "spark.hadoop.fs.file.impl.disable.cache": "true",
            "spark.driver.extraJavaOptions": f"-Djava.library.path={hadoop_home}\\bin -Dhadoop.home.dir={hadoop_home} -Djava.io.tmpdir={spark_tmp}",
            "spark.executor.extraJavaOptions": f"-Djava.library.path={hadoop_home}\\bin -Dhadoop.home.dir={hadoop_home} -Djava.io.tmpdir={spark_tmp}"
        }
        checkpoint_uri = f"{checkpoint_dir}/impact_scores"
    else:
        # Linux/Mac setup
        tmp_dir = "/tmp/spark"
        warehouse_dir = f"file://{tmp_dir}/warehouse"
        checkpoint_dir = f"file://{tmp_dir}/checkpoints"
        
        # Create directories
        Path(tmp_dir).mkdir(parents=True, exist_ok=True)
        Path(f"{tmp_dir}/warehouse").mkdir(parents=True, exist_ok=True)
        Path(f"{tmp_dir}/checkpoints").mkdir(parents=True, exist_ok=True)
        
        spark_config = {
            "spark.sql.warehouse.dir": warehouse_dir,
        }
        checkpoint_uri = f"{checkpoint_dir}/impact_scores"
    
    return spark_config, checkpoint_uri

def test_kafka_connection(broker):
    """Test if we can connect to Kafka"""
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            bootstrap_servers=broker,
            consumer_timeout_ms=5000,
            auto_offset_reset='latest'
        )
        # Try to get metadata
        consumer.topics()
        consumer.close()
        return True
    except Exception as e:
        print(f"Kafka connection test failed: {e}")
        return False

def main():
    load_dotenv()

    # Kafka configuration - use external port for host applications
    broker = os.getenv("KAFKA_BROKER", "localhost:29092")  # Note: 29092 for external
    topic_news = os.getenv("KAFKA_TOPIC_NEWS", "news_headlines")
    topic_prices = os.getenv("KAFKA_TOPIC_PRICES", "price_ticks")
    topic_out = os.getenv("KAFKA_TOPIC_IMPACT", "impact_scores")

    print(f"Kafka configuration:")
    print(f"  Broker: {broker}")
    print(f"  News topic: {topic_news}")
    print(f"  Prices topic: {topic_prices}")
    print(f"  Output topic: {topic_out}")

    # Test Kafka connectivity
    print("Testing Kafka connection...")
    if not test_kafka_connection(broker):
        print("ERROR: Cannot connect to Kafka. Please check:")
        print("1. Kafka is running: docker-compose ps")
        print("2. Broker address is correct (try localhost:29092)")
        print("3. Topics exist: docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --list")
        return

    print("✅ Kafka connection successful")

    symbols_env = os.getenv("SYMBOLS", "")
    universe = {s.strip().upper() for s in symbols_env.split(",") if s.strip()}
    print(f"Tracking symbols: {sorted(universe) if universe else 'All'}")

    # Platform-specific Spark setup
    spark_config, checkpoint_uri = setup_spark_config()
    
    # Build Spark session with Kafka packages
    builder = (
        SparkSession.builder
        .appName("impact-scoring")
        .config("spark.master", "local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.adaptive.enabled", "false")  # Disable for streaming
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    
    # Apply platform-specific configs
    for key, value in spark_config.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark session created")
    print(f"Checkpoint location: {checkpoint_uri}")

    # Schemas
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

    print("Creating Kafka streaming sources...")

    # Kafka sources with better error handling
    try:
        news_raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", broker)
            .option("subscribe", topic_news)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("kafka.consumer.timeout.ms", "10000")
            .load()
        )
        print(f"✅ News stream created for topic: {topic_news}")
    except Exception as e:
        print(f"❌ Failed to create news stream: {e}")
        return

    try:
        prices_raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", broker)
            .option("subscribe", topic_prices)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("kafka.consumer.timeout.ms", "10000")
            .load()
        )
        print(f"✅ Price stream created for topic: {topic_prices}")
    except Exception as e:
        print(f"❌ Failed to create price stream: {e}")
        return

    # Parse news with comprehensive error handling
    print("Setting up news processing pipeline...")
    news = (
        news_raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), news_schema).alias("d"))
        .filter(col("d").isNotNull())
        .select("d.*")
        .withColumn("event_time", to_timestamp("timestamp"))
        .filter(col("event_time").isNotNull())
        .withWatermark("event_time", "10 minutes")
    )

    # Filter to universe and explode symbols
    if universe:
        syms = ", ".join([f"'{s}'" for s in sorted(universe)])
        news = news.withColumn("tickers", expr(f"filter(tickers, x -> x in ({syms}))"))

    news_exp = (
        news.withColumn("symbol", explode_outer("tickers"))
        .filter(col("symbol").isNotNull())
    )

    # Add sentiment
    print("Adding sentiment analysis...")
    news_sent = news_exp.withColumn("sentiment", sent_udf(col("headline")))

    # Parse prices with error handling
    print("Setting up price processing pipeline...")
    prices = (
        prices_raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), price_schema).alias("d"))
        .filter(col("d").isNotNull())
        .select("d.*")
        .withColumn("event_time", to_timestamp("timestamp"))
        .filter(col("event_time").isNotNull())
        .withWatermark("event_time", "10 minutes")
    )

    # Price aggregation with better null handling
    print("Setting up price aggregation...")
    price_window = (
        prices
        .groupBy(window(col("event_time"), "10 minutes", "1 minute"), col("symbol"))
        .agg(
            first("price", ignorenulls=True).alias("price_first"),
            last("price", ignorenulls=True).alias("price_last"),
        )
        .filter((col("price_first").isNotNull()) & (col("price_last").isNotNull()))
        .withColumn(
            "pct_change",
            when((col("price_first") > 0) & (col("price_first").isNotNull()),
                 ((col("price_last") / col("price_first")) - 1.0) * 100.0
            ).otherwise(lit(0.0))
        )
    )

    # Window news for join
    news_win = news_sent.withColumn("window", window(col("event_time"), "10 minutes", "1 minute"))

    # Join with coalesce for null handling
    print("Setting up stream join...")
    joined = (
        news_win.alias("n")
        .join(
            price_window.alias("p"),
            on=[col("n.symbol") == col("p.symbol"),
                col("n.window") == col("p.window")],
            how="left"
        )
        .withColumn(
            "pct_change_final", 
            F.coalesce(col("p.pct_change"), lit(0.0))
        )
        .withColumn(
            "impact_score", 
            col("n.sentiment") * col("pct_change_final")
        )
        .select(
            col("n.symbol").alias("symbol"),
            lit(600).alias("window_sec"),
            col("n.sentiment").alias("sentiment"),
            col("pct_change_final").alias("pct_change"),
            col("impact_score"),
            col("n.id").alias("headline_id"),
            col("n.headline").alias("headline"),
            date_format(col("n.event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"),
        )
        .filter(col("impact_score").isNotNull())
    )

    # Console output for debugging
    print("Starting console output stream...")
    console_q = (
        joined.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Kafka output with better error handling
    print("Setting up Kafka output stream...")
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

    try:
        kafka_q = (
            out_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", broker)
            .option("topic", topic_out)
            .option("checkpointLocation", checkpoint_uri)
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .start()
        )
        print(f"✅ Kafka output stream started, writing to: {topic_out}")
    except Exception as e:
        print(f"❌ Failed to start Kafka output stream: {e}")
        console_q.stop()
        return

    print(f"🚀 Impact scoring job started successfully!")
    print(f"📊 Console output: Enabled")
    print(f"📤 Kafka output: {topic_out}")
    print(f"💾 Checkpoint: {checkpoint_uri}")
    print("\nWaiting for data... (Press Ctrl+C to stop)")

    # Wait for termination
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping streams...")
        console_q.stop()
        kafka_q.stop()
        spark.stop()
        print("✅ Stopped successfully")

if __name__ == "__main__":
    main()