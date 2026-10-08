import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

# MinIO configuration
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")      # localhost:9000 on your Mac, minio:9000 in Docker

RAW_BASE = f"s3a://{MINIO_BUCKET}/raw/historical/"
OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/processed/historical/"


def create_spark_session():
    """Create a Spark session configured to talk to MinIO through S3A."""
    spark = (
        SparkSession.builder
        .appName("StockMarketBatchProcessor")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized successfully")
    return spark


def read_data_from_s3(spark, date=None):
    """Read raw CSVs.

    Layout: raw/historical/symbol=AAPL/year=2025/month=10/AAPL_2025-10-03.csv
    date=None reads everything; "YYYY-MM-DD" reads that trading day only.
    """
    logger.info("Reading data from S3")

    if date is None:
        path = RAW_BASE
    else:
        d = datetime.strptime(date, "%Y-%m-%d")
        path = f"{RAW_BASE}symbol=*/year={d.year}/month={d.month:02d}/*_{date}.csv"

    logger.info(f"Reading data from: {path}")

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("basePath", RAW_BASE)      # keeps symbol/year/month as columns
            .csv(path)
        )
        df.show(5, truncate=False)             # console output, can't go through the logger
        df.printSchema()
        return df
    except Exception:
        logger.exception("Error reading data from S3")
        return None


def process_stock_data(df):
    """Add daily change, daily return and a 7-day moving average per symbol."""
    logger.info("Processing historical stock data")

    if df is None or df.isEmpty():
        logger.warning("No data to process")
        return None

    try:
        w = Window.partitionBy("symbol").orderBy("date")

        df = (
            df
            .withColumn("daily_change_pct",
                        (F.col("close") - F.col("open")) / F.col("open") * 100)
            .withColumn("prev_close", F.lag("close").over(w))
            .withColumn("daily_return_pct",
                        (F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100)
            .withColumn("ma_7", F.avg("close").over(w.rowsBetween(-6, 0)))
        )

        df.select("symbol", "date", "open", "close",
                  "daily_change_pct", "daily_return_pct", "ma_7").show(5)
        return df

    except Exception:
        logger.exception("Error processing data")
        return None


def write_to_s3(spark, df):
    """Write processed data as Parquet, one folder per symbol."""
    logger.info("Writing processed data to S3")

    if df is None:
        logger.warning("No data to write")
        return False

    logger.info(f"Writing processed data to: {OUTPUT_PATH}")

    try:
        out = spark.read.parquet(OUTPUT_PATH)
        out.groupBy("symbol").count().show()
        df.write.partitionBy("symbol").mode("overwrite").parquet(OUTPUT_PATH)
        logger.info(f"Data written to: {OUTPUT_PATH}")
        return True
    except Exception:
        logger.exception("Error writing to S3")
        return False


def main():
    """Main function to process historical data."""
    logger.info("STARTING STOCK MARKET BATCH PROCESSOR")

    date = None      # None = process all dates (a single date would break lag/ma_7 and the overwrite)

    spark = create_spark_session()

    try:
        df = read_data_from_s3(spark, date)

        if df is None:
            logger.error("Failed to read data from S3")
            return

        processed_df = process_stock_data(df)

        if processed_df is None:
            logger.error("Error processing data")
            return

        write_to_s3(spark, processed_df)

    except Exception:
        logger.exception("Unexpected error in batch processor")
    finally:
        logger.info("Stopping Spark session")
        spark.stop()
        logger.info("BATCH PROCESSING COMPLETE")


if __name__ == "__main__":
    main()