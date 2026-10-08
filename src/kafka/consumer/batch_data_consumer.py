import json 
import logging
import os
from datetime import datetime

import pandas as pd 
import numpy as np

from confluent_kafka import Consumer
from minio import Minio
from minio.error import S3Error

from dotenv import load_dotenv

load_dotenv()

#Configure Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_BATCH = os.getenv('KAFKA_TOPIC_BATCH')
# KAFKA_GROUP_ID = os.getenv('KAFAK_GROUP_BATCH_ID')
KAFKA_GROUP_ID =  "stock-market-consumer-group-batch"

#MinIO configuration
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')

def create_minio_client():
    """Initialize MinIO Client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def ensure_bucket_exists(minio_client, bucket_name):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created bucket {bucket_name}")
        else:
            logger.info(f"Bucket {bucket_name} already exists")
    except S3Error as e:
        logger.error(f"Error creating bucket {bucket_name}: {e}")
        raise

def main():
    # Create a MinIO client
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, MINIO_BUCKET)

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_BATCH])

    logger.info(f"Starting consumer topic {KAFKA_TOPIC_BATCH}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            try:
                data = json.loads(msg.value().decode("utf-8"))
                print(data)
                symbol = data['symbol']
                # date = data['batch_date']
                trade_date = data['date']
                year, month, day = trade_date.split("-")

                df = pd.DataFrame([data])

                #Save to minio

                object_name = f"raw/historical/symbol={symbol}/year={year}/month={month}/{symbol}_{trade_date}.csv"
                csv_file = f"/tmp/{symbol}.csv"
                df.to_csv(csv_file, index=False)

                minio_client.fput_object(
                    MINIO_BUCKET,
                    object_name,
                    csv_file,
                )
                logger.info(f"Wrote data for {symbol} to s3://{MINIO_BUCKET}/{object_name}")

                os.remove(csv_file)
                consumer.commit(asynchronous=False)
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    except KeyboardInterrupt:
        logger.info("Stopping consumer")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()