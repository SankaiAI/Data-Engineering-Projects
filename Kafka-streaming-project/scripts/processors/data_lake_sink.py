import json
import sys
import os
from typing import Dict, Any
from kafka import KafkaConsumer
import pandas as pd
from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error
import io

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import Config
from utils.logger import setup_logger

class DataLakeSink:
    def __init__(self):
        self.config = Config()
        self.logger = setup_logger("data_lake_sink")
        
        # Initialize MinIO client
        self.minio_client = Minio(
            self.config.MINIO_ENDPOINT,
            access_key=self.config.MINIO_ACCESS_KEY,
            secret_key=self.config.MINIO_SECRET_KEY,
            secure=False
        )
        
        # Create bucket if it doesn't exist
        try:
            if not self.minio_client.bucket_exists(self.config.S3_BUCKET_NAME):
                self.minio_client.make_bucket(self.config.S3_BUCKET_NAME)
                self.logger.info(f"Created bucket: {self.config.S3_BUCKET_NAME}")
        except S3Error as e:
            self.logger.error(f"Error creating bucket: {e}")
            raise
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            self.config.PROCESSED_LEADS_TOPIC,
            self.config.PROCESSED_ACTIVITIES_TOPIC,
            **self.config.get_kafka_consumer_config("data_lake_sink_group")
        )
        
        # Batch storage
        self.batch_data = {
            'leads': [],
            'activities': []
        }
        
        self.batch_size = self.config.BATCH_SIZE
        self.logger.info("Data lake sink initialized successfully")
    
    def upload_to_s3(self, data: pd.DataFrame, object_name: str) -> bool:
        try:
            # Convert DataFrame to Parquet
            parquet_buffer = io.BytesIO()
            data.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            # Upload to MinIO/S3
            self.minio_client.put_object(
                self.config.S3_BUCKET_NAME,
                object_name,
                parquet_buffer,
                length=len(parquet_buffer.getvalue()),
                content_type='application/octet-stream'
            )
            
            self.logger.info(f"Uploaded {object_name} to S3", records=len(data))
            return True
            
        except Exception as e:
            self.logger.error(f"Error uploading {object_name} to S3: {e}")
            return False
    
    def process_batch(self, data_type: str, batch_data: list) -> bool:
        if not batch_data:
            return True
        
        try:
            # Convert to DataFrame
            df = pd.json_normalize(batch_data)
            
            # Generate S3 object name with partitioning
            current_time = datetime.utcnow()
            object_name = f"raw_data/{data_type}/year={current_time.year}/month={current_time.month:02d}/day={current_time.day:02d}/hour={current_time.hour:02d}/{data_type}_{current_time.strftime('%Y%m%d_%H%M%S')}.parquet"
            
            return self.upload_to_s3(df, object_name)
            
        except Exception as e:
            self.logger.error(f"Error processing batch for {data_type}: {e}")
            return False
    
    def flush_batches(self):
        for data_type, batch in self.batch_data.items():
            if batch:
                self.process_batch(data_type, batch)
                self.batch_data[data_type] = []
    
    def start_sinking(self):
        self.logger.info("Starting data lake sink...")
        
        try:
            for message in self.consumer:
                try:
                    # Parse message
                    data = json.loads(message.value)
                    
                    # Add to appropriate batch
                    if message.topic == self.config.PROCESSED_LEADS_TOPIC:
                        self.batch_data['leads'].append(data)
                    elif message.topic == self.config.PROCESSED_ACTIVITIES_TOPIC:
                        self.batch_data['activities'].append(data)
                    
                    # Check if any batch is ready for processing
                    for data_type, batch in self.batch_data.items():
                        if len(batch) >= self.batch_size:
                            self.process_batch(data_type, batch)
                            self.batch_data[data_type] = []
                    
                    # Commit offset after successful processing
                    self.consumer.commit()
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON decode error: {e}")
                except Exception as e:
                    self.logger.error(f"Sink processing error: {e}")
                    
        except KeyboardInterrupt:
            self.logger.info("Data lake sink stopped by user")
        finally:
            # Flush remaining batches
            self.flush_batches()
            self.close()
    
    def close(self):
        if hasattr(self, 'consumer'):
            self.consumer.close()
        self.logger.info("Data lake sink closed")

if __name__ == "__main__":
    sink = DataLakeSink()
    try:
        sink.start_sinking()
    except KeyboardInterrupt:
        print("Shutting down data lake sink...")
    finally:
        sink.close()