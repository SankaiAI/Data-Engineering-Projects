import os
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
    
    # Kafka Topics
    MARKETO_LEADS_TOPIC = 'marketo.leads'
    MARKETO_ACTIVITIES_TOPIC = 'marketo.activities' 
    MARKETO_OPPORTUNITIES_TOPIC = 'marketo.opportunities'
    PROCESSED_LEADS_TOPIC = 'processed.leads'
    PROCESSED_ACTIVITIES_TOPIC = 'processed.activities'
    SNOWFLAKE_SINK_TOPIC = 'snowflake.sink'
    
    # MinIO/S3 Configuration
    MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'marketo-data-lake')
    
    # Redis Configuration
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
    
    # Snowflake Configuration
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT', '')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER', '')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD', '')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'MARKETO_DB')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'RAW_DATA')
    
    # Application Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
    PROCESSING_INTERVAL = int(os.getenv('PROCESSING_INTERVAL', '30'))
    
    @classmethod
    def get_kafka_producer_config(cls) -> Dict[str, Any]:
        return {
            'bootstrap_servers': cls.KAFKA_BOOTSTRAP_SERVERS,
            'value_serializer': lambda x: x.encode('utf-8') if isinstance(x, str) else x,
            'key_serializer': lambda x: x.encode('utf-8') if isinstance(x, str) else x,
            'acks': 'all',
            'retries': 3,
            'max_in_flight_requests_per_connection': 1
        }
    
    @classmethod
    def get_kafka_consumer_config(cls, group_id: str) -> Dict[str, Any]:
        return {
            'bootstrap_servers': cls.KAFKA_BOOTSTRAP_SERVERS,
            'group_id': group_id,
            'value_deserializer': lambda x: x.decode('utf-8'),
            'key_deserializer': lambda x: x.decode('utf-8') if x else None,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,
            'max_poll_records': cls.BATCH_SIZE
        }
    
    @classmethod
    def get_snowflake_config(cls) -> Dict[str, Any]:
        return {
            'account': cls.SNOWFLAKE_ACCOUNT,
            'user': cls.SNOWFLAKE_USER,
            'password': cls.SNOWFLAKE_PASSWORD,
            'warehouse': cls.SNOWFLAKE_WAREHOUSE,
            'database': cls.SNOWFLAKE_DATABASE,
            'schema': cls.SNOWFLAKE_SCHEMA
        }