import json
import sys
import os
from typing import Dict, Any, List
from kafka import KafkaConsumer
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import Config
from utils.logger import setup_logger

class SnowflakeConsumer:
    def __init__(self):
        self.config = Config()
        self.logger = setup_logger("snowflake_consumer")
        
        # Initialize Snowflake connection
        self.snowflake_conn = None
        self._initialize_snowflake()
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            self.config.PROCESSED_LEADS_TOPIC,
            self.config.PROCESSED_ACTIVITIES_TOPIC,
            self.config.SNOWFLAKE_SINK_TOPIC,
            **self.config.get_kafka_consumer_config("snowflake_consumer_group")
        )
        
        # Batch storage
        self.batch_data = {
            'leads': [],
            'activities': [],
            'opportunities': []
        }
        
        self.batch_size = self.config.BATCH_SIZE
        self.logger.info("Snowflake consumer initialized successfully")
    
    def _initialize_snowflake(self):
        try:
            self.snowflake_conn = snowflake.connector.connect(
                **self.config.get_snowflake_config()
            )
            
            # Create database and schema if they don't exist
            self._setup_snowflake_schema()
            self.logger.info("Snowflake connection established")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            self.snowflake_conn = None
    
    def _setup_snowflake_schema(self):
        if not self.snowflake_conn:
            return
        
        cursor = self.snowflake_conn.cursor()
        
        try:
            # Create database and schema
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.config.SNOWFLAKE_DATABASE}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config.SNOWFLAKE_DATABASE}.{self.config.SNOWFLAKE_SCHEMA}")
            cursor.execute(f"USE SCHEMA {self.config.SNOWFLAKE_DATABASE}.{self.config.SNOWFLAKE_SCHEMA}")
            
            # Create tables
            self._create_leads_table(cursor)
            self._create_activities_table(cursor)
            self._create_opportunities_table(cursor)
            
            self.logger.info("Snowflake schema setup completed")
            
        except Exception as e:
            self.logger.error(f"Error setting up Snowflake schema: {e}")
        finally:
            cursor.close()
    
    def _create_leads_table(self, cursor):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS leads (
            id VARCHAR(255) PRIMARY KEY,
            email VARCHAR(255),
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            company VARCHAR(255),
            job_title VARCHAR(255),
            industry VARCHAR(255),
            lead_source VARCHAR(255),
            lead_score INTEGER,
            enriched_score INTEGER,
            lead_grade VARCHAR(1),
            phone VARCHAR(50),
            country VARCHAR(10),
            state VARCHAR(10),
            city VARCHAR(255),
            created_at TIMESTAMP_TZ,
            updated_at TIMESTAMP_TZ,
            processed_at TIMESTAMP_TZ,
            inserted_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cursor.execute(create_table_sql)
    
    def _create_activities_table(self, cursor):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS activities (
            id VARCHAR(255) PRIMARY KEY,
            lead_id VARCHAR(255),
            activity_type VARCHAR(255),
            activity_date TIMESTAMP_TZ,
            campaign_id VARCHAR(255),
            campaign_name VARCHAR(255),
            asset_name VARCHAR(255),
            web_page_url VARCHAR(500),
            email_id VARCHAR(255),
            email_subject VARCHAR(500),
            score INTEGER,
            activity_score INTEGER,
            lead_company VARCHAR(255),
            lead_industry VARCHAR(255),
            processed_at TIMESTAMP_TZ,
            inserted_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        )
        """
        cursor.execute(create_table_sql)
    
    def _create_opportunities_table(self, cursor):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS opportunities (
            id VARCHAR(255) PRIMARY KEY,
            lead_id VARCHAR(255),
            opportunity_name VARCHAR(255),
            stage VARCHAR(255),
            amount INTEGER,
            close_date TIMESTAMP_TZ,
            probability INTEGER,
            created_at TIMESTAMP_TZ,
            updated_at TIMESTAMP_TZ,
            processed_at TIMESTAMP_TZ,
            inserted_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        )
        """
        cursor.execute(create_table_sql)
    
    def _transform_leads_data(self, leads_data: List[Dict[str, Any]]) -> pd.DataFrame:
        df = pd.json_normalize(leads_data)
        
        # Rename columns to match Snowflake schema
        column_mapping = {
            'firstName': 'first_name',
            'lastName': 'last_name',
            'jobTitle': 'job_title',
            'leadSource': 'lead_source',
            'leadScore': 'lead_score',
            'enrichedScore': 'enriched_score',
            'leadGrade': 'lead_grade',
            'createdAt': 'created_at',
            'updatedAt': 'updated_at',
            'processedAt': 'processed_at'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Convert timestamp columns
        timestamp_cols = ['created_at', 'updated_at', 'processed_at']
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True)
        
        return df
    
    def _transform_activities_data(self, activities_data: List[Dict[str, Any]]) -> pd.DataFrame:
        df = pd.json_normalize(activities_data)
        
        # Rename columns to match Snowflake schema
        column_mapping = {
            'leadId': 'lead_id',
            'activityType': 'activity_type',
            'activityDate': 'activity_date',
            'campaignId': 'campaign_id',
            'campaignName': 'campaign_name',
            'assetName': 'asset_name',
            'webPageUrl': 'web_page_url',
            'emailId': 'email_id',
            'emailSubject': 'email_subject',
            'activityScore': 'activity_score',
            'leadCompany': 'lead_company',
            'leadIndustry': 'lead_industry',
            'processedAt': 'processed_at'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Convert timestamp columns
        timestamp_cols = ['activity_date', 'processed_at']
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True)
        
        return df
    
    def _transform_opportunities_data(self, opportunities_data: List[Dict[str, Any]]) -> pd.DataFrame:
        df = pd.json_normalize(opportunities_data)
        
        # Rename columns to match Snowflake schema
        column_mapping = {
            'leadId': 'lead_id',
            'opportunityName': 'opportunity_name',
            'closeDate': 'close_date',
            'createdAt': 'created_at',
            'updatedAt': 'updated_at',
            'processedAt': 'processed_at'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Convert timestamp columns
        timestamp_cols = ['close_date', 'created_at', 'updated_at', 'processed_at']
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True)
        
        return df
    
    def load_to_snowflake(self, data_type: str, batch_data: List[Dict[str, Any]]) -> bool:
        if not batch_data or not self.snowflake_conn:
            return True
        
        try:
            # Transform data based on type
            if data_type == 'leads':
                df = self._transform_leads_data(batch_data)
                table_name = 'leads'
            elif data_type == 'activities':
                df = self._transform_activities_data(batch_data)
                table_name = 'activities'
            elif data_type == 'opportunities':
                df = self._transform_opportunities_data(batch_data)
                table_name = 'opportunities'
            else:
                self.logger.error(f"Unknown data type: {data_type}")
                return False
            
            # Write to Snowflake using MERGE for upsert
            success, nchunks, nrows, _ = write_pandas(
                self.snowflake_conn,
                df,
                table_name,
                database=self.config.SNOWFLAKE_DATABASE,
                schema=self.config.SNOWFLAKE_SCHEMA,
                quote_identifiers=False,
                auto_create_table=False
            )
            
            if success:
                self.logger.info(f"Successfully loaded {nrows} {data_type} records to Snowflake")
                return True
            else:
                self.logger.error(f"Failed to load {data_type} data to Snowflake")
                return False
                
        except Exception as e:
            self.logger.error(f"Error loading {data_type} to Snowflake: {e}")
            return False
    
    def flush_batches(self):
        for data_type, batch in self.batch_data.items():
            if batch:
                self.load_to_snowflake(data_type, batch)
                self.batch_data[data_type] = []
    
    def start_consuming(self):
        self.logger.info("Starting Snowflake consumer...")
        
        if not self.snowflake_conn:
            self.logger.error("No Snowflake connection available")
            return
        
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
                    elif message.topic == self.config.SNOWFLAKE_SINK_TOPIC:
                        # Assume opportunities for now
                        self.batch_data['opportunities'].append(data)
                    
                    # Check if any batch is ready for processing
                    for data_type, batch in self.batch_data.items():
                        if len(batch) >= self.batch_size:
                            self.load_to_snowflake(data_type, batch)
                            self.batch_data[data_type] = []
                    
                    # Commit offset after successful processing
                    self.consumer.commit()
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON decode error: {e}")
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    
        except KeyboardInterrupt:
            self.logger.info("Snowflake consumer stopped by user")
        finally:
            # Flush remaining batches
            self.flush_batches()
            self.close()
    
    def close(self):
        if hasattr(self, 'consumer'):
            self.consumer.close()
        if hasattr(self, 'snowflake_conn') and self.snowflake_conn:
            self.snowflake_conn.close()
        self.logger.info("Snowflake consumer closed")

if __name__ == "__main__":
    consumer = SnowflakeConsumer()
    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        print("Shutting down Snowflake consumer...")
    finally:
        consumer.close()