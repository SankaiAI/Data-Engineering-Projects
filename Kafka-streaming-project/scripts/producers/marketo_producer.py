import json
import time
import sys
import os
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Add both scripts directory and project root to path
scripts_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(scripts_dir)
sys.path.append(scripts_dir)
sys.path.append(project_root)

from config.config import Config
from utils.logger import setup_logger
from utils.data_validator import DataValidator
from dataSource.marketo_data_generator import MarketoDataGenerator

class MarketoKafkaProducer:
    def __init__(self):
        self.config = Config()
        self.logger = setup_logger("marketo_producer")
        self.data_generator = MarketoDataGenerator()
        self.validator = DataValidator()
        
        try:
            self.producer = KafkaProducer(**self.config.get_kafka_producer_config())
            self.logger.info("Kafka producer initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def send_message(self, topic: str, key: str, value: Dict[str, Any]) -> bool:
        try:
            message = json.dumps(value)
            future = self.producer.send(topic, key=key, value=message)
            record_metadata = future.get(timeout=10)
            
            self.logger.info(
                "Message sent successfully",
                topic=record_metadata.topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset
            )
            return True
            
        except KafkaError as e:
            self.logger.error(f"Kafka error while sending message: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error while sending message: {e}")
            return False
    
    def process_lead(self, lead_data: Dict[str, Any]) -> bool:
        validated_lead = self.validator.validate_lead(lead_data)
        if not validated_lead:
            self.logger.warning("Lead validation failed", lead_id=lead_data.get('id'))
            return False
        
        return self.send_message(
            topic=self.config.MARKETO_LEADS_TOPIC,
            key=validated_lead.id,
            value=validated_lead.model_dump()
        )
    
    def process_activity(self, activity_data: Dict[str, Any]) -> bool:
        validated_activity = self.validator.validate_activity(activity_data)
        if not validated_activity:
            self.logger.warning("Activity validation failed", activity_id=activity_data.get('id'))
            return False
        
        return self.send_message(
            topic=self.config.MARKETO_ACTIVITIES_TOPIC,
            key=validated_activity.id,
            value=validated_activity.model_dump()
        )
    
    def process_opportunity(self, opportunity_data: Dict[str, Any]) -> bool:
        validated_opportunity = self.validator.validate_opportunity(opportunity_data)
        if not validated_opportunity:
            self.logger.warning("Opportunity validation failed", opportunity_id=opportunity_data.get('id'))
            return False
        
        return self.send_message(
            topic=self.config.MARKETO_OPPORTUNITIES_TOPIC,
            key=validated_opportunity.id,
            value=validated_opportunity.model_dump()
        )
    
    def start_streaming(self, interval_seconds: int = 5):
        self.logger.info(f"Starting Marketo data streaming every {interval_seconds} seconds")
        
        metrics = {
            'leads_sent': 0,
            'activities_sent': 0,
            'opportunities_sent': 0,
            'errors': 0
        }
        
        try:
            while True:
                batch = self.data_generator.generate_batch()
                batch_metrics = {'leads': 0, 'activities': 0, 'opportunities': 0, 'errors': 0}
                
                for record in batch:
                    try:
                        data_type = record.get('type')
                        data = record.get('data')
                        
                        success = False
                        if data_type == 'lead':
                            success = self.process_lead(data)
                            if success:
                                batch_metrics['leads'] += 1
                                metrics['leads_sent'] += 1
                        elif data_type == 'activity':
                            success = self.process_activity(data)
                            if success:
                                batch_metrics['activities'] += 1
                                metrics['activities_sent'] += 1
                        elif data_type == 'opportunity':
                            success = self.process_opportunity(data)
                            if success:
                                batch_metrics['opportunities'] += 1
                                metrics['opportunities_sent'] += 1
                        
                        if not success:
                            batch_metrics['errors'] += 1
                            metrics['errors'] += 1
                            
                    except Exception as e:
                        self.logger.error(f"Error processing record: {e}")
                        batch_metrics['errors'] += 1
                        metrics['errors'] += 1
                
                self.producer.flush()
                
                self.logger.info(
                    "Batch processed",
                    batch_size=len(batch),
                    **batch_metrics,
                    total_metrics=metrics
                )
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            self.logger.info("Streaming stopped by user")
        except Exception as e:
            self.logger.error(f"Streaming error: {e}")
        finally:
            self.close()
    
    def close(self):
        if hasattr(self, 'producer'):
            self.producer.close()
            self.logger.info("Kafka producer closed")

if __name__ == "__main__":
    producer = MarketoKafkaProducer()
    try:
        producer.start_streaming(interval_seconds=5)
    except KeyboardInterrupt:
        print("Shutting down producer...")
    finally:
        producer.close()