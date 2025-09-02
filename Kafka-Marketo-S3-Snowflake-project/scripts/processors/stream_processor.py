import json
import sys
import os
from typing import Dict, Any, Optional
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
from datetime import datetime
import redis

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import Config
from utils.logger import setup_logger
from utils.data_validator import DataValidator

class StreamProcessor:
    def __init__(self):
        self.config = Config()
        self.logger = setup_logger("stream_processor")
        self.validator = DataValidator()
        
        # Initialize Redis for deduplication and caching
        self.redis_client = redis.Redis(
            host=self.config.REDIS_HOST,
            port=self.config.REDIS_PORT,
            decode_responses=True
        )
        
        # Initialize Kafka consumer and producer
        self.consumer = KafkaConsumer(
            self.config.MARKETO_LEADS_TOPIC,
            self.config.MARKETO_ACTIVITIES_TOPIC,
            self.config.MARKETO_OPPORTUNITIES_TOPIC,
            **self.config.get_kafka_consumer_config("stream_processor_group")
        )
        
        self.producer = KafkaProducer(**self.config.get_kafka_producer_config())
        
        self.logger.info("Stream processor initialized successfully")
    
    def deduplicate_record(self, record_id: str, record_type: str) -> bool:
        cache_key = f"{record_type}:{record_id}"
        
        if self.redis_client.exists(cache_key):
            self.logger.debug(f"Duplicate record detected: {cache_key}")
            return False
        
        # Store record ID with 24-hour expiration
        self.redis_client.setex(cache_key, 86400, "1")
        return True
    
    def enrich_lead_data(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        enriched = lead_data.copy()
        
        # Add processing timestamp
        enriched['processedAt'] = datetime.utcnow().isoformat() + "Z"
        
        # Lead scoring logic
        score = 0
        if lead_data.get('company'):
            score += 10
        if lead_data.get('jobTitle'):
            if any(title in lead_data['jobTitle'].lower() for title in ['vp', 'director', 'manager']):
                score += 15
        if lead_data.get('leadSource') == 'Website':
            score += 5
        
        enriched['enrichedScore'] = score
        
        # Add lead grade based on score
        if score >= 20:
            enriched['leadGrade'] = 'A'
        elif score >= 10:
            enriched['leadGrade'] = 'B'
        else:
            enriched['leadGrade'] = 'C'
        
        return enriched
    
    def enrich_activity_data(self, activity_data: Dict[str, Any]) -> Dict[str, Any]:
        enriched = activity_data.copy()
        enriched['processedAt'] = datetime.utcnow().isoformat() + "Z"
        
        # Activity scoring
        activity_scores = {
            'Email Opened': 1,
            'Email Clicked': 3,
            'Web Page Visited': 2,
            'Form Filled': 5,
            'Downloaded Asset': 4,
            'Webinar Attended': 6
        }
        
        enriched['activityScore'] = activity_scores.get(activity_data.get('activityType'), 0)
        
        # Get lead information from cache if available
        lead_cache_key = f"lead:{activity_data.get('leadId')}"
        cached_lead = self.redis_client.get(lead_cache_key)
        if cached_lead:
            lead_info = json.loads(cached_lead)
            enriched['leadCompany'] = lead_info.get('company')
            enriched['leadIndustry'] = lead_info.get('industry')
        
        return enriched
    
    def aggregate_lead_activities(self, lead_id: str) -> Dict[str, Any]:
        try:
            # Get recent activities for this lead from cache
            activities_key = f"activities:{lead_id}"
            activities_data = self.redis_client.get(activities_key)
            
            if activities_data:
                activities = json.loads(activities_data)
                
                aggregation = {
                    'leadId': lead_id,
                    'totalActivities': len(activities),
                    'totalScore': sum(act.get('activityScore', 0) for act in activities),
                    'lastActivityDate': max(act.get('activityDate') for act in activities),
                    'activityTypes': list(set(act.get('activityType') for act in activities)),
                    'aggregatedAt': datetime.utcnow().isoformat() + "Z"
                }
                
                return aggregation
            
        except Exception as e:
            self.logger.error(f"Error aggregating activities for lead {lead_id}: {e}")
        
        return {}
    
    def process_lead(self, lead_data: Dict[str, Any]) -> bool:
        try:
            # Deduplicate
            if not self.deduplicate_record(lead_data['id'], 'lead'):
                return True
            
            # Enrich data
            enriched_lead = self.enrich_lead_data(lead_data)
            
            # Cache lead data for activity enrichment
            lead_cache_key = f"lead:{lead_data['id']}"
            self.redis_client.setex(lead_cache_key, 3600, json.dumps(lead_data))
            
            # Send to processed topic
            self.producer.send(
                self.config.PROCESSED_LEADS_TOPIC,
                key=enriched_lead['id'],
                value=json.dumps(enriched_lead)
            )
            
            self.logger.info(f"Processed lead: {lead_data['id']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing lead {lead_data.get('id')}: {e}")
            return False
    
    def process_activity(self, activity_data: Dict[str, Any]) -> bool:
        try:
            # Deduplicate
            if not self.deduplicate_record(activity_data['id'], 'activity'):
                return True
            
            # Enrich data
            enriched_activity = self.enrich_activity_data(activity_data)
            
            # Update activities cache for aggregation
            activities_key = f"activities:{activity_data['leadId']}"
            cached_activities = self.redis_client.get(activities_key)
            
            if cached_activities:
                activities = json.loads(cached_activities)
            else:
                activities = []
            
            activities.append(enriched_activity)
            
            # Keep only last 100 activities per lead
            if len(activities) > 100:
                activities = activities[-100:]
            
            self.redis_client.setex(activities_key, 7200, json.dumps(activities))
            
            # Send to processed topic
            self.producer.send(
                self.config.PROCESSED_ACTIVITIES_TOPIC,
                key=enriched_activity['id'],
                value=json.dumps(enriched_activity)
            )
            
            self.logger.info(f"Processed activity: {activity_data['id']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing activity {activity_data.get('id')}: {e}")
            return False
    
    def start_processing(self):
        self.logger.info("Starting stream processing...")
        
        try:
            for message in self.consumer:
                try:
                    # Parse message
                    data = json.loads(message.value)
                    
                    if message.topic == self.config.MARKETO_LEADS_TOPIC:
                        self.process_lead(data)
                    elif message.topic == self.config.MARKETO_ACTIVITIES_TOPIC:
                        self.process_activity(data)
                    elif message.topic == self.config.MARKETO_OPPORTUNITIES_TOPIC:
                        # Process opportunities (simplified for now)
                        self.producer.send(
                            self.config.SNOWFLAKE_SINK_TOPIC,
                            key=data['id'],
                            value=json.dumps({**data, 'processedAt': datetime.utcnow().isoformat() + "Z"})
                        )
                    
                    # Commit offset after successful processing
                    self.consumer.commit()
                    
                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON decode error: {e}")
                except Exception as e:
                    self.logger.error(f"Processing error: {e}")
                    
        except KeyboardInterrupt:
            self.logger.info("Processing stopped by user")
        finally:
            self.close()
    
    def close(self):
        if hasattr(self, 'consumer'):
            self.consumer.close()
        if hasattr(self, 'producer'):
            self.producer.close()
        if hasattr(self, 'redis_client'):
            self.redis_client.close()
        self.logger.info("Stream processor closed")

if __name__ == "__main__":
    processor = StreamProcessor()
    try:
        processor.start_processing()
    except KeyboardInterrupt:
        print("Shutting down processor...")
    finally:
        processor.close()