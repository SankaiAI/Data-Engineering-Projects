import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List
import time

class MarketoDataGenerator:
    def __init__(self):
        self.companies = ["TechCorp", "DataInc", "CloudSoft", "AITech", "StreamCorp", "AnalyticsPro", "DevOps Inc", "SoftwareLab"]
        self.industries = ["Technology", "Healthcare", "Finance", "Retail", "Manufacturing", "Education", "Government"]
        self.lead_sources = ["Website", "Social Media", "Email Campaign", "Webinar", "Trade Show", "Referral", "Organic Search", "Paid Ads"]
        self.email_domains = ["gmail.com", "yahoo.com", "company.com", "business.org", "enterprise.net"]
        
    def generate_lead(self) -> Dict:
        return {
            "id": str(uuid.uuid4()),
            "email": f"user{random.randint(1000, 9999)}@{random.choice(self.email_domains)}",
            "firstName": random.choice(["John", "Jane", "Mike", "Sarah", "David", "Lisa", "Chris", "Amanda"]),
            "lastName": random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]),
            "company": random.choice(self.companies),
            "jobTitle": random.choice(["Manager", "Director", "VP", "Analyst", "Specialist", "Coordinator"]),
            "industry": random.choice(self.industries),
            "leadSource": random.choice(self.lead_sources),
            "leadScore": random.randint(1, 100),
            "phone": f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
            "country": random.choice(["US", "CA", "UK", "DE", "FR", "AU"]),
            "state": random.choice(["CA", "NY", "TX", "FL", "IL", "PA", "OH", "MI"]),
            "city": random.choice(["San Francisco", "New York", "Austin", "Miami", "Chicago", "Philadelphia"]),
            "createdAt": datetime.utcnow().isoformat() + "Z",
            "updatedAt": datetime.utcnow().isoformat() + "Z"
        }
    
    def generate_activity(self, lead_id: str) -> Dict:
        activity_types = ["Email Opened", "Email Clicked", "Web Page Visited", "Form Filled", "Downloaded Asset", "Webinar Attended"]
        
        return {
            "id": str(uuid.uuid4()),
            "leadId": lead_id,
            "activityType": random.choice(activity_types),
            "activityDate": datetime.utcnow().isoformat() + "Z",
            "campaignId": str(uuid.uuid4()),
            "campaignName": f"Campaign-{random.randint(100, 999)}",
            "assetName": f"Asset-{random.randint(100, 999)}",
            "webPageUrl": f"https://company.com/page-{random.randint(1, 50)}",
            "emailId": str(uuid.uuid4()),
            "emailSubject": f"Subject Line {random.randint(1, 100)}",
            "score": random.randint(1, 10)
        }
    
    def generate_opportunity(self, lead_id: str) -> Dict:
        stages = ["Qualification", "Needs Analysis", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
        
        return {
            "id": str(uuid.uuid4()),
            "leadId": lead_id,
            "opportunityName": f"Opportunity-{random.randint(1000, 9999)}",
            "stage": random.choice(stages),
            "amount": random.randint(10000, 1000000),
            "closeDate": (datetime.utcnow() + timedelta(days=random.randint(30, 180))).isoformat() + "Z",
            "probability": random.randint(10, 90),
            "createdAt": datetime.utcnow().isoformat() + "Z",
            "updatedAt": datetime.utcnow().isoformat() + "Z"
        }
    
    def generate_batch(self, num_leads: int = 10) -> List[Dict]:
        batch = []
        
        for _ in range(num_leads):
            lead = self.generate_lead()
            batch.append({"type": "lead", "data": lead})
            
            num_activities = random.randint(1, 5)
            for _ in range(num_activities):
                activity = self.generate_activity(lead["id"])
                batch.append({"type": "activity", "data": activity})
            
            if random.random() < 0.3:
                opportunity = self.generate_opportunity(lead["id"])
                batch.append({"type": "opportunity", "data": opportunity})
        
        return batch
    
    def generate_streaming_data(self, interval_seconds: int = 5):
        print(f"Starting Marketo data generation every {interval_seconds} seconds...")
        while True:
            batch = self.generate_batch(random.randint(5, 15))
            for record in batch:
                print(json.dumps(record))
                time.sleep(0.1)
            time.sleep(interval_seconds)

if __name__ == "__main__":
    generator = MarketoDataGenerator()
    generator.generate_streaming_data()