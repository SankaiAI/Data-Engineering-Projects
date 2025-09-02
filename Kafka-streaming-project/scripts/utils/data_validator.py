from pydantic import BaseModel, Field, EmailStr, validator
from typing import Optional, Dict, Any
from datetime import datetime
import json

class MarketoLead(BaseModel):
    id: str
    email: EmailStr
    firstName: str
    lastName: str
    company: Optional[str] = None
    jobTitle: Optional[str] = None
    industry: Optional[str] = None
    leadSource: Optional[str] = None
    leadScore: Optional[int] = Field(None, ge=0, le=100)
    phone: Optional[str] = None
    country: Optional[str] = None
    state: Optional[str] = None
    city: Optional[str] = None
    createdAt: str
    updatedAt: str
    
    @validator('createdAt', 'updatedAt')
    def validate_datetime(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError('Invalid datetime format')

class MarketoActivity(BaseModel):
    id: str
    leadId: str
    activityType: str
    activityDate: str
    campaignId: Optional[str] = None
    campaignName: Optional[str] = None
    assetName: Optional[str] = None
    webPageUrl: Optional[str] = None
    emailId: Optional[str] = None
    emailSubject: Optional[str] = None
    score: Optional[int] = Field(None, ge=0)
    
    @validator('activityDate')
    def validate_datetime(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError('Invalid datetime format')

class MarketoOpportunity(BaseModel):
    id: str
    leadId: str
    opportunityName: str
    stage: str
    amount: int = Field(ge=0)
    closeDate: str
    probability: int = Field(ge=0, le=100)
    createdAt: str
    updatedAt: str
    
    @validator('closeDate', 'createdAt', 'updatedAt')
    def validate_datetime(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError('Invalid datetime format')

class DataValidator:
    @staticmethod
    def validate_lead(data: Dict[str, Any]) -> Optional[MarketoLead]:
        try:
            return MarketoLead(**data)
        except Exception as e:
            print(f"Lead validation error: {e}")
            return None
    
    @staticmethod
    def validate_activity(data: Dict[str, Any]) -> Optional[MarketoActivity]:
        try:
            return MarketoActivity(**data)
        except Exception as e:
            print(f"Activity validation error: {e}")
            return None
    
    @staticmethod
    def validate_opportunity(data: Dict[str, Any]) -> Optional[MarketoOpportunity]:
        try:
            return MarketoOpportunity(**data)
        except Exception as e:
            print(f"Opportunity validation error: {e}")
            return None
    
    @staticmethod
    def validate_json_message(message: str) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(message)
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            return None