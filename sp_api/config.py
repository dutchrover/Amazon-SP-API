import os
from typing import Dict, Any
from dotenv import load_dotenv
import json
from sp_api.exceptions import ValidationError
from dataclasses import dataclass

@dataclass
class SPAPIConfig:
    LWA_CLIENT_ID: str
    LWA_CLIENT_SECRET: str
    REFRESH_TOKEN: str
    AWS_REGION: str
    MARKETPLACE_ID: str
    SP_API_BASE_URL: str
    LWA_BASE_URL: str

    @classmethod
    def from_env(cls) -> 'SPAPIConfig':
        load_dotenv()
        
        required_vars = {
            'LWA_CLIENT_ID': os.getenv('LWA_CLIENT_ID'),
            'LWA_CLIENT_SECRET': os.getenv('LWA_CLIENT_SECRET'),
            'REFRESH_TOKEN': os.getenv('REFRESH_TOKEN'),
            'MARKETPLACE_ID': os.getenv('MARKETPLACE_ID')
        }

        missing = [k for k, v in required_vars.items() if not v]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        return cls(
            **required_vars,
            AWS_REGION=os.getenv('AWS_REGION', 'us-east-1'),
            SP_API_BASE_URL='https://sellingpartnerapi-na.amazon.com',
            LWA_BASE_URL='https://api.amazon.com/auth/o2/token'
        )

    @classmethod
    def validate_config(cls, config: 'SPAPIConfig') -> None:
        """Validate configuration values"""
        if not config.LWA_CLIENT_ID.startswith('amzn1.application-oa2-client'):
            raise ValidationError("Invalid LWA_CLIENT_ID format")
        
        if not config.MARKETPLACE_ID:
            raise ValidationError("MARKETPLACE_ID cannot be empty")
            
        if not config.AWS_REGION in ['us-east-1', 'eu-west-1', 'us-west-2']:
            raise ValidationError("Invalid AWS_REGION")