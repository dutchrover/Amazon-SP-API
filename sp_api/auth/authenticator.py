import requests
import time
from typing import Dict
from datetime import datetime, timedelta
from sp_api.config import SPAPIConfig as Config
from sp_api.exceptions import AuthenticationError
import logging
import json

logger = logging.getLogger(__name__)

class SPAPIAuthenticator:
    def __init__(self):
        self.config = Config.from_env()
        self._access_token = None
        self._token_expiry = None

    def _get_lwa_token(self):
        """Get Login with Amazon access token"""
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.config.REFRESH_TOKEN,
            'client_id': self.config.LWA_CLIENT_ID,
            'client_secret': self.config.LWA_CLIENT_SECRET
        }
        
        logger.debug(f"Getting LWA token with client_id: {self.config.LWA_CLIENT_ID}")
        logger.debug(f"Token URL: {self.config.LWA_BASE_URL}")
        
        try:
            response = requests.post(self.config.LWA_BASE_URL, data=data)
            response.raise_for_status()
            token_data = response.json()
            logger.debug("Successfully obtained LWA token")
            return token_data
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get LWA token: {str(e)}")
            if response:
                logger.error(f"Response: {response.text}")
            raise AuthenticationError(f"Failed to get LWA token: {str(e)}")

    def get_auth_token(self):
        """Get valid authentication token"""
        if (not self._access_token or 
            not self._token_expiry or 
            datetime.now() >= self._token_expiry):
            
            logger.debug("Getting new auth token")
            token_data = self._get_lwa_token()
            self._access_token = token_data['access_token']
            self._token_expiry = datetime.now() + timedelta(seconds=token_data['expires_in'])
            logger.debug(f"Token will expire at {self._token_expiry}")
        else:
            logger.debug("Using cached auth token")
        
        return self._access_token

    def get_headers(self, method: str, url: str, base_headers: Dict) -> Dict:
        """Get request headers"""
        return {
            **base_headers,
            'x-amz-access-token': self.get_auth_token()
        }