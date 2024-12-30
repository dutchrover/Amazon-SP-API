import requests
from sp_api.auth.authenticator import SPAPIAuthenticator
from sp_api.config import SPAPIConfig as Config
from typing import Callable, List, Dict, Any
from sp_api.exceptions import SPAPIException, RateLimitError, AuthenticationError
from sp_api.utils.rate_limiter import RateLimiter
from sp_api.utils.metrics import APIMetrics
import time
import aiohttp
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseClient:
    def __init__(self):
        self.config = Config.from_env()
        self.auth = SPAPIAuthenticator()
        self.rate_limiter = RateLimiter()
        self.metrics = APIMetrics()
        self.request_middlewares: List[Callable] = []
        self.response_middlewares: List[Callable] = []
        self.base_url = self.config.SP_API_BASE_URL

    def add_request_middleware(self, middleware: Callable):
        self.request_middlewares.append(middleware)

    def add_response_middleware(self, middleware: Callable):
        self.response_middlewares.append(middleware)

    def _get_headers(self):
        """Get base headers"""
        return {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'SP-API/Python/1.0'
        }

    async def _do_request(self, method: str, endpoint: str, params=None, data=None):
        """Make HTTP request to SP-API"""
        url = f"{self.base_url}{endpoint}"
        base_headers = self._get_headers()
        
        # Get headers with auth token
        headers = self.auth.get_headers(method, url, base_headers)
        
        logger.debug(f"Making request to {url}")
        logger.debug(f"Headers: {headers}")
        logger.debug(f"Params: {params}")
        logger.debug(f"Data: {data}")
        
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data
            ) as response:
                if response.status == 403:
                    error_text = await response.text()
                    logger.error(f"Authentication error: {error_text}")
                    raise AuthenticationError(f"Authentication failed: {error_text}")
                    
                response.raise_for_status()
                return await response.json()

    async def _make_request(self, method, endpoint, params=None, data=None):
        try:
            # Rate limiting
            await self.rate_limiter.acquire(endpoint)
            
            start_time = time.time()
            
            # Apply request middlewares
            for middleware in self.request_middlewares:
                params, data = await middleware(params, data)

            response = await self._do_request(method, endpoint, params, data)
            
            # Update metrics
            self.metrics.total_requests += 1
            self.metrics.successful_requests += 1
            self.metrics.response_times.append(time.time() - start_time)

            # Apply response middlewares
            for middleware in self.response_middlewares:
                response = await middleware(response)

            return response
            
        except requests.exceptions.HTTPError as e:
            self.metrics.failed_requests += 1
            error_type = type(e).__name__
            self.metrics.errors_by_type[error_type] = self.metrics.errors_by_type.get(error_type, 0) + 1
            
            if e.response.status_code == 429:
                raise RateLimitError("Rate limit exceeded") from e
            elif e.response.status_code == 401:
                raise AuthenticationError("Authentication failed") from e
            else:
                raise SPAPIException(f"API request failed: {str(e)}") from e

    async def test_connection(self) -> Dict[str, Any]:
        """
        Test the connection to SP-API by making a simple catalog lookup
        
        Returns:
            Dict containing connection status and details
        
        Raises:
            AuthenticationError: If authentication fails
            SPAPIException: If connection fails for other reasons
        """
        try:
            logger.info("Testing SP-API connection...")
            
            # Test authentication by getting access token
            access_token = self.auth.get_auth_token()
            if not access_token:
                raise AuthenticationError("Failed to obtain access token")
                
            # Make a simple API call to test full connection
            test_endpoint = '/catalog/2020-12-01/items'
            params = {
                'keywords': 'test',
                'marketplaceIds': [self.config.MARKETPLACE_ID]
            }
            
            start_time = time.time()
            await self._make_request('GET', test_endpoint, params=params)
            response_time = time.time() - start_time
            
            return {
                'status': 'success',
                'marketplace_id': self.config.MARKETPLACE_ID,
                'response_time_ms': round(response_time * 1000, 2),
                'timestamp': datetime.now().isoformat()
            }
            
        except AuthenticationError as e:
            logger.error(f"Authentication failed during connection test: {str(e)}")
            return {
                'status': 'failed',
                'error': 'authentication_error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return {
                'status': 'failed',
                'error': 'connection_error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }