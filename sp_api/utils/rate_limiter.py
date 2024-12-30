from datetime import datetime, timedelta
import asyncio
from typing import Dict
import random

class RateLimiter:
    def __init__(self, calls_per_second: float = 1.0):
        self.calls_per_second = calls_per_second
        self.minimum_interval = 1.0 / calls_per_second
        self.last_call_time: Dict[str, datetime] = {}

    async def acquire(self, endpoint: str):
        """Rate limit by endpoint with backoff"""
        now = datetime.now()
        if endpoint in self.last_call_time:
            elapsed = (now - self.last_call_time[endpoint]).total_seconds()
            if elapsed < self.minimum_interval:
                wait_time = self.minimum_interval - elapsed
                # Add small random jitter to prevent thundering herd
                wait_time += random.uniform(0, 0.1)
                await asyncio.sleep(wait_time)
        self.last_call_time[endpoint] = datetime.now() 