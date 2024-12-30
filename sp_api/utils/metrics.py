from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List
import json

@dataclass
class APIMetrics:
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    response_times: List[float] = field(default_factory=list)
    errors_by_type: Dict[str, int] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps({
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'average_response_time': sum(self.response_times) / len(self.response_times) if self.response_times else 0,
            'errors_by_type': self.errors_by_type
        }) 