class SPAPIException(Exception):
    """Base exception for SP-API errors"""
    pass

class AuthenticationError(SPAPIException):
    """Authentication related errors"""
    pass

class RateLimitError(SPAPIException):
    """Rate limit exceeded errors"""
    pass

class ValidationError(SPAPIException):
    """Data validation errors"""
    pass 