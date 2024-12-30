from .base_client import BaseClient
from datetime import datetime, timedelta

class OrdersAPI(BaseClient):
    def __init__(self):
        super().__init__()
    
    async def get_orders(self, created_after=None, created_before=None, last_updated_after=None, 
                  last_updated_before=None, order_statuses=None, fulfillment_channels=None,
                  payment_methods=None, next_token=None):
        """
        Get orders with extensive filtering options
        
        Args:
            created_after (str): ISO date for orders created after this date
            created_before (str): ISO date for orders created before this date
            last_updated_after (str): ISO date for orders updated after this date
            last_updated_before (str): ISO date for orders updated before this date
            order_statuses (list): List of order statuses to filter
            fulfillment_channels (list): List of fulfillment channels
            payment_methods (list): List of payment methods
            next_token (str): Token for pagination
        """
        endpoint = '/orders/v0/orders'
        
        params = {
            'MarketplaceIds': [self.config.MARKETPLACE_ID]
        }
        
        # Add date filters
        if created_after:
            params['CreatedAfter'] = created_after
        if created_before:
            params['CreatedBefore'] = created_before
        if last_updated_after:
            params['LastUpdatedAfter'] = last_updated_after
        if last_updated_before:
            params['LastUpdatedBefore'] = last_updated_before
        if next_token:
            params['NextToken'] = next_token
            
        # Add other filters
        if order_statuses:
            params['OrderStatuses'] = order_statuses
        if fulfillment_channels:
            params['FulfillmentChannels'] = fulfillment_channels
        if payment_methods:
            params['PaymentMethods'] = payment_methods
            
        return await self._make_request('GET', endpoint, params=params)

    async def get_order_items(self, order_id):
        """Get items for a specific order"""
        endpoint = f'/orders/v0/orders/{order_id}/orderItems'
        return await self._make_request('GET', endpoint)

    async def get_order_metrics(self, interval, granularity):
        """Get order metrics for specified interval and granularity"""
        endpoint = '/sales/v1/orderMetrics'
        params = {
            'marketplaceIds': [self.config.MARKETPLACE_ID],
            'interval': interval,
            'granularity': granularity
        }
        return await self._make_request('GET', endpoint, params=params)