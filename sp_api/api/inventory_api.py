from .base_client import BaseClient

class InventoryAPI(BaseClient):
    def __init__(self):
        super().__init__()
    
    async def get_inventory_summary(self, marketplace_ids=None, next_token=None):
        """
        Get inventory summary with pagination support
        
        Args:
            marketplace_ids (list): List of marketplace IDs
            next_token (str): Token for pagination
        """
        endpoint = '/fba/inventory/v1/summaries'
        params = {
            'marketplaceIds': marketplace_ids or [self.config.MARKETPLACE_ID],
            'details': 'true',
            'granularityType': 'Marketplace',
            'granularityId': self.config.MARKETPLACE_ID
        }
        
        if next_token:
            params['nextToken'] = next_token
            
        return await self._make_request('GET', endpoint, params=params)

    async def get_inventory_details(self, sku_list, marketplace_ids=None):
        """
        Get detailed inventory information for specific SKUs
        
        Args:
            sku_list (list): List of SKUs to get details for
            marketplace_ids (list): List of marketplace IDs
        """
        endpoint = '/fba/inventory/v1/inventories'
        params = {
            'marketplaceIds': marketplace_ids or [self.config.MARKETPLACE_ID],
            'sellerSkus': sku_list,
            'details': 'true'
        }
        return await self._make_request('GET', endpoint, params=params)

    async def get_all_inventory(self, marketplace_ids=None):
        """
        Get all inventory items using pagination
        
        Args:
            marketplace_ids (list): List of marketplace IDs
        """
        all_inventory = []
        next_token = None
        
        while True:
            response = await self.get_inventory_summary(marketplace_ids, next_token)
            all_inventory.extend(response.get('inventorySummaries', []))
            
            next_token = response.get('nextToken')
            if not next_token:
                break
                
        return all_inventory