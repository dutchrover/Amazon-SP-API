from .base_client import BaseClient

class CatalogAPI(BaseClient):
    def __init__(self):
        super().__init__()
    
    async def get_catalog_item(self, asin):
        """
        Get catalog item details by ASIN
        
        Args:
            asin (str): Amazon Standard Identification Number
        """
        endpoint = f'/catalog/2020-12-01/items/{asin}'
        return await self._make_request('GET', endpoint)

    async def search_catalog_items(self, keywords, marketplace_ids=None, next_token=None):
        """
        Search catalog items by keywords with pagination support
        
        Args:
            keywords (str): Search keywords
            marketplace_ids (list): List of marketplace IDs
            next_token (str): Token for pagination
        """
        endpoint = '/catalog/2020-12-01/items'
        params = {
            'keywords': keywords,
            'marketplaceIds': marketplace_ids or [self.config.MARKETPLACE_ID]
        }
        
        if next_token:
            params['nextToken'] = next_token
            
        return await self._make_request('GET', endpoint, params=params)

    async def get_all_catalog_items(self, keywords, marketplace_ids=None):
        """
        Get all catalog items matching keywords using pagination
        
        Args:
            keywords (str): Search keywords
            marketplace_ids (list): List of marketplace IDs
        """
        all_items = []
        next_token = None
        
        while True:
            response = await self.search_catalog_items(keywords, marketplace_ids, next_token)
            all_items.extend(response.get('items', []))
            
            next_token = response.get('nextToken')
            if not next_token:
                break
                
        return all_items