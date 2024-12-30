# sp_api/utils/batch_processor.py
from datetime import datetime, timedelta
import time
from typing import Dict, List, Generator, Any
import logging
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, 
                 batch_size: int = 1000, 
                 rate_limit_pause: float = 0.5,
                 max_retries: int = 3):
        self.batch_size = batch_size
        self.rate_limit_pause = rate_limit_pause
        self.max_retries = max_retries

    def _chunk_date_range(self, 
                         start_date: datetime, 
                         end_date: datetime, 
                         chunk_days: int = 7) -> Generator[tuple, None, None]:
        """Split date range into smaller chunks"""
        current_date = start_date
        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=chunk_days), end_date)
            yield current_date, chunk_end
            current_date = chunk_end

    def _chunk_list(self, items: List[Any], chunk_size: int) -> Generator[List[Any], None, None]:
        """Split list into smaller chunks"""
        for i in range(0, len(items), chunk_size):
            yield items[i:i + chunk_size]

    async def process_orders_incremental(self, 
                                       orders_api: Any, 
                                       staging_handler: Any,
                                       end_date: datetime = None) -> Dict[str, Any]:
        """
        Process orders incrementally - APPEND mode
        Only processes new orders after the last processed date
        """
        # Get last processed date
        last_processed = staging_handler.get_last_processed_date('Orders')
        if not last_processed:
            # Default to 30 days back if no previous processing
            last_processed = datetime.now() - timedelta(days=30)
            
        end_date = end_date or datetime.now()
        
        logger.info(f"Processing new orders from {last_processed} to {end_date}")
        return await self.process_orders(
            orders_api,
            staging_handler,
            start_date=last_processed,
            end_date=end_date,
            mode='APPEND'
        )

    async def process_orders(self, 
                           orders_api: Any, 
                           staging_handler: Any,
                           start_date: datetime,
                           end_date: datetime,
                           mode: str = 'APPEND') -> Dict[str, Any]:
        """Process orders in batches"""
        total_stats = {
            'total_processed': 0,
            'total_batches': 0,
            'errors': 0,
            'batch_ids': [],
            'mode': mode
        }

        # Process by date chunks to handle large date ranges
        for chunk_start, chunk_end in self._chunk_date_range(start_date, end_date):
            orders_buffer = []
            
            try:
                # Get orders for date chunk with pagination
                next_token = None
                while True:
                    retry_count = 0
                    while retry_count < self.max_retries:
                        try:
                            orders_response = await orders_api.get_orders(
                                created_after=chunk_start.isoformat(),
                                created_before=chunk_end.isoformat(),
                                next_token=next_token
                            )
                            break
                        except Exception as e:
                            retry_count += 1
                            if retry_count == self.max_retries:
                                raise
                            logger.warning(f"Retry {retry_count} for orders API call: {e}")
                            time.sleep(2 ** retry_count)  # Exponential backoff
                    
                    orders = orders_response.get('Orders', [])
                    orders_buffer.extend(orders)
                    
                    # Process buffer if it reaches batch size
                    if len(orders_buffer) >= self.batch_size:
                        batch_id = await self._process_order_batch(
                            orders_buffer[:self.batch_size],
                            staging_handler,
                            mode
                        )
                        total_stats['batch_ids'].append(batch_id)
                        total_stats['total_processed'] += len(orders_buffer[:self.batch_size])
                        total_stats['total_batches'] += 1
                        orders_buffer = orders_buffer[self.batch_size:]
                    
                    next_token = orders_response.get('NextToken')
                    if not next_token:
                        break
                        
                    time.sleep(self.rate_limit_pause)
                
                # Process remaining orders in buffer
                if orders_buffer:
                    batch_id = await self._process_order_batch(orders_buffer, staging_handler, mode)
                    total_stats['batch_ids'].append(batch_id)
                    total_stats['total_processed'] += len(orders_buffer)
                    total_stats['total_batches'] += 1
                
            except Exception as e:
                logger.error(f"Error processing orders for date range {chunk_start} to {chunk_end}: {e}")
                total_stats['errors'] += 1
                continue

        return total_stats

    async def process_inventory_incremental(self,
                                          inventory_api: Any,
                                          staging_handler: Any,
                                          sku_list: List[str] = None) -> Dict[str, Any]:
        """
        Process inventory incrementally - REPLACE mode
        Archives old inventory data and replaces with current state
        """
        try:
            # Archive existing inventory data
            await staging_handler.archive_inventory_data()
            
            logger.info("Processing current inventory state")
            return await self.process_inventory(
                inventory_api,
                staging_handler,
                sku_list,
                mode='REPLACE'
            )
        except Exception as e:
            logger.error(f"Error in incremental inventory processing: {e}")
            raise

    async def process_inventory(self,
                              inventory_api: Any,
                              staging_handler: Any,
                              sku_list: List[str] = None,
                              mode: str = 'REPLACE') -> Dict[str, Any]:
        """Process inventory in batches"""
        total_stats = {
            'total_processed': 0,
            'total_batches': 0,
            'errors': 0,
            'batch_ids': [],
            'mode': mode
        }

        try:
            if sku_list:
                # Process specific SKUs in chunks
                for sku_chunk in self._chunk_list(sku_list, self.batch_size):
                    retry_count = 0
                    while retry_count < self.max_retries:
                        try:
                            inventory_data = await inventory_api.get_inventory_details(sku_chunk)
                            break
                        except Exception as e:
                            retry_count += 1
                            if retry_count == self.max_retries:
                                raise
                            logger.warning(f"Retry {retry_count} for inventory API call: {e}")
                            time.sleep(2 ** retry_count)
                    
                    batch_id = await self._process_inventory_batch(
                        inventory_data, 
                        staging_handler,
                        mode
                    )
                    total_stats['batch_ids'].append(batch_id)
                    total_stats['total_processed'] += len(inventory_data)
                    total_stats['total_batches'] += 1
                    
                    time.sleep(self.rate_limit_pause)
            else:
                # Get all inventory with pagination
                next_token = None
                inventory_buffer = []
                
                while True:
                    retry_count = 0
                    while retry_count < self.max_retries:
                        try:
                            inventory_response = await inventory_api.get_inventory_summary(
                                next_token=next_token
                            )
                            break
                        except Exception as e:
                            retry_count += 1
                            if retry_count == self.max_retries:
                                raise
                            logger.warning(f"Retry {retry_count} for inventory API call: {e}")
                            time.sleep(2 ** retry_count)
                    
                    inventory_items = inventory_response.get('inventory', [])
                    inventory_buffer.extend(inventory_items)
                    
                    if len(inventory_buffer) >= self.batch_size:
                        batch_id = await self._process_inventory_batch(
                            inventory_buffer[:self.batch_size],
                            staging_handler,
                            mode
                        )
                        total_stats['batch_ids'].append(batch_id)
                        total_stats['total_processed'] += len(inventory_buffer[:self.batch_size])
                        total_stats['total_batches'] += 1
                        inventory_buffer = inventory_buffer[self.batch_size:]
                    
                    next_token = inventory_response.get('NextToken')
                    if not next_token:
                        break
                        
                    time.sleep(self.rate_limit_pause)
                
                # Process remaining inventory items
                if inventory_buffer:
                    batch_id = await self._process_inventory_batch(
                        inventory_buffer, 
                        staging_handler,
                        mode
                    )
                    total_stats['batch_ids'].append(batch_id)
                    total_stats['total_processed'] += len(inventory_buffer)
                    total_stats['total_batches'] += 1
                    
        except Exception as e:
            logger.error(f"Error processing inventory: {e}")
            total_stats['errors'] += 1

        return total_stats

    async def _process_order_batch(self, 
                                 orders: List[Dict], 
                                 staging_handler: Any,
                                 mode: str = 'APPEND') -> str:
        """Process a batch of orders"""
        try:
            # Get max date from batch for tracking
            max_date = max(
                datetime.fromisoformat(order['PurchaseDate'])
                for order in orders
                if 'PurchaseDate' in order
            )
            
            if mode == 'APPEND':
                result = await staging_handler.append_data(
                    orders, 
                    'Orders',
                    last_processed_date=max_date
                )
            else:  # mode == 'REPLACE'
                result = await staging_handler.replace_data(
                    orders,
                    'Orders',
                    last_processed_date=max_date
                )
                
            return result['batch_id']
        except Exception as e:
            logger.error(f"Error processing order batch: {e}")
            raise

    async def _process_inventory_batch(self, 
                                     inventory: List[Dict], 
                                     staging_handler: Any,
                                     mode: str = 'REPLACE') -> str:
        """Process a batch of inventory items"""
        try:
            result = await staging_handler.replace_data(
                inventory, 
                'Inventory',
                last_processed_date=datetime.now()
            )
            return result['batch_id']
        except Exception as e:
            logger.error(f"Error processing inventory batch: {e}")
            raise