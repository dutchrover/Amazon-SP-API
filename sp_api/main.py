import asyncio
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any
import argparse
import json
import sys

from sp_api.api.orders_api import OrdersAPI
from sp_api.api.inventory_api import InventoryAPI
from sp_api.api.catalog_api import CatalogAPI
from sp_api.utils.batch_processor import BatchProcessor
from sp_api.utils.sql_handler import SQLServerHandler
from sp_api.utils.sql_staging_definitions import StagingHandler

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Set specific loggers to DEBUG level
logging.getLogger('sp_api.auth').setLevel(logging.DEBUG)
logging.getLogger('sp_api.api').setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)

class SPAPIProcessor:
    def __init__(self, sql_connection_string: str):
        # Initialize API clients
        self.orders_api = OrdersAPI()
        self.inventory_api = InventoryAPI()
        self.catalog_api = CatalogAPI()

        # Initialize handlers
        self.staging_handler = StagingHandler(sql_connection_string)
        self.batch_processor = BatchProcessor(
            batch_size=100,
            rate_limit_pause=1.0,
            max_retries=3
        )

    async def initialize_database(self):
        """Initialize database and staging environment"""
        logger.info("Initializing database...")
        self.staging_handler.initialize_staging()
        logger.info("Database initialization complete")

    async def process_orders(self, start_date: datetime, end_date: datetime) -> Dict:
        """Process orders for date range in chunks"""
        logger.info(f"Processing orders from {start_date} to {end_date}")
        
        try:
            # Process in 30-day chunks to avoid timeouts and respect rate limits
            current_start = start_date
            total_stats = {'total_processed': 0, 'total_batches': 0, 'errors': 0, 'batch_ids': [], 'mode': 'APPEND'}
            
            while current_start < end_date:
                current_end = min(current_start + timedelta(days=30), end_date)
                logger.info(f"Processing chunk from {current_start} to {current_end}")
                
                # Add delay between chunks to respect rate limits
                if current_start > start_date:
                    await asyncio.sleep(2.0)  # 2-second pause between chunks
                
                chunk_stats = await self.batch_processor.process_orders(
                    self.orders_api,
                    self.staging_handler,
                    current_start,
                    current_end
                )
                
                # Aggregate stats
                total_stats['total_processed'] += chunk_stats.get('total_processed', 0)
                total_stats['total_batches'] += chunk_stats.get('total_batches', 0)
                total_stats['errors'] += chunk_stats.get('errors', 0)
                total_stats['batch_ids'].extend(chunk_stats.get('batch_ids', []))
                
                current_start = current_end
                logger.info(f"Chunk processing complete: {chunk_stats}")
            
            logger.info(f"All orders processing complete: {total_stats}")
            return total_stats
            
        except Exception as e:
            logger.error(f"Error processing orders: {e}")
            raise

    async def process_inventory(self, sku_list: List[str] = None) -> Dict:
        """Process inventory"""
        try:
            if sku_list:
                logger.info(f"Processing inventory for {len(sku_list)} SKUs")
            else:
                logger.info("Processing full inventory")
                
            stats = await self.batch_processor.process_inventory(
                self.inventory_api,
                self.staging_handler,
                sku_list
            )
            
            logger.info(f"Inventory processing complete: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error processing inventory: {e}")
            raise

    def generate_reports(self):
        """Generate standard reports"""
        try:
            # Order summary report
            order_summary = self.staging_handler.execute_stored_procedure(
                'SP_OrderSummary',
                {
                    'StartDate': datetime.now() - timedelta(days=30),
                    'EndDate': datetime.now()
                }
            )
            logger.info("Order summary report generated")

            # Inventory aging report
            aging_inventory = self.staging_handler.execute_stored_procedure(
                'SP_InventoryAging',
                {'DaysThreshold': 90}
            )
            logger.info("Inventory aging report generated")

            return {
                'order_summary': order_summary,
                'aging_inventory': aging_inventory
            }

        except Exception as e:
            logger.error(f"Error generating reports: {e}")
            raise

    def optimize_database(self):
        """Perform database optimization"""
        try:
            logger.info("Starting database optimization")
            self.staging_handler.optimize_tables()
            logger.info("Database optimization complete")
        except Exception as e:
            logger.error(f"Error optimizing database: {e}")
            raise

    async def test_api_connection(self) -> Dict[str, Any]:
        """Test connection to SP-API"""
        try:
            # Test each API client
            results = {
                'orders': await self.orders_api.test_connection(),
                'inventory': await self.inventory_api.test_connection(),
                'catalog': await self.catalog_api.test_connection()
            }
            
            # Check if all connections were successful
            all_successful = all(r['status'] == 'success' for r in results.values())
            
            return {
                'overall_status': 'success' if all_successful else 'partial_failure',
                'timestamp': datetime.now().isoformat(),
                'api_results': results
            }
            
        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            return {
                'overall_status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

async def main():
    parser = argparse.ArgumentParser(description='Amazon SP-API Data Processor')
    parser.add_argument('--connection-string', required=True, help='SQL Server connection string')
    parser.add_argument('--test-connection', action='store_true', help='Test SP-API connection')
    parser.add_argument('--start-date', type=str, default='2024-12-27', help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format (defaults to today)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    parser.add_argument('--initialize-db', action='store_true', help='Initialize database')
    args = parser.parse_args()

    processor = SPAPIProcessor(args.connection_string)

    if args.test_connection:
        logger.info("Testing SP-API connection...")
        result = await processor.test_api_connection()
        print(json.dumps(result, indent=2))
        if result['overall_status'] != 'success':
            sys.exit(1)
        logger.info("Connection test successful!")
        return

    if args.initialize_db:
        await processor.initialize_database()

    try:
        # Process orders
        end_date = datetime.now()
        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        
        order_stats = await processor.process_orders(start_date, end_date)
        logger.info(f"Order processing stats: {order_stats}")

        # Add delay before processing inventory to respect rate limits
        await asyncio.sleep(2.0)

        # Process inventory
        inventory_stats = await processor.process_inventory()
        logger.info(f"Inventory processing stats: {inventory_stats}")

        # Generate reports
        reports = processor.generate_reports()
        logger.info("Reports generated successfully")

        # Optimize database
        processor.optimize_database()
        logger.info("Database optimization complete")

    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())