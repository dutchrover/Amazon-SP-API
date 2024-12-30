import uuid
from datetime import datetime
import pandas as pd
import json
import logging
from typing import Dict, List, Any
from .sql_handler import SQLServerHandler
from .sql_stored_procedures import create_stored_procedures

logger = logging.getLogger(__name__)

STAGING_SCHEMAS = {
    'Orders': """
    CREATE TABLE [stage].[SP_API_Orders] (
        StagingId BIGINT IDENTITY(1,1) PRIMARY KEY,
        AmazonOrderId NVARCHAR(50),
        RawData NVARCHAR(MAX),
        ProcessedDate DATETIME2,
        ValidationStatus NVARCHAR(20),
        ValidationMessage NVARCHAR(MAX),
        IsDuplicate BIT DEFAULT 0,
        BatchId UNIQUEIDENTIFIER,
        InsertedAt DATETIME2 DEFAULT GETDATE(),
        INDEX IX_Stage_OrderId (AmazonOrderId),
        INDEX IX_Stage_Status (ValidationStatus),
        INDEX IX_Stage_BatchId (BatchId)
    )
    """,
    
    'OrderItems': """
    CREATE TABLE [stage].[SP_API_OrderItems] (
        StagingId BIGINT IDENTITY(1,1) PRIMARY KEY,
        AmazonOrderId NVARCHAR(50),
        ASIN NVARCHAR(20),
        RawData NVARCHAR(MAX),
        ProcessedDate DATETIME2,
        ValidationStatus NVARCHAR(20),
        ValidationMessage NVARCHAR(MAX),
        IsDuplicate BIT DEFAULT 0,
        BatchId UNIQUEIDENTIFIER,
        InsertedAt DATETIME2 DEFAULT GETDATE(),
        INDEX IX_Stage_OrderId (AmazonOrderId),
        INDEX IX_Stage_ASIN (ASIN)
    )
    """,
    
    'Inventory': """
    CREATE TABLE [stage].[SP_API_Inventory] (
        StagingId BIGINT IDENTITY(1,1) PRIMARY KEY,
        SellerSKU NVARCHAR(50),
        ASIN NVARCHAR(20),
        RawData NVARCHAR(MAX),
        ProcessedDate DATETIME2,
        ValidationStatus NVARCHAR(20),
        ValidationMessage NVARCHAR(MAX),
        IsDuplicate BIT DEFAULT 0,
        BatchId UNIQUEIDENTIFIER,
        InsertedAt DATETIME2 DEFAULT GETDATE(),
        INDEX IX_Stage_SKU (SellerSKU),
        INDEX IX_Stage_ASIN (ASIN)
    )
    """
}

CONTROL_SCHEMA = """
CREATE TABLE [stage].[ProcessingControl] (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    DataType NVARCHAR(50),
    LastProcessedDate DATETIME2,
    LastBatchId UNIQUEIDENTIFIER,
    RecordsProcessed INT,
    Status NVARCHAR(20),
    ProcessingStartTime DATETIME2,
    ProcessingEndTime DATETIME2,
    CreatedAt DATETIME2 DEFAULT GETDATE(),
    INDEX IX_DataType_LastProcessed (DataType, LastProcessedDate)
)
"""

ARCHIVE_SCHEMAS = {
    'Orders': """
    CREATE TABLE [archive].[SP_API_Orders] (
        ArchiveId BIGINT IDENTITY(1,1) PRIMARY KEY,
        OriginalId BIGINT,
        AmazonOrderId NVARCHAR(50),
        RawData NVARCHAR(MAX),
        ProcessedDate DATETIME2,
        BatchId UNIQUEIDENTIFIER,
        ArchivedAt DATETIME2 DEFAULT GETDATE(),
        INDEX IX_Archive_OrderId (AmazonOrderId)
    )
    """,
    
    'Inventory': """
    CREATE TABLE [archive].[SP_API_Inventory] (
        ArchiveId BIGINT IDENTITY(1,1) PRIMARY KEY,
        OriginalId BIGINT,
        SellerSKU NVARCHAR(50),
        ASIN NVARCHAR(20),
        RawData NVARCHAR(MAX),
        ProcessedDate DATETIME2,
        BatchId UNIQUEIDENTIFIER,
        ArchivedAt DATETIME2 DEFAULT GETDATE(),
        INDEX IX_Archive_SKU (SellerSKU)
    )
    """
}

class StagingHandler(SQLServerHandler):
    def __init__(self, connection_string: str):
        super().__init__(connection_string)

    def initialize_staging(self):
        """Initialize staging environment"""
        with self._create_connection() as conn:
            cursor = conn.cursor()
            
            # Create schemas if they don't exist
            cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stage') EXEC('CREATE SCHEMA stage')")
            cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'archive') EXEC('CREATE SCHEMA archive')")
            
            # Create staging tables if they don't exist
            for table_name, schema in STAGING_SCHEMAS.items():
                cursor.execute(f"""
                IF NOT EXISTS (
                    SELECT * FROM sys.tables t 
                    JOIN sys.schemas s ON t.schema_id = s.schema_id 
                    WHERE s.name = 'stage' AND t.name = 'SP_API_{table_name}'
                )
                BEGIN
                    {schema}
                END
                """)
            
            # Create control table if it doesn't exist
            cursor.execute(f"""
            IF NOT EXISTS (
                SELECT * FROM sys.tables t 
                JOIN sys.schemas s ON t.schema_id = s.schema_id 
                WHERE s.name = 'stage' AND t.name = 'ProcessingControl'
            )
            BEGIN
                {CONTROL_SCHEMA}
            END
            """)
            
            # Create archive tables if they don't exist
            for table_name, schema in ARCHIVE_SCHEMAS.items():
                cursor.execute(f"""
                IF NOT EXISTS (
                    SELECT * FROM sys.tables t 
                    JOIN sys.schemas s ON t.schema_id = s.schema_id 
                    WHERE s.name = 'archive' AND t.name = 'SP_API_{table_name}'
                )
                BEGIN
                    {schema}
                END
                """)
            
            # Create stored procedures
            create_stored_procedures(conn)
                
            conn.commit()

    async def archive_data(self, table_name: str, batch_id: str = None):
        """Archive data to archive tables"""
        archive_sql = f"""
        INSERT INTO [archive].[SP_API_{table_name}]
        (OriginalId, AmazonOrderId, RawData, ProcessedDate, BatchId)
        SELECT 
            StagingId,
            AmazonOrderId,
            RawData,
            ProcessedDate,
            BatchId
        FROM [stage].[SP_API_{table_name}]
        WHERE BatchId = ? OR ? IS NULL
        """
        
        delete_sql = f"""
        DELETE FROM [stage].[SP_API_{table_name}]
        WHERE BatchId = ? OR ? IS NULL
        """
        
        with self._create_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(archive_sql, batch_id, batch_id)
            cursor.execute(delete_sql, batch_id, batch_id)
            conn.commit()

    def get_last_processed_date(self, data_type: str) -> datetime:
        """Get the last processed date for a specific data type"""
        query = """
        SELECT TOP 1 LastProcessedDate
        FROM [stage].[ProcessingControl]
        WHERE DataType = ?
        AND Status = 'SUCCESS'
        ORDER BY LastProcessedDate DESC
        """
        
        with self._create_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, data_type)
            result = cursor.fetchone()
            
            return result[0] if result else None

    def record_processing_start(self, data_type: str, batch_id: str) -> int:
        """Record the start of a processing batch"""
        sql = """
        INSERT INTO [stage].[ProcessingControl]
        (DataType, LastBatchId, Status, ProcessingStartTime)
        VALUES (?, ?, 'IN_PROGRESS', GETDATE());
        
        SELECT SCOPE_IDENTITY();
        """
        
        with self._create_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, data_type, batch_id)
            control_id = cursor.fetchone()[0]
            conn.commit()
            return control_id

    def record_processing_complete(self, control_id: int, last_processed_date: datetime, 
                                 records_processed: int, status: str = 'SUCCESS'):
        """Record the completion of a processing batch"""
        sql = """
        UPDATE [stage].[ProcessingControl]
        SET Status = ?,
            LastProcessedDate = ?,
            RecordsProcessed = ?,
            ProcessingEndTime = GETDATE()
        WHERE Id = ?
        """
        
        with self._create_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, status, last_processed_date, records_processed, control_id)
            conn.commit()

    async def append_data(self, data: List[Dict], table_name: str, 
                         last_processed_date: datetime = None) -> Dict:
        """Append new data to staging tables with duplicate handling"""
        try:
            batch_id = str(uuid.uuid4())
            control_id = self.record_processing_start(table_name, batch_id)
            
            # Handle duplicates based on business keys
            if table_name == 'Orders':
                dedup_key = 'AmazonOrderId'
            elif table_name == 'Inventory':
                dedup_key = 'SellerSKU'
            else:
                dedup_key = None
                
            # Get existing keys
            if dedup_key:
                existing_keys_sql = f"""
                SELECT DISTINCT {dedup_key}
                FROM [stage].[SP_API_{table_name}]
                """
                with self._create_connection() as conn:
                    existing_keys = pd.read_sql(existing_keys_sql, conn)
                    existing_keys_set = set(existing_keys[dedup_key])
                    
                # Filter out duplicates
                new_data = [
                    record for record in data 
                    if record.get(dedup_key) not in existing_keys_set
                ]
            else:
                new_data = data

            # Insert new records
            insert_sql = f"""
            INSERT INTO [stage].[SP_API_{table_name}] 
            (AmazonOrderId, RawData, BatchId, ValidationStatus)
            VALUES (?, ?, ?, 'PENDING')
            """
            
            with self._create_connection() as conn:
                cursor = conn.cursor()
                for record in new_data:
                    cursor.execute(
                        insert_sql,
                        record.get(dedup_key),
                        json.dumps(record),
                        batch_id
                    )
                conn.commit()

            # Record completion
            self.record_processing_complete(
                control_id,
                last_processed_date or datetime.now(),
                len(new_data)
            )

            return {
                'batch_id': batch_id,
                'records_processed': len(new_data),
                'duplicates_skipped': len(data) - len(new_data)
            }

        except Exception as e:
            logger.error(f"Error in append_data: {e}")
            self.record_processing_complete(
                control_id,
                last_processed_date or datetime.now(),
                0,
                'ERROR'
            )
            raise

    async def replace_data(self, data: List[Dict], table_name: str,
                          last_processed_date: datetime = None) -> Dict:
        """Replace existing data in staging tables"""
        try:
            batch_id = str(uuid.uuid4())
            control_id = self.record_processing_start(table_name, batch_id)
            
            # Archive existing data
            await self.archive_data(table_name)
            
            # Insert new data
            insert_sql = f"""
            INSERT INTO [stage].[SP_API_{table_name}] 
            (AmazonOrderId, RawData, BatchId, ValidationStatus)
            VALUES (?, ?, ?, 'PENDING')
            """
            
            with self._create_connection() as conn:
                cursor = conn.cursor()
                for record in data:
                    cursor.execute(
                        insert_sql,
                        record.get('AmazonOrderId'),
                        json.dumps(record),
                        batch_id
                    )
                conn.commit()

            # Record completion
            self.record_processing_complete(
                control_id,
                last_processed_date or datetime.now(),
                len(data)
            )

            return {
                'batch_id': batch_id,
                'records_processed': len(data)
            }

        except Exception as e:
            logger.error(f"Error in replace_data: {e}")
            self.record_processing_complete(
                control_id,
                last_processed_date or datetime.now(),
                0,
                'ERROR'
            )
            raise

    def get_staging_errors(self, batch_id: str) -> pd.DataFrame:
        """Get validation errors for a batch"""
        query = """
        SELECT 
            StagingId,
            AmazonOrderId,
            ValidationStatus,
            ValidationMessage,
            IsDuplicate,
            InsertedAt
        FROM stage.SP_API_Orders
        WHERE BatchId = ?
        AND (ValidationStatus = 'ERROR' OR IsDuplicate = 1)
        """
        
        with self._create_connection() as conn:
            return pd.read_sql(query, conn, params=[batch_id])
