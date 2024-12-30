import pyodbc
import json
from datetime import datetime
import pandas as pd
from typing import Dict, List, Any
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class SQLServerHandler:
    def __init__(self, connection_string: str, pool_size: int = 5):
        """Initialize SQL Server connection"""
        self.conn_str = connection_string
        self.pool_size = pool_size
        self._connection_pool = []

    @contextmanager
    def get_connection(self):
        """Get connection from pool or create new one"""
        conn = None
        try:
            if self._connection_pool:
                conn = self._connection_pool.pop()
                # Test connection is still alive
                conn.execute("SELECT 1").fetchone()
            else:
                conn = pyodbc.connect(self.conn_str)
            yield conn
        except (pyodbc.Error, pyodbc.OperationalError):
            # Connection is dead, create new one
            if conn:
                try:
                    conn.close()
                except:
                    pass
            conn = pyodbc.connect(self.conn_str)
            yield conn
        finally:
            if conn and len(self._connection_pool) < self.pool_size:
                try:
                    self._connection_pool.append(conn)
                except:
                    conn.close()
            elif conn:
                conn.close()

    def _create_connection(self):
        """Create database connection"""
        return pyodbc.connect(self.conn_str)

    def _get_sql_type(self, value: Any) -> str:
        """Map Python types to SQL Server types"""
        if isinstance(value, bool):
            return 'BIT'
        elif isinstance(value, int):
            return 'BIGINT'
        elif isinstance(value, float):
            return 'FLOAT'
        elif isinstance(value, datetime):
            return 'DATETIME2'
        elif isinstance(value, (dict, list)):
            return 'NVARCHAR(MAX)'
        else:
            return 'NVARCHAR(500)'

    def _flatten_dict(self, d: Dict, parent_key: str = '') -> Dict:
        """Recursively flatten nested dictionary"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def query_data(self, table_name: str, schema: str = 'dbo', 
                   where_clause: str = None, order_by: str = None) -> pd.DataFrame:
        """Query data from table"""
        query = f"SELECT * FROM [{schema}].[{table_name}]"
        if where_clause:
            query += f" WHERE {where_clause}"
        if order_by:
            query += f" ORDER BY {order_by}"
            
        with self._create_connection() as conn:
            return pd.read_sql(query, conn)

    def execute_query(self, query: str, params: List = None) -> pd.DataFrame:
        """Execute a custom query"""
        with self._create_connection() as conn:
            return pd.read_sql(query, conn, params=params)

    def execute_nonquery(self, query: str, params: List = None) -> int:
        """Execute a non-query SQL command"""
        with self._create_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor.rowcount

    def optimize_tables(self):
        """Perform table optimization tasks"""
        maintenance_sql = """
        -- Update statistics
        UPDATE STATISTICS stage.SP_API_Orders WITH FULLSCAN;
        UPDATE STATISTICS stage.SP_API_OrderItems WITH FULLSCAN;
        UPDATE STATISTICS stage.SP_API_Inventory WITH FULLSCAN;
        
        -- Rebuild indexes
        ALTER INDEX ALL ON stage.SP_API_Orders REBUILD;
        ALTER INDEX ALL ON stage.SP_API_OrderItems REBUILD;
        ALTER INDEX ALL ON stage.SP_API_Inventory REBUILD;
        """
        
        with self._create_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(maintenance_sql)
            conn.commit()

    def execute_stored_procedure(self, proc_name: str, params: Dict = None) -> pd.DataFrame:
        """Execute a stored procedure and return results"""
        param_str = ""
        if params:
            param_str = ", ".join([f"@{k}=?" for k in params.keys()])
        
        sql = f"EXEC {proc_name} {param_str}"
        
        with self._create_connection() as conn:
            if params:
                df = pd.read_sql(sql, conn, params=list(params.values()))
            else:
                df = pd.read_sql(sql, conn)
        return df

    def bulk_insert(self, table_name: str, data: List[Dict], schema: str = 'dbo') -> int:
        """Bulk insert data into a table"""
        if not data:
            return 0

        # Get column names and types from first record
        sample_record = data[0]
        columns = list(sample_record.keys())
        
        # Create insert statement
        insert_sql = f"""
        INSERT INTO [{schema}].[{table_name}] 
        ({','.join([f'[{col}]' for col in columns])})
        VALUES ({','.join(['?' for _ in columns])})
        """

        # Insert data
        with self._create_connection() as conn:
            cursor = conn.cursor()
            for record in data:
                values = [record.get(col) for col in columns]
                cursor.execute(insert_sql, values)
            conn.commit()
            return len(data)
        