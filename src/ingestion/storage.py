"""Data storage layer for PostgreSQL."""

from typing import Dict, Any, Optional
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Numeric, DateTime, Index
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from src.config.settings import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataStorage:
    """Handles data storage to PostgreSQL database."""
    
    def __init__(self):
        """Initialize database connection."""
        self.connection_string = settings.config.database.get_connection_string()
        self.engine: Optional[Engine] = None
        self.metadata = MetaData()
    
    def get_engine(self) -> Engine:
        """
        Get or create database engine.
        
        Returns:
            SQLAlchemy engine
        """
        if self.engine is None:
            logger.info("Creating database connection")
            self.engine = create_engine(
                self.connection_string,
                poolclass=NullPool,  # Use NullPool for simpler connection management
                echo=False
            )
        return self.engine
    
    def create_metadata_table(self) -> None:
        """Create dataset metadata tracking table if it doesn't exist."""
        engine = self.get_engine()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS dataset_metadata (
            dataset_id VARCHAR(20) PRIMARY KEY,
            dataset_name VARCHAR(255),
            table_name VARCHAR(255),
            last_ingestion TIMESTAMP,
            record_count INTEGER,
            status VARCHAR(50)
        );
        """
        
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        
        logger.info("Dataset metadata table created/verified")
    
    def create_table_from_schema(self, schema: Dict[str, Any]) -> None:
        """
        Create table from schema definition.
        
        Args:
            schema: Schema dictionary from transformer
        """
        engine = self.get_engine()
        table_name = schema['table_name']
        
        logger.info(f"Creating table: {table_name}")
        
        # Build CREATE TABLE statement
        columns = []
        for col_name, col_def in schema['columns'].items():
            col_type = col_def['type']
            nullable = col_def.get('nullable', True)
            default = col_def.get('default')
            primary_key = col_def.get('primary_key', False)
            
            col_str = f"{col_name} {col_type}"
            
            if primary_key:
                col_str += " PRIMARY KEY"
            elif not nullable:
                col_str += " NOT NULL"
            
            if default:
                col_str += f" DEFAULT {default}"
            
            columns.append(col_str)
        
        # Add constraints
        if 'constraints' in schema:
            columns.extend(schema['constraints'])
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        );
        """
        
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        
        # Create indexes
        if 'indexes' in schema:
            for index_def in schema['indexes']:
                index_name = index_def['name']
                index_columns = ', '.join(index_def['columns'])
                
                create_index_sql = f"""
                CREATE INDEX IF NOT EXISTS {index_name} 
                ON {table_name} ({index_columns});
                """
                
                with engine.connect() as conn:
                    conn.execute(text(create_index_sql))
                    conn.commit()
        
        logger.info(f"Table {table_name} created/verified with indexes")
    
    def store_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        dataset_id: str,
        if_exists: str = 'append'
    ) -> int:
        """
        Store DataFrame to PostgreSQL table.
        
        Args:
            df: DataFrame to store
            table_name: Target table name
            dataset_id: Dataset identifier
            if_exists: How to behave if table exists ('append', 'replace', 'fail')
            
        Returns:
            Number of records stored
        """
        engine = self.get_engine()
        
        logger.info(f"Storing {len(df)} records to table: {table_name}")
        
        try:
            # Use pandas to_sql for simplicity
            # For production, consider using COPY or bulk insert for better performance
            df.to_sql(
                table_name,
                engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )
            
            # Update metadata table
            self._update_metadata(dataset_id, table_name, len(df))
            
            logger.info(f"Successfully stored {len(df)} records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to store data: {e}")
            raise
    
    def upsert_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        dataset_id: str,
        unique_columns: list
    ) -> int:
        """
        Upsert data using PostgreSQL's ON CONFLICT clause.
        
        Args:
            df: DataFrame to upsert
            table_name: Target table name
            dataset_id: Dataset identifier
            unique_columns: Columns that define uniqueness
            
        Returns:
            Number of records upserted
        """
        engine = self.get_engine()
        
        logger.info(f"Upserting {len(df)} records to table: {table_name}")
        
        try:
            # Convert DataFrame to list of dicts
            records = df.to_dict('records')
            
            with engine.connect() as conn:
                # Build upsert statement
                # This is a simplified version; for production, use SQLAlchemy's insert with on_conflict_do_update
                for record in records:
                    columns = list(record.keys())
                    values = [record[col] for col in columns]
                    
                    # Build INSERT ... ON CONFLICT DO UPDATE
                    insert_cols = ', '.join(columns)
                    insert_vals = ', '.join([f":{col}" for col in columns])
                    
                    update_cols = [col for col in columns if col not in unique_columns]
                    update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                    
                    conflict_cols = ', '.join(unique_columns)
                    
                    upsert_sql = f"""
                    INSERT INTO {table_name} ({insert_cols})
                    VALUES ({insert_vals})
                    ON CONFLICT ({conflict_cols})
                    DO UPDATE SET {update_set};
                    """
                    
                    conn.execute(text(upsert_sql), record)
                
                conn.commit()
            
            # Update metadata
            self._update_metadata(dataset_id, table_name, len(df))
            
            logger.info(f"Successfully upserted {len(df)} records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to upsert data: {e}")
            raise
    
    def _update_metadata(self, dataset_id: str, table_name: str, record_count: int) -> None:
        """
        Update dataset metadata table.
        
        Args:
            dataset_id: Dataset identifier
            table_name: Table name
            record_count: Number of records
        """
        engine = self.get_engine()
        
        update_sql = """
        INSERT INTO dataset_metadata (dataset_id, table_name, last_ingestion, record_count, status)
        VALUES (:dataset_id, :table_name, CURRENT_TIMESTAMP, :record_count, 'success')
        ON CONFLICT (dataset_id)
        DO UPDATE SET
            last_ingestion = CURRENT_TIMESTAMP,
            record_count = :record_count,
            status = 'success';
        """
        
        with engine.connect() as conn:
            conn.execute(
                text(update_sql),
                {
                    'dataset_id': dataset_id,
                    'table_name': table_name,
                    'record_count': record_count
                }
            )
            conn.commit()
    
    def export_to_parquet(
        self,
        df: pd.DataFrame,
        dataset_id: str,
        output_path: Optional[Path] = None
    ) -> Path:
        """
        Export DataFrame to Parquet format.
        
        Args:
            df: DataFrame to export
            dataset_id: Dataset identifier
            output_path: Optional output path
            
        Returns:
            Path to exported Parquet file
        """
        if output_path is None:
            output_path = settings.get_data_path('processed') / f"{dataset_id}.parquet"
        
        logger.info(f"Exporting to Parquet: {output_path}")
        
        try:
            df.to_parquet(
                output_path,
                compression='snappy',
                index=False
            )
            logger.info(f"Successfully exported {len(df)} records to Parquet")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to export to Parquet: {e}")
            raise
    
    def query_data(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame.
        
        Args:
            query: SQL query string
            
        Returns:
            Query results as DataFrame
        """
        engine = self.get_engine()
        
        try:
            df = pd.read_sql(query, engine)
            return df
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
    
    def close(self) -> None:
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")
