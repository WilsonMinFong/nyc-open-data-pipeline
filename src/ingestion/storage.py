"""Data storage layer for PostgreSQL."""

from typing import Dict, Any, Optional
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, DateTime, Numeric, Index
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from geoalchemy2 import Geometry

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

    def enable_postgis(self) -> None:
        """Enable PostGIS extension if not exists."""
        engine = self.get_engine()
        with engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            conn.commit()
        logger.info("PostGIS extension enabled/verified")

    def create_metadata_table(self) -> None:
        """Create dataset metadata tracking table if it doesn't exist."""
        engine = self.get_engine()

        # Define table using SQLAlchemy Core
        metadata_table = Table(
            'dataset_metadata',
            self.metadata,
            Column('dataset_id', String(20), primary_key=True),
            Column('dataset_name', String(255)),
            Column('table_name', String(255)),
            Column('last_ingestion', DateTime),
            Column('record_count', Integer),
            Column('status', String(50)),
            extend_existing=True
        )

        # Create table
        self.metadata.create_all(engine)

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

        # Map string types to SQLAlchemy types
        type_mapping = {
            'INTEGER': Integer,
            'VARCHAR': String,
            'TEXT': String,
            'NUMERIC': Numeric,
            'TIMESTAMP': DateTime,
            'SERIAL': Integer,  # SQLAlchemy handles auto-increment for Integer primary keys
            'GEOMETRY': Geometry,
        }

        columns = []
        for col_name, col_def in schema['columns'].items():
            # Parse type string (e.g., "VARCHAR(20)" -> String(20))
            type_str = col_def['type'].upper()
            col_type = None

            if '(' in type_str:
                base_type, args = type_str.split('(', 1)
                args = args.rstrip(')')
                base_type = base_type.strip()

                if base_type in type_mapping:
                    # Handle types with arguments like VARCHAR(20) or NUMERIC(10, 2)
                    if base_type == 'GEOMETRY':
                         # Handle Geometry('MULTIPOLYGON', srid=4326)
                         # Simple parsing for now, assuming format: GEOMETRY(TYPE, SRID) or GEOMETRY(TYPE)
                         geom_args = [a.strip() for a in args.split(',')]
                         geometry_type = geom_args[0].strip("'").strip('"')
                         srid = 4326
                         if len(geom_args) > 1:
                             srid_str = geom_args[1].lower().replace('srid=', '').strip()
                             srid = int(srid_str)
                         col_type = Geometry(geometry_type=geometry_type, srid=srid)
                    elif ',' in args:
                        arg_list = [int(a.strip()) for a in args.split(',')]
                        col_type = type_mapping[base_type](*arg_list)
                    else:
                        col_type = type_mapping[base_type](int(args))
            else:
                col_type = type_mapping.get(type_str, String)

            # Create Column object
            kwargs = {}
            if col_def.get('primary_key'):
                kwargs['primary_key'] = True
                # For SERIAL/AUTO_INCREMENT behavior in PostgreSQL with SQLAlchemy
                if type_str == 'SERIAL':
                    kwargs['autoincrement'] = True

            if not col_def.get('nullable', True):
                kwargs['nullable'] = False

            if 'default' in col_def:
                # Handle simple defaults
                if col_def['default'] == 'CURRENT_TIMESTAMP':
                    kwargs['server_default'] = text('CURRENT_TIMESTAMP')
                else:
                    kwargs['server_default'] = str(col_def['default'])

            columns.append(Column(col_name, col_type, **kwargs))

        # Add indexes to table definition
        schema_items = list(columns)
        if 'indexes' in schema:
            for index_def in schema['indexes']:
                index_name = index_def['name']
                # We can use column names directly for Index inside Table
                index_cols = index_def['columns']
                schema_items.append(Index(index_name, *index_cols))

        logger.info(f"Creating table {table_name} with columns: {[c.name for c in columns]}")

        # Define table
        table = Table(
            table_name,
            self.metadata,
            *schema_items,
            extend_existing=True
        )

        # Create table (and indexes)
        self.metadata.create_all(engine)

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

            if not records:
                return 0

            with engine.connect() as conn:
                # Reflect the table to get the Table object required by insert()
                table = Table(table_name, self.metadata, autoload_with=engine)

                # Use SQLAlchemy's PostgreSQL insert dialect for proper upsert handling
                stmt = insert(table).values(records)

                # Define what to do on conflict
                # We want to update all columns except the unique keys
                update_dict = {
                    col.name: col
                    for col in stmt.excluded
                    if col.name not in unique_columns
                }

                if update_dict:
                    upsert_stmt = stmt.on_conflict_do_update(
                        index_elements=unique_columns,
                        set_=update_dict
                    )
                else:
                    # If no columns to update (e.g. only keys), do nothing
                    upsert_stmt = stmt.on_conflict_do_nothing(
                        index_elements=unique_columns
                    )

                conn.execute(upsert_stmt)
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

        with engine.connect() as conn:
            # Reflect the table
            metadata_table = Table('dataset_metadata', self.metadata, autoload_with=engine)

            # Prepare insert statement
            stmt = insert(metadata_table).values(
                dataset_id=dataset_id,
                table_name=table_name,
                last_ingestion=text('CURRENT_TIMESTAMP'),
                record_count=record_count,
                status='success'
            )

            # Prepare upsert (on conflict update)
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=['dataset_id'],
                set_={
                    'last_ingestion': text('CURRENT_TIMESTAMP'),
                    'record_count': record_count,
                    'status': 'success'
                }
            )

            conn.execute(upsert_stmt)
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
            # Create a copy to avoid modifying the original dataframe
            df_export = df.copy()

            # Convert WKTElement objects to string for Parquet compatibility
            for col in df_export.columns:
                if df_export[col].dtype == 'object':
                    # Check if the first non-null value is a WKTElement
                    first_valid = df_export[col].dropna().first_valid_index()
                    if first_valid is not None:
                        val = df_export.loc[first_valid, col]
                        if hasattr(val, 'desc') or 'WKTElement' in str(type(val)):
                            df_export[col] = df_export[col].astype(str)

            df_export.to_parquet(
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
