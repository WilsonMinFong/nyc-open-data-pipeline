"""Food Supply Gap dataset transformer."""

from typing import Dict, Any
import pandas as pd
import numpy as np
from src.config.models import DatasetConfig
from datasets.base import BaseDatasetTransformer


class FoodSupplyGapTransformer(BaseDatasetTransformer):
    """Transformer for NYC Emergency Food Supply Gap dataset."""
    
    def __init__(self, config: DatasetConfig):
        super().__init__(config)
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Food Supply Gap data.
        
        Args:
            df: Raw DataFrame from API
            
        Returns:
            Cleaned and transformed DataFrame
        """
        # Drop SODA3 metadata columns BEFORE standardizing names
        # (columns starting with ':' like :id, :version, :created_at, :updated_at)
        metadata_cols = [col for col in df.columns if col.startswith(':')]
        if metadata_cols:
            df = df.drop(columns=metadata_cols)
        
        # Standardize column names
        df = self.standardize_column_names(df)
        
        # Map to cleaner column names based on actual API response
        column_mapping = {
            'nta': 'nta_code',
            'food_insecure_percentage': 'food_insecure_pct',
            'vulnerable_population': 'vulnerable_pop_score'
        }
        df = df.rename(columns=column_mapping)
        
        # Validate required columns
        required_columns = ['year', 'nta_code']
        self.validate_required_columns(df, required_columns)

        
        # Convert data types
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
        df['nta_code'] = df['nta_code'].astype(str).str.strip()
        df['nta_name'] = df['nta_name'].astype(str) if 'nta_name' in df.columns else None
        
        # Convert numeric columns
        numeric_columns = [
            'supply_gap_lbs', 'food_insecure_pct', 'unemployment_rate',
            'vulnerable_pop_score', 'weighted_score', 'rank'
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Validate percentage ranges
        for pct_col in ['food_insecure_pct', 'unemployment_rate']:
            if pct_col in df.columns:
                invalid_pct = (df[pct_col] < 0) | (df[pct_col] > 100)
                if invalid_pct.any():
                    print(f"Warning: Found {invalid_pct.sum()} invalid values in {pct_col}")
                    df.loc[invalid_pct, pct_col] = np.nan
        
        # Convert NaN to None for integer columns (PostgreSQL compatibility)
        # rank is stored as INTEGER in database but may have NaN values
        if 'rank' in df.columns:
            df['rank'] = df['rank'].replace({np.nan: None})
        
        # Also ensure year column doesn't have NaN (though it should be required)
        if 'year' in df.columns:
            df['year'] = df['year'].replace({np.nan: None})
        
        # Remove duplicates based on unique keys
        df = df.drop_duplicates(subset=['year', 'nta_code'], keep='last')
        
        # Add our own metadata
        df = self.add_metadata(df)
        
        # Sort by year and rank
        df = df.sort_values(['year', 'rank'], na_position='last')
        
        return df
    
    def get_schema(self) -> Dict[str, Any]:
        """
        Get database schema for Food Supply Gap table.
        
        Returns:
            Schema definition dictionary
        """
        return {
            'table_name': 'food_supply_gaps',
            'columns': {
                'id': {'type': 'SERIAL', 'primary_key': True},
                'dataset_id': {'type': 'VARCHAR(20)', 'nullable': False},
                'year': {'type': 'INTEGER', 'nullable': False},
                'nta_code': {'type': 'VARCHAR(10)', 'nullable': False},
                'nta_name': {'type': 'VARCHAR(255)', 'nullable': True},
                'supply_gap_lbs': {'type': 'NUMERIC(12, 2)', 'nullable': True},
                'food_insecure_pct': {'type': 'NUMERIC(5, 2)', 'nullable': True},
                'unemployment_rate': {'type': 'NUMERIC(5, 2)', 'nullable': True},
                'vulnerable_pop_score': {'type': 'NUMERIC(10, 2)', 'nullable': True},
                'weighted_score': {'type': 'NUMERIC(10, 2)', 'nullable': True},
                'rank': {'type': 'INTEGER', 'nullable': True},
                'ingestion_timestamp': {
                    'type': 'TIMESTAMP',
                    'default': 'CURRENT_TIMESTAMP',
                    'nullable': False
                }
            },
            'constraints': [
                'UNIQUE(dataset_id, year, nta_code)'
            ],
            'indexes': [
                {'name': 'idx_dataset_year', 'columns': ['dataset_id', 'year']},
                {'name': 'idx_nta_code', 'columns': ['nta_code']},
                {'name': 'idx_rank', 'columns': ['rank']}
            ]
        }
