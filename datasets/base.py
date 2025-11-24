"""Base dataset transformer class for all NYC Open Data datasets."""

from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd
from datetime import datetime


class BaseDatasetTransformer(ABC):
    """
    Abstract base class for dataset-specific transformers.
    
    All dataset transformers must inherit from this class and implement
    the required abstract methods.
    """
    
    def __init__(self, dataset_id: str, dataset_name: str):
        """
        Initialize the transformer.
        
        Args:
            dataset_id: Unique identifier for the dataset
            dataset_name: Human-readable name for the dataset
        """
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
    
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw data into cleaned format.
        
        This method should implement all dataset-specific data cleaning,
        transformation, and validation logic.
        
        Args:
            df: Raw DataFrame from API/CSV
            
        Returns:
            Cleaned and transformed DataFrame
        """
        pass
    
    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """
        Return database schema definition for this dataset.
        
        Returns:
            Dictionary containing table schema definition with columns,
            types, constraints, and indexes
        """
        pass
    
    def add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add standard metadata columns to the DataFrame.
        
        Args:
            df: DataFrame to add metadata to
            
        Returns:
            DataFrame with metadata columns added
        """
        df = df.copy()
        df['dataset_id'] = self.dataset_id
        df['ingestion_timestamp'] = datetime.now()
        return df
    
    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names to snake_case.
        
        Args:
            df: DataFrame with original column names
            
        Returns:
            DataFrame with standardized column names
        """
        df = df.copy()
        df.columns = (
            df.columns
            .str.lower()
            .str.replace(r'[^\w\s]', '', regex=True)
            .str.replace(r'\s+', '_', regex=True)
        )
        return df
    
    def validate_required_columns(self, df: pd.DataFrame, required_columns: list) -> None:
        """
        Validate that all required columns are present.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            
        Raises:
            ValueError: If any required columns are missing
        """
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(
                f"Missing required columns for {self.dataset_name}: {missing_columns}"
            )
