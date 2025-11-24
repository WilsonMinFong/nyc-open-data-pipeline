"""Data parser and validator."""

import importlib
from typing import Optional
import pandas as pd

from src.config.models import DatasetConfig
from src.utils.logger import get_logger
from datasets.base import BaseDatasetTransformer

logger = get_logger(__name__)


class DataParser:
    """Parses and validates data using dataset-specific transformers."""
    
    def __init__(self, dataset_config: DatasetConfig, transformer_class_path: str):
        """
        Initialize the data parser.
        
        Args:
            dataset_config: Dataset configuration
            transformer_class_path: Full path to transformer class
                                   (e.g., "datasets.food_supply_gap.transformer.FoodSupplyGapTransformer")
        """
        self.dataset_config = dataset_config
        self.transformer = self._load_transformer(transformer_class_path)
    
    def _load_transformer(self, class_path: str) -> BaseDatasetTransformer:
        """
        Dynamically load transformer class.
        
        Args:
            class_path: Full path to transformer class
            
        Returns:
            Transformer instance
        """
        try:
            module_path, class_name = class_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            transformer_class = getattr(module, class_name)
            return transformer_class()
        except Exception as e:
            logger.error(f"Failed to load transformer {class_path}: {e}")
            raise
    
    def parse(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse and transform raw data.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Cleaned and transformed DataFrame
        """
        logger.info(f"Parsing data with {self.transformer.__class__.__name__}")
        logger.info(f"Input shape: {df.shape}")
        
        try:
            # Apply dataset-specific transformation
            transformed_df = self.transformer.transform(df)
            
            logger.info(f"Output shape: {transformed_df.shape}")
            logger.info(f"Columns: {list(transformed_df.columns)}")
            
            # Validate against schema
            self._validate_schema(transformed_df)
            
            return transformed_df
            
        except Exception as e:
            logger.error(f"Data parsing failed: {e}")
            raise
    
    def _validate_schema(self, df: pd.DataFrame) -> None:
        """
        Validate DataFrame against dataset schema.
        
        Args:
            df: DataFrame to validate
        """
        schema = self.dataset_config.schema.columns
        
        for col_name, col_schema in schema.items():
            # Check required columns
            if col_schema.required and col_name not in df.columns:
                raise ValueError(f"Required column '{col_name}' is missing")
            
            if col_name in df.columns:
                # Check numeric ranges
                if col_schema.min is not None:
                    invalid_count = (df[col_name] < col_schema.min).sum()
                    if invalid_count > 0:
                        logger.warning(
                            f"Column '{col_name}' has {invalid_count} values below minimum {col_schema.min}"
                        )
                
                if col_schema.max is not None:
                    invalid_count = (df[col_name] > col_schema.max).sum()
                    if invalid_count > 0:
                        logger.warning(
                            f"Column '{col_name}' has {invalid_count} values above maximum {col_schema.max}"
                        )
        
        # Check for duplicates if not allowed
        validation = self.dataset_config.validation
        if not validation.allow_duplicates and validation.unique_keys:
            duplicates = df.duplicated(subset=validation.unique_keys, keep=False)
            if duplicates.any():
                dup_count = duplicates.sum()
                logger.warning(f"Found {dup_count} duplicate records based on {validation.unique_keys}")
        
        logger.info("Schema validation completed")
