"""Application settings and configuration management."""

import os
from pathlib import Path
from typing import Optional
import yaml
from dotenv import load_dotenv
from src.config.models import AppConfig, DatabaseConfig, DatasetRegistry, DatasetConfig

# Load environment variables
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent


class Settings:
    """Application settings manager."""
    
    def __init__(self):
        """Initialize settings from environment variables."""
        self.config = AppConfig(
            api_token=os.getenv('NYC_OPEN_DATA_API_TOKEN', ''),
            api_base_url=os.getenv(
                'NYC_OPEN_DATA_API_BASE_URL',
                'https://data.cityofnewyork.us/api/v3/views'
            ),
            database=DatabaseConfig(
                host=os.getenv('POSTGRES_HOST', 'localhost'),
                port=int(os.getenv('POSTGRES_PORT', '5432')),
                database=os.getenv('POSTGRES_DB', 'nyc_open_data'),
                user=os.getenv('POSTGRES_USER', ''),
                password=os.getenv('POSTGRES_PASSWORD', '')
            ),
            raw_data_path=os.getenv('RAW_DATA_PATH', 'data/raw'),
            processed_data_path=os.getenv('PROCESSED_DATA_PATH', 'data/processed'),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            log_file=os.getenv('LOG_FILE', 'logs/ingestion.log')
        )
        
        self._registry: Optional[DatasetRegistry] = None
    
    def get_registry(self) -> DatasetRegistry:
        """
        Load and return the dataset registry.
        
        Returns:
            DatasetRegistry instance
        """
        if self._registry is None:
            registry_path = PROJECT_ROOT / 'datasets' / 'registry.yaml'
            with open(registry_path, 'r') as f:
                registry_data = yaml.safe_load(f)
            self._registry = DatasetRegistry(**registry_data)
        return self._registry
    
    def get_dataset_config(self, dataset_key: str) -> DatasetConfig:
        """
        Load dataset-specific configuration.
        
        Args:
            dataset_key: Dataset key from registry
            
        Returns:
            DatasetConfig instance
        """
        registry = self.get_registry()
        dataset_entry = registry.get_dataset(dataset_key)
        
        if not dataset_entry:
            raise ValueError(f"Dataset '{dataset_key}' not found in registry")
        
        config_path = PROJECT_ROOT / dataset_entry.config_path
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        return DatasetConfig(**config_data)
    
    def get_data_path(self, path_type: str = 'raw') -> Path:
        """
        Get data directory path.
        
        Args:
            path_type: Type of path ('raw' or 'processed')
            
        Returns:
            Path to data directory
        """
        if path_type == 'raw':
            path = PROJECT_ROOT / self.config.raw_data_path
        elif path_type == 'processed':
            path = PROJECT_ROOT / self.config.processed_data_path
        else:
            raise ValueError(f"Invalid path type: {path_type}")
        
        # Create directory if it doesn't exist
        path.mkdir(parents=True, exist_ok=True)
        return path


# Global settings instance
settings = Settings()
