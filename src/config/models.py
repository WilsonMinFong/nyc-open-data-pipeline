"""Pydantic models for configuration validation."""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class APIConfig(BaseModel):
    """API configuration model."""
    endpoint: Optional[str] = None
    limit: int = 1000
    timeout: int = 30


class ColumnSchema(BaseModel):
    """Column schema definition."""
    type: str
    required: bool = False
    max_length: Optional[int] = None
    min: Optional[float] = None
    max: Optional[float] = None
    description: Optional[str] = None


class ValidationConfig(BaseModel):
    """Validation configuration."""
    allow_duplicates: bool = True
    unique_keys: List[str] = Field(default_factory=list)


class DatasetConfigModel(BaseModel):
    """Dataset configuration model."""
    id: str
    name: str
    description: Optional[str] = None


class DatasetSchemaConfig(BaseModel):
    """Dataset schema configuration."""
    columns: Dict[str, ColumnSchema]


class DatasetConfig(BaseModel):
    """Complete dataset configuration."""
    dataset: DatasetConfigModel
    api: APIConfig
    schema: DatasetSchemaConfig
    validation: ValidationConfig = Field(default_factory=ValidationConfig)


class DatasetRegistryEntry(BaseModel):
    """Dataset registry entry model."""
    name: str
    dataset_id: str
    table_name: str
    enabled: bool = True
    update_frequency: str
    config_path: str
    transformer_class: str


class DatasetRegistry(BaseModel):
    """Dataset registry model."""
    datasets: Dict[str, DatasetRegistryEntry]
    
    def get_enabled_datasets(self) -> Dict[str, DatasetRegistryEntry]:
        """Get only enabled datasets."""
        return {
            key: dataset 
            for key, dataset in self.datasets.items() 
            if dataset.enabled
        }
    
    def get_dataset(self, dataset_key: str) -> Optional[DatasetRegistryEntry]:
        """Get a specific dataset by key."""
        return self.datasets.get(dataset_key)


class DatabaseConfig(BaseModel):
    """Database configuration model."""
    host: str = "localhost"
    port: int = 5432
    database: str
    user: str
    password: str
    
    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class AppConfig(BaseModel):
    """Application configuration model."""
    api_token: str
    api_base_url: str = "https://data.cityofnewyork.us/api/v3/views"
    database: DatabaseConfig
    raw_data_path: str = "data/raw"
    processed_data_path: str = "data/processed"
    log_level: str = "INFO"
    log_file: str = "logs/ingestion.log"
