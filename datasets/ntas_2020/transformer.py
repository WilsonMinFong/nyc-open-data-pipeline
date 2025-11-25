"""Transformer for 2020 Neighborhood Tabulation Areas (NTAs) dataset."""

import json
from typing import Dict, Any, List
import pandas as pd
from shapely.geometry import shape
from geoalchemy2.elements import WKTElement

from datasets.base import BaseDatasetTransformer
from src.utils.logger import get_logger

logger = get_logger(__name__)


class Ntas2020Transformer(BaseDatasetTransformer):
    """Transformer for NTA 2020 dataset."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform NTA data.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Transforming {len(df)} NTA records")
        
        # Rename columns to snake_case
        column_mapping = {
            'borocode': 'boro_code',
            'boroname': 'boro_name',
            'countyfips': 'county_fips',
            'nta2020': 'nta2020',
            'ntaname': 'nta_name',
            'ntaabbrev': 'nta_abbrev',
            'ntatype': 'nta_type',
            'cdta2020': 'cdta2020',
            'cdtaname': 'cdta_name',
            'shape_leng': 'shape_leng',
            'shape_area': 'shape_area',
            'the_geom': 'geom'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Remove SODA metadata columns
        df = df[[c for c in df.columns if not c.startswith(':')]]
        
        # Convert numeric columns
        numeric_cols = ['boro_code', 'shape_leng', 'shape_area']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert geometry to WKT for PostGIS
        if 'geom' in df.columns:
            df['geom'] = df['geom'].apply(self._convert_geometry)
        
        # Add metadata
        df = self.add_metadata(df)
        
        # Get required columns from config
        required_cols = [
            name for name, schema in self.config.data_schema.columns.items()
            if schema.required or schema.primary_key
        ]
        
        # Validate required columns
        self.validate_required_columns(df, required_cols)
        
        return df
    
    def get_schema(self) -> Dict[str, Any]:
        """
        Get dataset schema.
        
        Returns:
            Schema dictionary
        """
        schema = self.config.data_schema.model_dump(by_alias=True)
        
        # Add metadata columns
        schema['columns']['dataset_id'] = {'type': 'VARCHAR(20)', 'nullable': False}
        schema['columns']['ingestion_timestamp'] = {
            'type': 'TIMESTAMP',
            'default': 'CURRENT_TIMESTAMP',
            'nullable': False
        }
        
        return schema

    def _convert_geometry(self, geom_data: Any) -> Any:
        """
        Convert GeoJSON/dict geometry to WKTElement.
        
        Args:
            geom_data: Geometry data (dict or str)
            
        Returns:
            WKTElement or None
        """
        if not geom_data:
            return None
            
        try:
            if isinstance(geom_data, str):
                geom_dict = json.loads(geom_data)
            else:
                geom_dict = geom_data
                
            # Use shapely to convert GeoJSON to WKT
            geom_shape = shape(geom_dict)
            
            # Create WKTElement with SRID 4326
            return WKTElement(geom_shape.wkt, srid=4326)
            
        except Exception as e:
            logger.warning(f"Failed to convert geometry: {e}")
            return None
