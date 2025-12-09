#!/usr/bin/env python3
"""Main data ingestion script for Poverty NYC pipeline."""

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Dict, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.settings import settings
from src.ingestion.nyc_open_data_fetcher import NYCOpenDataFetcher
from src.ingestion.census_fetcher import CensusFetcher
from src.ingestion.shapefile_fetcher import ShapefileFetcher
from src.ingestion.url_fetcher import UrlFetcher
from src.ingestion.parser import DataParser
from src.ingestion.storage import DataStorage
from src.utils.logger import setup_logger

logger = setup_logger(
    'ingestion',
    log_level=settings.config.log_level,
    log_file=settings.config.log_file
)


def ingest_dataset(
    dataset_key: str,
    source: str = 'api',
    filters: Optional[Dict[str, Any]] = None,
    force: bool = False,
    dry_run: bool = False
) -> None:
    """
    Ingest a single dataset.

    Args:
        dataset_key: Dataset key from registry
        source: Data source ('api' or 'csv')
        filters: Optional filter parameters
        limit: Optional limit for records fetched
        force: Force re-download even if data exists
        dry_run: Preview without storing
    """
    try:
        logger.info(f"Starting ingestion for dataset: {dataset_key}")
        logger.info(f"Source: {source}, Filters: {filters}, Dry run: {dry_run}")

        # Load dataset configuration
        registry = settings.get_registry()
        dataset_entry = registry.get_dataset(dataset_key)

        if not dataset_entry:
            raise ValueError(f"Dataset '{dataset_key}' not found in registry")

        if not dataset_entry.enabled:
            logger.warning(f"Dataset '{dataset_key}' is disabled in registry")
            return

        dataset_config = settings.get_dataset_config(dataset_key)

        # Initialize components
        if dataset_config.source_type == 'census_api':
            fetcher = CensusFetcher(dataset_config)
        elif dataset_config.source_type == 'shapefile_download':
            fetcher = ShapefileFetcher(dataset_config)
        elif dataset_config.source_type == 'url_download':
            fetcher = UrlFetcher(dataset_config)
        else:
            fetcher = NYCOpenDataFetcher(dataset_config)

        parser = DataParser(dataset_config, dataset_entry.transformer_class)
        storage = DataStorage()

        # Fetch data
        logger.info("=" * 60)
        logger.info("STEP 1: Fetching data")
        logger.info("=" * 60)

        if source == 'api':
            if dataset_config.source_type == 'shapefile_download':
                df_raw = fetcher.fetch_data(force=force)
            elif dataset_config.source_type == 'url_download':
                df_raw = fetcher.fetch_data(force=force)
            else:
                df_raw = fetcher.fetch_from_api(filters=filters)
        elif source == 'csv':
            csv_path = settings.get_data_path('raw') / f"{dataset_entry.dataset_id}.csv"
            if not csv_path.exists() or force:
                csv_path = fetcher.download_csv()
            df_raw = fetcher.fetch_from_csv(str(csv_path))
        else:
            raise ValueError(f"Invalid source: {source}")

        if df_raw.empty:
            logger.warning("No data fetched. Exiting.")
            return

        # Parse and transform data
        logger.info("=" * 60)
        logger.info("STEP 2: Parsing and transforming data")
        logger.info("=" * 60)

        df_transformed = parser.parse(df_raw)

        if dry_run:
            logger.info("=" * 60)
            logger.info("DRY RUN - Preview of transformed data:")
            logger.info("=" * 60)
            logger.info(f"\n{df_transformed.head(10)}")
            logger.info(f"\nData types:\n{df_transformed.dtypes}")
            logger.info(f"\nSummary statistics:\n{df_transformed.describe()}")
            logger.info("Dry run completed. No data stored.")
            return

        # Store data
        logger.info("=" * 60)
        logger.info("STEP 3: Storing data")
        logger.info("=" * 60)

        # Enable PostGIS
        storage.enable_postgis()

        # Create metadata table
        storage.create_metadata_table()

        # Get schema
        schema = parser.transformer.get_schema()

        # Create table
        storage.create_table_from_schema(schema)

        # Upsert data (update if exists, insert if new)
        unique_columns = dataset_config.validation.unique_keys
        if unique_columns:
            record_count = storage.upsert_data(
                df_transformed,
                dataset_entry.table_name,
                dataset_entry.dataset_id,
                unique_columns
            )
        else:
            record_count = storage.store_data(
                df_transformed,
                dataset_entry.table_name,
                dataset_entry.dataset_id,
                if_exists='append'
            )

        # Export to Parquet
        logger.info("=" * 60)
        logger.info("STEP 4: Exporting to Parquet")
        logger.info("=" * 60)

        parquet_path = storage.export_to_parquet(
            df_transformed,
            dataset_entry.dataset_id
        )

        # Summary
        logger.info("=" * 60)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Dataset: {dataset_entry.name}")
        logger.info(f"Records processed: {record_count}")
        logger.info(f"Table: {dataset_entry.table_name}")
        logger.info(f"Parquet file: {parquet_path}")
        logger.info("Ingestion completed successfully!")

    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        raise
    finally:
        if 'storage' in locals():
            storage.close()


def ingest_all_datasets(
    source: str = 'api',
    filters: Optional[Dict[str, Any]] = None,
    force: bool = False,
    dry_run: bool = False
) -> None:
    """
    Ingest all enabled datasets.

    Args:
        source: Data source ('api' or 'csv')
        filters: Optional filter parameters
        force: Force re-download even if data exists
        dry_run: Preview without storing
    """
    registry = settings.get_registry()
    enabled_datasets = registry.get_enabled_datasets()

    logger.info(f"Found {len(enabled_datasets)} enabled datasets")

    for dataset_key in enabled_datasets.keys():
        logger.info(f"\n{'=' * 80}")
        logger.info(f"Processing dataset: {dataset_key}")
        logger.info(f"{'=' * 80}\n")

        try:
            ingest_dataset(dataset_key, source, filters, force, dry_run)
        except Exception as e:
            logger.error(f"Failed to ingest {dataset_key}: {e}")
            logger.info("Continuing with next dataset...")
            continue


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Poverty NYC Data Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest single dataset from API
  python scripts/ingest_data.py --dataset food_supply_gap

  # Ingest all enabled datasets
  python scripts/ingest_data.py --dataset all

  # Ingest with filters
  python scripts/ingest_data.py --dataset food_supply_gap --filter '{"year": 2023}'

  # Dry run to preview data
  python scripts/ingest_data.py --dataset food_supply_gap --dry-run

  # Download and use CSV instead of API
  python scripts/ingest_data.py --dataset food_supply_gap --source csv
        """
    )

    parser.add_argument(
        '--dataset',
        required=True,
        help='Dataset key from registry or "all" for all enabled datasets'
    )

    parser.add_argument(
        '--source',
        choices=['api', 'csv'],
        default='api',
        help='Data source (default: api)'
    )

    parser.add_argument(
        '--filter',
        type=str,
        help='JSON filter parameters (e.g., \'{"year": 2023}\')'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-download even if data exists'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview data without storing'
    )

    args = parser.parse_args()

    # Parse filters if provided
    filters = None
    if args.filter:
        try:
            filters = json.loads(args.filter)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid filter JSON: {e}")
            sys.exit(1)

    # Run ingestion
    try:
        if args.dataset == 'all':
            ingest_all_datasets(args.source, filters, args.force, args.dry_run)
        else:
            ingest_dataset(args.dataset, args.source, filters, args.force, args.dry_run)
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
