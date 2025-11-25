"""Data fetcher for NYC Open Data API."""

import time
from typing import Dict, Any, Optional, List
import requests
import pandas as pd
from pathlib import Path

from src.config.settings import settings
from src.config.models import DatasetConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataFetcher:
    """Fetches data from NYC Open Data SODA3 API."""

    def __init__(self, dataset_config: DatasetConfig):
        """
        Initialize the data fetcher.

        Args:
            dataset_config: Dataset configuration
        """
        self.dataset_config = dataset_config
        self.api_token = settings.config.api_token
        self.session = requests.Session()

        # Set up authentication header if token is provided
        if self.api_token:
            self.session.headers.update({
                'X-App-Token': self.api_token
            })

    def fetch_from_api(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Fetch data from SODA3 API using POST requests.

        Args:
            filters: Optional filter parameters (e.g., {'year': 2023})
            limit: Optional limit on number of records per page

        Returns:
            DataFrame with fetched data
        """

        endpoint = self.dataset_config.api.endpoint
        if not endpoint:
            base_url = settings.config.api_base_url.rstrip('/')
            dataset_id = self.dataset_config.dataset.id
            endpoint = f"{base_url}/{dataset_id}/query.json"

        page_size = limit or self.dataset_config.api.limit
        timeout = self.dataset_config.api.timeout

        logger.info(f"Fetching data from SODA3 API: {endpoint}")

        # Build SoQL query
        soql_query = "SELECT *"

        # Add WHERE clause if filters provided
        if filters:
            where_clauses = []
            for key, value in filters.items():
                # Handle different value types
                if isinstance(value, str):
                    where_clauses.append(f"{key} = '{value}'")
                else:
                    where_clauses.append(f"{key} = {value}")

            if where_clauses:
                soql_query += " WHERE " + " AND ".join(where_clauses)

        all_data = []
        page_number = 1

        while True:
            # Build request body for SODA3
            request_body = {
                "query": soql_query,
                "page": {
                    "pageNumber": page_number,
                    "pageSize": page_size
                }
            }

            try:
                response = self._make_request(endpoint, request_body, timeout)
                data = response.json()

                # SODA3 returns data in different formats, handle both
                if isinstance(data, dict) and 'data' in data:
                    records = data['data']
                elif isinstance(data, list):
                    records = data
                else:
                    logger.warning(f"Unexpected response format: {type(data)}")
                    records = []

                if not records:
                    break

                all_data.extend(records)
                logger.info(f"Fetched {len(records)} records from page {page_number} (total: {len(all_data)})")

                # If we got fewer records than the page size, we're done
                if len(records) < page_size:
                    break

                page_number += 1

            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed: {e}")
                raise

        logger.info(f"Total records fetched: {len(all_data)}")
        return pd.DataFrame(all_data)


    def _make_request(
        self,
        url: str,
        request_body: Dict[str, Any],
        timeout: int,
        max_retries: int = 3
    ) -> requests.Response:
        """
        Make HTTP POST request with retry logic for SODA3 API.

        Args:
            url: Request URL
            request_body: JSON request body with query and page parameters
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts

        Returns:
            Response object
        """
        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    url,
                    json=request_body,
                    timeout=timeout,
                    headers={'Content-Type': 'application/json'}
                )

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                return response

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Request timeout. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Request failed: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise

        raise requests.exceptions.RequestException("Max retries exceeded")


    def fetch_from_csv(self, csv_path: str) -> pd.DataFrame:
        """
        Load data from CSV file.

        Args:
            csv_path: Path to CSV file

        Returns:
            DataFrame with loaded data
        """
        logger.info(f"Loading data from CSV: {csv_path}")

        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} records from CSV")
            return df
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            raise

    def download_csv(self, output_path: Optional[Path] = None) -> Path:
        """
        Download dataset as CSV from NYC Open Data.

        Args:
            output_path: Optional output path for CSV file

        Returns:
            Path to downloaded CSV file
        """
        dataset_id = self.dataset_config.dataset.id
        csv_url = f"https://data.cityofnewyork.us/api/views/{dataset_id}/rows.csv"

        if output_path is None:
            output_path = settings.get_data_path('raw') / f"{dataset_id}.csv"

        logger.info(f"Downloading CSV from {csv_url}")

        try:
            # CSV download uses simple GET request, not SODA3 POST
            response = self.session.get(csv_url, timeout=self.dataset_config.api.timeout)
            response.raise_for_status()

            output_path.write_bytes(response.content)
            logger.info(f"CSV downloaded to {output_path}")
            return output_path

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download CSV: {e}")
            raise
