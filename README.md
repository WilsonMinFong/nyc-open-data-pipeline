# NYC Open Data Ingestion Pipeline

An extensible, configuration-driven data ingestion pipeline for NYC Open Data datasets. Built with Python, PostgreSQL, and designed for easy addition of new datasets.

## Features

- 🔌 **Extensible Architecture**: Add new datasets with just configuration + transformer
- 🗄️ **PostgreSQL Storage**: Production-grade database with ACID guarantees
- 📊 **Parquet Export**: Optional export for analytics workflows
- 🔄 **SODA3 API Support**: Automated data fetching with pagination and rate limiting
- ✅ **Data Validation**: Schema validation and data quality checks
- 🎯 **Type-Safe Configuration**: Pydantic models for configuration validation
- 📝 **Comprehensive Logging**: Detailed logs for monitoring and debugging

## Project Structure

```
nyc-open-data-pipeline/
├── datasets/               # Dataset configurations
│   ├── registry.yaml      # Central dataset registry
│   ├── base.py           # Base transformer class
│   └── food_supply_gap/  # Example dataset
├── src/
│   ├── ingestion/        # Core ingestion logic
│   ├── config/           # Configuration management
│   └── utils/            # Utilities (logging, etc.)
├── scripts/
│   └── ingest_data.py    # Main ingestion script
├── data/
│   ├── raw/              # Raw data from API
│   └── processed/        # Processed Parquet files
└── tests/                # Unit tests
```

## Installation

### Prerequisites

- Python 3.10+
- PostgreSQL 12+
- NYC Open Data API token (optional but recommended)

### Setup

1. **Clone or navigate to the project**:
   ```bash
   cd /Users/wilson/.gemini/antigravity/scratch/nyc-open-data-pipeline
   ```

2. **Install dependencies with uv** (recommended):
   ```bash
   # Install uv if not already installed
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # Install dependencies
   uv pip install -e .
   
   # Or with dev dependencies
   uv pip install -e ".[dev]"
   ```

   **Or with pip**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up PostgreSQL**:
   ```bash
   # Create database
   createdb nyc_open_data
   
   # Or using Docker
   docker run --name nyc-postgres \
     -e POSTGRES_DB=nyc_open_data \
     -e POSTGRES_USER=your_username \
     -e POSTGRES_PASSWORD=your_password \
     -p 5432:5432 \
     -d postgres:15
   ```

4. **Configure environment variables**:
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

   Required variables:
   ```bash
   NYC_OPEN_DATA_API_TOKEN=your_api_token_here
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DB=nyc_open_data
   POSTGRES_USER=your_username
   POSTGRES_PASSWORD=your_password
   ```

### Getting an API Token

1. Go to [NYC Open Data](https://data.cityofnewyork.us/)
2. Sign up or log in
3. Go to Developer Settings
4. Create a new application token
5. Copy the token to your `.env` file

## Usage

### Ingest a Single Dataset

```bash
# Ingest Food Supply Gap dataset from API
python scripts/ingest_data.py --dataset food_supply_gap

# Dry run to preview data without storing
python scripts/ingest_data.py --dataset food_supply_gap --dry-run

# Ingest with filters
python scripts/ingest_data.py --dataset food_supply_gap --filter '{"year": 2023}'

# Use CSV instead of API
python scripts/ingest_data.py --dataset food_supply_gap --source csv
```

### Ingest All Enabled Datasets

```bash
python scripts/ingest_data.py --dataset all
```

### Command-Line Options

- `--dataset`: Dataset key from registry or "all" (required)
- `--source`: Data source - "api" or "csv" (default: api)
- `--filter`: JSON filter parameters
- `--force`: Force re-download even if data exists
- `--dry-run`: Preview data without storing

## Available Datasets

Currently configured datasets:

| Dataset | ID | Table | Status | Update Frequency |
|---------|----|----|--------|------------------|
| Emergency Food Supply Gap | `4kc9-zrs2` | `food_supply_gap` | ✅ Enabled | Annually |

## Adding New Datasets

Adding a new NYC Open Data dataset is simple:

### 1. Create Dataset Directory

```bash
mkdir -p datasets/your_dataset_name
touch datasets/your_dataset_name/__init__.py
touch datasets/your_dataset_name/config.yaml
touch datasets/your_dataset_name/transformer.py
```

### 2. Define Configuration (`config.yaml`)

```yaml
dataset:
  id: "your-dataset-id"
  name: "Your Dataset Name"
  description: "Dataset description"

api:
  endpoint: "https://data.cityofnewyork.us/resource/your-id.json"
  limit: 1000
  timeout: 30

schema:
  columns:
    column_name:
      type: "string"
      required: true
      max_length: 255

validation:
  allow_duplicates: false
  unique_keys: ["id"]
```

### 3. Implement Transformer (`transformer.py`)

```python
from datasets.base import BaseDatasetTransformer
import pandas as pd

class YourDatasetTransformer(BaseDatasetTransformer):
    def __init__(self):
        super().__init__(
            dataset_id="your-dataset-id",
            dataset_name="Your Dataset Name"
        )
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Your custom transformation logic
        df = self.standardize_column_names(df)
        df = self.add_metadata(df)
        return df
    
    def get_schema(self) -> dict:
        # Return database schema definition
        return {
            'table_name': 'your_table_name',
            'columns': {...},
            'constraints': [...],
            'indexes': [...]
        }
```

### 4. Register in Registry (`datasets/registry.yaml`)

```yaml
datasets:
  your_dataset_name:
    name: "Your Dataset Name"
    dataset_id: "your-dataset-id"
    table_name: "your_table_name"
    enabled: true
    update_frequency: "daily"
    config_path: "datasets/your_dataset_name/config.yaml"
    transformer_class: "datasets.your_dataset_name.transformer.YourDatasetTransformer"
```

### 5. Run Ingestion

```bash
python scripts/ingest_data.py --dataset your_dataset_name
```

## Storage Strategy

### PostgreSQL (Primary Storage)

- **Purpose**: Production database for querying and analysis
- **Benefits**: ACID compliance, complex queries, multi-user access
- **Use for**: Live queries, BI tools, applications

### Parquet (Secondary Export)

- **Purpose**: Analytics-optimized file format
- **Benefits**: Columnar storage, excellent compression, fast reads
- **Use for**: Data science workflows, archival, sharing

## Database Schema

### Food Supply Gap Table

```sql
CREATE TABLE food_supply_gap (
    id SERIAL PRIMARY KEY,
    dataset_id VARCHAR(20) NOT NULL,
    year INTEGER NOT NULL,
    nta_code VARCHAR(10) NOT NULL,
    nta_name VARCHAR(255),
    supply_gap_lbs NUMERIC(12, 2),
    food_insecure_pct NUMERIC(5, 2),
    unemployment_rate NUMERIC(5, 2),
    vulnerable_pop_score NUMERIC(10, 2),
    weighted_score NUMERIC(10, 2),
    rank INTEGER,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_id, year, nta_code)
);
```

### Metadata Table

```sql
CREATE TABLE dataset_metadata (
    dataset_id VARCHAR(20) PRIMARY KEY,
    dataset_name VARCHAR(255),
    table_name VARCHAR(255),
    last_ingestion TIMESTAMP,
    record_count INTEGER,
    status VARCHAR(50)
);
```

## Querying Data

### Using PostgreSQL

```sql
-- Get latest data for all neighborhoods
SELECT * FROM food_supply_gap WHERE year = 2023 ORDER BY rank;

-- Top 10 neighborhoods by food supply gap
SELECT nta_name, supply_gap_lbs, food_insecure_pct
FROM food_supply_gap
WHERE year = 2023
ORDER BY supply_gap_lbs DESC
LIMIT 10;

-- Check ingestion status
SELECT * FROM dataset_metadata;
```

### Using Python

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://user:password@localhost/nyc_open_data')
df = pd.read_sql('SELECT * FROM food_supply_gap', engine)
```

### Using Parquet

```python
import pandas as pd

df = pd.read_parquet('data/processed/4kc9-zrs2.parquet')
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Formatting

```bash
black src/ datasets/ scripts/
ruff check src/ datasets/ scripts/
```

## Architecture Benefits

1. **Easy to Add Datasets**: New datasets require only config + transformer
2. **Consistent Processing**: All datasets use the same ingestion pipeline
3. **Centralized Management**: Single registry to enable/disable datasets
4. **Type Safety**: Pydantic models ensure configuration validity
5. **Maintainable**: Clear separation between generic and dataset-specific logic
6. **Testable**: Each transformer can be unit tested independently

## Troubleshooting

### Connection Errors

- Verify PostgreSQL is running: `pg_isready`
- Check `.env` file has correct credentials
- Ensure database exists: `psql -l`

### API Rate Limiting

- Get an API token from NYC Open Data
- The pipeline automatically handles rate limits with exponential backoff
- Consider using CSV source for large datasets

### Import Errors

- Ensure you're running from project root
- Check Python path includes project directory
- Verify all dependencies are installed

## Resources

- [NYC Open Data Portal](https://opendata.cityofnewyork.us/)
- [SODA3 API Documentation](https://dev.socrata.com/foundry/data.cityofnewyork.us/4kc9-zrs2)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Parquet Format](https://parquet.apache.org/)

## License

This project is for data analysis and educational purposes.

## Contributing

When adding new datasets:
1. Follow the existing pattern in `datasets/food_supply_gap/`
2. Add comprehensive docstrings
3. Include validation rules in config
4. Test with `--dry-run` before full ingestion
