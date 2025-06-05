# COVID-19 & Vaccination Tracker ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for tracking COVID-19 cases and vaccination data worldwide with visualizations.

## Features

- **Multi-source Data Extraction**:
  - [disease.sh](https://disease.sh/) API for current case and vaccination data
  - [COVID19API](https://api.covid19api.com/) for historical data
  - CSV file upload for custom data sources

- **Robust Data Transformation**:
  - Data cleaning and normalization
  - Time-series processing with historical tracking
  - Calculation of key metrics (case rates, moving averages, etc.)
  - Automated data quality checks

- **PostgreSQL Database** using [Neon](https://neon.tech/)
  - Efficient storage with proper schema design
  - Time-series historical data tracking
  - Incremental loading to avoid duplicates

- **Interactive Dashboard** with [Streamlit](https://streamlit.io/):
  - Global overview with key metrics
  - Country comparison and detailed analysis
  - Time-series visualization
  - Vaccination progress tracking
  - Choropleth maps and heatmaps

## Project Structure

```
ETL/
├── data/
│   ├── raw/         # Raw data from APIs and uploads
│   └── processed/   # Processed and cleaned data
├── src/
│   ├── extraction/  # Data extraction modules
│   ├── transformation/ # Data transformation modules
│   ├── loading/     # Database loading modules
│   └── utils.py     # Utility functions
├── dashboard/
│   └── dashboard.py # Streamlit dashboard
├── etl_pipeline.py  # Main ETL orchestration script
├── requirements.txt # Project dependencies
└── README.md        # Project documentation
```

## Setup Instructions

### Prerequisites

- Python 3.8+
- PostgreSQL database (Using Neon PostgreSQL)

### Installation

1. Clone this repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Set up environment variables in `.env` file:

```
DB_CONNECTION_STRING=postgresql://ETL_owner:npg_mhpgxqW2BO7r@ep-lively-lab-a83xnoj7-pooler.eastus2.azure.neon.tech/ETL?sslmode=require
DISEASE_SH_API=https://disease.sh/v3/covid-19
COVID19_API=https://api.covid19api.com
RAW_DATA_PATH=data/raw
PROCESSED_DATA_PATH=data/processed
```

## Usage

### Running the ETL Pipeline

To run the complete ETL pipeline:

```bash
python etl_pipeline.py
```

To process a CSV file:

```bash
python etl_pipeline.py path/to/your/file.csv
```

### Running the Dashboard

To launch the Streamlit dashboard:

```bash
streamlit run dashboard/dashboard.py
```

## Data Sources

- **disease.sh API**: Provides current COVID-19 statistics and vaccination data.
- **COVID19API**: Offers historical COVID-19 data by country.
- **CSV Upload**: Supports user-provided datasets in CSV format.

## Dashboard Features

- **Global Overview**: Summary of worldwide COVID-19 statistics
- **Country Comparison**: Compare metrics across different countries
- **Country Detail**: In-depth analysis of individual countries
- **Vaccination Analysis**: Track vaccination progress globally

## Key ETL Pipeline Features

1. **Time-Series & Historical Data Management**:
   - Tracks how data changes over time
   - Stores daily snapshots with date as a primary key
   - Enables trend analysis with 7-day/14-day moving averages

2. **Data Quality Checks**:
   - Checks for missing values
   - Identifies negative cases or deaths (data errors)
   - Detects sudden spikes that could indicate bad data

3. **Delta Loads**:
   - Only fetches new data since the last run
   - Compares hashes to identify changes
   - Avoids duplicates

4. **User-Friendly Interface**:
   - Interactive dashboard built with Streamlit
   - Select countries, date ranges, and metrics
   - Multiple visualization options

## License

This project is licensed under the MIT License.
