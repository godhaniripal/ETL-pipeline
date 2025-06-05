import os
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
import time

# Ensure paths are set correctly
sys.path.append(str(Path(__file__).absolute().parent))

# Import modules
from src.extraction.extract import (
    fetch_disease_sh_data, fetch_covid19api_data, load_csv_data
)
import reset_db
from src.transformation.transform import (
    clean_disease_sh_cases, clean_covid19api_data, clean_csv_data,
    calculate_trends_and_metrics, merge_covid_data,
    save_processed_data
)
from src.loading.load import (
    initialize_database, get_country_mappings, load_country_data,
    load_cases_data, check_database_connection
)
# Import parallel loading module for cases only
from src.loading.parallel_load import (
    parallel_load_cases
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("etl_pipeline")

def run_etl_pipeline(csv_file_path=None, use_parallel=True):
    """
    Run the complete ETL pipeline for COVID-19 case data only
    
    Args:
        csv_file_path (str, optional): Path to a CSV file for manual data input
        use_parallel (bool): Whether to use parallel processing for loading (Default: True)
        
    Returns:
        bool: True if successful, False otherwise
    """
    start_time = time.time()
    logger.info("Starting ETL pipeline for COVID-19 case data...")
    
    try:
        # Step 1: Extract data
        extract_start = time.time()
        logger.info("Step 1: Extracting data...")
        
        # Extract from disease.sh API (only get cases, ignore vaccines_df)
        cases_df, _ = fetch_disease_sh_data()
        
        # Extract from covid19api.com
        covid19api_df = fetch_covid19api_data()
        
        # Extract from CSV if provided
        csv_df = None
        if csv_file_path:
            csv_df = load_csv_data(csv_file_path)
            
        extract_time = time.time() - extract_start
        logger.info(f"Data extraction completed in {extract_time:.2f} seconds")
        
        # Step 2: Transform data
        transform_start = time.time()
        logger.info("Step 2: Transforming data...")
        
        # Clean data
        clean_cases_df = clean_disease_sh_cases(cases_df)
        clean_covid19api_df = clean_covid19api_data(covid19api_df)
        
        # Apply data enrichment and trend calculations
        if not clean_cases_df.empty:
            clean_cases_df = calculate_trends_and_metrics(clean_cases_df)
        
        # Process CSV data if provided
        if csv_df is not None and not csv_df.empty:
            clean_csv_df = clean_csv_data(csv_df)
            # Merge with other sources depending on the detected structure
            # This is simplified - you might want more sophisticated merging logic
        
        # Merge different data sources (pass None for vaccination data)
        merged_df = merge_covid_data(clean_cases_df, None, clean_covid19api_df)
        
        # Save processed data
        processed_file_path = save_processed_data(merged_df)
        
        transform_time = time.time() - transform_start
        logger.info(f"Data transformation completed in {transform_time:.2f} seconds")
        
        # Step 3: Load data to database
        load_start = time.time()
        logger.info("Step 3: Loading data to database...")
        
        # Check database connection
        if not check_database_connection():
            logger.error("Database connection failed, aborting data load")
            return False
        
        # Initialize database schema
        initialize_database()
          # Create a dedicated dataframe with just country information
        country_data = []
        
        # Extract countries from merged_df
        if not merged_df.empty and 'country' in merged_df.columns:
            for country in merged_df['country'].unique():
                country_data.append({'country': country})
        
        # Create a set of countries for quick lookup
        countries_set = {entry['country'] for entry in country_data}
        
        # Add additional missing countries
        missing_countries = ['Guernsey', 'Turkmenistan']
        for country_name in missing_countries:
            if country_name not in countries_set:
                country_data.append({'country': country_name})
        
        # Create dataframe from collected countries
        country_df = pd.DataFrame(country_data)
        logger.info(f"Prepared {len(country_df)} countries for loading")
        
        # Load country data and get mappings
        country_map = load_country_data(country_df)
        
        if not country_map:
            logger.error("Failed to load country data, cannot continue with case data")
            return False
        
        # Load data using either parallel or regular methods
        if use_parallel:
            logger.info("Using parallel loading for improved performance")
            # Load cases data in parallel
            cases_loaded = parallel_load_cases(merged_df, country_map)
        else:
            logger.info("Using standard loading methods")
            # Load cases data
            cases_loaded = load_cases_data(merged_df, country_map)
        
        load_time = time.time() - load_start
        total_time = time.time() - start_time
        
        logger.info(f"Data loading completed in {load_time:.2f} seconds")
        logger.info(f"ETL pipeline completed successfully in {total_time:.2f} seconds. Loaded {cases_loaded} case records.")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {str(e)}")
        return False

if __name__ == "__main__":
    import argparse
    
    # Set up command line arguments
    parser = argparse.ArgumentParser(description="COVID-19 Case Data ETL Pipeline")
    parser.add_argument("--csv", help="Path to a CSV file for manual data input")
    parser.add_argument("--standard", action="store_true", help="Use standard (non-parallel) loading")
    parser.add_argument("--reset-db", action="store_true", help="Reset database before running ETL")
    args = parser.parse_args()    # Reset database if requested
    if args.reset_db:
        print("Resetting database...")
        reset_db.reset_database()
    
    # Run the ETL pipeline
    success = run_etl_pipeline(
        csv_file_path=args.csv, 
        use_parallel=not args.standard
    )
    
    if success:
        print("ETL pipeline completed successfully!")
    else:
        print("ETL pipeline failed. Check the logs for details.")
        sys.exit(1)
