import os
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
import sys
from pathlib import Path

# Ensure paths are set correctly
sys.path.append(str(Path(__file__).parent.parent.absolute()))
from src.utils import DISEASE_SH_API, COVID19_API, RAW_DATA_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent.parent.absolute() / "etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("extraction")

def fetch_disease_sh_data(save_to_disk=True):
    """
    Fetch COVID-19 data from disease.sh API
    
    Returns:
        tuple: (cases_df, vaccines_df)
    """
    logger.info("Fetching data from disease.sh API...")
    
    # Fetch current global data with country details
    try:
        cases_response = requests.get(f"{DISEASE_SH_API}/countries")
        cases_response.raise_for_status()
        cases_data = cases_response.json()
        cases_df = pd.json_normalize(cases_data)
        logger.info(f"Successfully fetched case data for {len(cases_df)} countries")
    except Exception as e:
        logger.error(f"Error fetching country data from disease.sh: {str(e)}")
        cases_df = pd.DataFrame()
    
    # Fetch global vaccine data
    try:
        vaccine_response = requests.get(f"{DISEASE_SH_API}/vaccine/coverage/countries?lastdays=all")
        vaccine_response.raise_for_status()
        vaccine_data = vaccine_response.json()
        
        # Process vaccine data - normalize the nested timeseries data
        vaccine_dfs = []
        for country in vaccine_data:
            country_name = country.get('country')
            timeline = country.get('timeline', {})
            
            # Convert timeline to rows
            for date, doses in timeline.items():
                vaccine_dfs.append({
                    'country': country_name,
                    'date': date,
                    'total_vaccinations': doses
                })
        
        vaccines_df = pd.DataFrame(vaccine_dfs)
        if not vaccines_df.empty:
            vaccines_df['date'] = pd.to_datetime(vaccines_df['date'])
        logger.info(f"Successfully fetched vaccination data with {len(vaccines_df)} records")
    except Exception as e:
        logger.error(f"Error fetching vaccine data from disease.sh: {str(e)}")
        vaccines_df = pd.DataFrame()
    
    # Save raw data to disk if requested
    if save_to_disk:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        cases_path = RAW_DATA_PATH / f"disease_sh_cases_{timestamp}.csv"
        vaccines_path = RAW_DATA_PATH / f"disease_sh_vaccines_{timestamp}.csv"
        
        os.makedirs(RAW_DATA_PATH, exist_ok=True)
        
        if not cases_df.empty:
            cases_df.to_csv(cases_path, index=False)
            logger.info(f"Saved cases data to {cases_path}")
        
        if not vaccines_df.empty:
            vaccines_df.to_csv(vaccines_path, index=False)
            logger.info(f"Saved vaccine data to {vaccines_path}")
    
    return cases_df, vaccines_df

def fetch_covid19api_data(save_to_disk=True):
    """
    Fetch COVID-19 data from covid19api.com
    
    Returns:
        pd.DataFrame: Historical COVID-19 data
    """
    logger.info("Fetching data from covid19api.com...")
    
    # Calculate dates for the last 30 days (limit data for performance)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    try:
        # Fetch country summaries for the date range
        response = requests.get(
            f"{COVID19_API}/world?from={start_date}T00:00:00Z&to={end_date}T23:59:59Z"
        )
        response.raise_for_status()
        data = response.json()
        
        # Convert to DataFrame
        df = pd.json_normalize(data)
        logger.info(f"Successfully fetched {len(df)} records from covid19api.com")
        
        # Save raw data to disk if requested
        if save_to_disk and not df.empty:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = RAW_DATA_PATH / f"covid19api_data_{timestamp}.csv"
            
            os.makedirs(RAW_DATA_PATH, exist_ok=True)
            df.to_csv(file_path, index=False)
            logger.info(f"Saved covid19api data to {file_path}")
        
        return df
    except Exception as e:
        logger.error(f"Error fetching data from covid19api.com: {str(e)}")
        return pd.DataFrame()

def load_csv_data(file_path, save_to_disk=True):
    """
    Load COVID-19 data from a CSV file
    
    Args:
        file_path (str): Path to the CSV file
        save_to_disk (bool): Whether to save a copy in the raw data folder
        
    Returns:
        pd.DataFrame: Loaded data
    """
    logger.info(f"Loading CSV data from {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully loaded {len(df)} records from CSV file")
        
        # Save a copy to the raw data folder if requested
        if save_to_disk:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = os.path.basename(file_path)
            save_path = RAW_DATA_PATH / f"manual_upload_{timestamp}_{base_filename}"
            
            os.makedirs(RAW_DATA_PATH, exist_ok=True)
            df.to_csv(save_path, index=False)
            logger.info(f"Saved copy of CSV data to {save_path}")
        
        return df
    except Exception as e:
        logger.error(f"Error loading CSV data: {str(e)}")
        return pd.DataFrame()


if __name__ == "__main__":
    # Test the data extraction functions
    cases_df, vaccines_df = fetch_disease_sh_data()
    covid19api_df = fetch_covid19api_data()
    
    print(f"Disease.sh cases data shape: {cases_df.shape}")
    print(f"Disease.sh vaccine data shape: {vaccines_df.shape}")
    print(f"Covid19api data shape: {covid19api_df.shape}")
