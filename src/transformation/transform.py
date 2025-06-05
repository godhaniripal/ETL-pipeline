import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Ensure paths are set correctly
sys.path.append(str(Path(__file__).parent.parent.absolute()))
from src.utils import PROCESSED_DATA_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent.parent.absolute() / "etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("transformation")

def clean_disease_sh_cases(cases_df):
    """
    Clean and transform disease.sh cases data
    
    Args:
        cases_df (pd.DataFrame): Raw cases data from disease.sh
        
    Returns:
        pd.DataFrame: Cleaned and transformed data
    """
    logger.info("Cleaning disease.sh cases data...")
    
    if cases_df.empty:
        logger.warning("Empty cases dataframe provided")
        return pd.DataFrame()
    
    try:
        # Rename columns for clarity
        column_mapping = {
            'country': 'country',
            'countryInfo.iso3': 'country_code',
            'cases': 'total_cases',
            'deaths': 'total_deaths',
            'recovered': 'total_recovered',
            'active': 'active_cases',
            'critical': 'critical_cases',
            'tests': 'total_tests',
            'population': 'population',
            'continent': 'continent',
            'updated': 'last_updated'
        }
        
        # Select and rename columns
        clean_df = cases_df.copy()
        columns_to_keep = [col for col in column_mapping.keys() if col in cases_df.columns]
        clean_df = clean_df[columns_to_keep]
        clean_df = clean_df.rename(columns={k: v for k, v in column_mapping.items() if k in columns_to_keep})
        
        # Add date column (when the data was fetched)
        clean_df['date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Convert timestamp to datetime
        if 'last_updated' in clean_df.columns:
            clean_df['last_updated'] = pd.to_datetime(clean_df['last_updated'], unit='ms')
        
        # Calculate additional metrics
        if all(col in clean_df.columns for col in ['total_cases', 'population']):
            clean_df['cases_per_million'] = (clean_df['total_cases'] / clean_df['population'] * 1000000).round(2)
        
        if all(col in clean_df.columns for col in ['total_deaths', 'total_cases']):
            clean_df['case_fatality_rate'] = (clean_df['total_deaths'] / clean_df['total_cases'] * 100).round(2)
        
        if all(col in clean_df.columns for col in ['total_tests', 'population']):
            clean_df['tests_per_million'] = (clean_df['total_tests'] / clean_df['population'] * 1000000).round(2)
        
        # Data quality checks
        clean_df = apply_data_quality_checks(clean_df)
        
        logger.info(f"Successfully cleaned cases data, resulting in {len(clean_df)} records")
        return clean_df
    
    except Exception as e:
        logger.error(f"Error cleaning disease.sh cases data: {str(e)}")
        return pd.DataFrame()

# Vaccination data functions removed to improve performance

def clean_covid19api_data(covid19api_df):
    """
    Clean and transform covid19api.com data
    
    Args:
        covid19api_df (pd.DataFrame): Raw data from covid19api.com
        
    Returns:
        pd.DataFrame: Cleaned and transformed data
    """
    logger.info("Cleaning covid19api.com data...")
    
    if covid19api_df.empty:
        logger.warning("Empty covid19api dataframe provided")
        return pd.DataFrame()
    
    try:
        # Select relevant columns and rename for clarity
        clean_df = covid19api_df.copy()
        
        # Convert date columns to datetime
        date_columns = ['Date']
        for col in date_columns:
            if col in clean_df.columns:
                clean_df[col] = pd.to_datetime(clean_df[col])
        
        # Rename columns for consistency
        column_mapping = {
            'Date': 'date',
            'NewConfirmed': 'new_cases',
            'TotalConfirmed': 'total_cases',
            'NewDeaths': 'new_deaths',
            'TotalDeaths': 'total_deaths',
            'NewRecovered': 'new_recovered',
            'TotalRecovered': 'total_recovered'
        }
        
        clean_df = clean_df.rename(columns={k: v for k, v in column_mapping.items() if k in clean_df.columns})
        
        # Add country column if missing (for global data)
        if 'country' not in clean_df.columns:
            clean_df['country'] = 'Global'
        
        # Calculate rolling averages for key metrics (7-day moving average)
        for metric in ['new_cases', 'new_deaths']:
            if metric in clean_df.columns:
                clean_df[f'{metric}_7day_avg'] = clean_df.groupby('country')[metric].transform(
                    lambda x: x.rolling(7, min_periods=1).mean()
                ).round(2)
        
        # Data quality checks
        clean_df = apply_data_quality_checks(clean_df)
        
        logger.info(f"Successfully cleaned covid19api data, resulting in {len(clean_df)} records")
        return clean_df
    
    except Exception as e:
        logger.error(f"Error cleaning covid19api data: {str(e)}")
        return pd.DataFrame()

def clean_csv_data(csv_df):
    """
    Clean and transform manually uploaded CSV data
    Attempts to detect the structure and adapt accordingly
    
    Args:
        csv_df (pd.DataFrame): Raw data from CSV upload
        
    Returns:
        pd.DataFrame: Cleaned and transformed data
    """
    logger.info("Cleaning manually uploaded CSV data...")
    
    if csv_df.empty:
        logger.warning("Empty CSV dataframe provided")
        return pd.DataFrame()
    
    try:
        # Make a copy to avoid modifying the original
        clean_df = csv_df.copy()
        
        # Detect the data structure by looking at column names
        expected_case_columns = ['country', 'cases', 'deaths', 'recovered']
        expected_vaccine_columns = ['country', 'vaccinations', 'date']
        
        # Determine if it's case data or vaccine data
        if any(col.lower() in [c.lower() for c in csv_df.columns] for col in expected_case_columns):
            data_type = 'cases'
        elif any(col.lower() in [c.lower() for c in csv_df.columns] for col in expected_vaccine_columns):
            data_type = 'vaccines'
        else:
            data_type = 'unknown'
            
        logger.info(f"Detected data type from CSV: {data_type}")
        
        # Standardize column names (convert to lowercase)
        clean_df.columns = [col.lower() for col in clean_df.columns]
        
        # Try to find and format date columns
        date_column_candidates = [col for col in clean_df.columns if 'date' in col.lower()]
        for col in date_column_candidates:
            try:
                clean_df[col] = pd.to_datetime(clean_df[col])
            except:
                pass
        
        # Ensure 'country' column exists
        if 'country' not in clean_df.columns and 'location' in clean_df.columns:
            clean_df['country'] = clean_df['location']
        elif 'country' not in clean_df.columns:
            # Try to find a country-like column
            country_candidates = [col for col in clean_df.columns 
                                if any(name in col.lower() for name in ['country', 'location', 'region', 'area'])]
            if country_candidates:
                clean_df['country'] = clean_df[country_candidates[0]]
        
        # Apply common data quality checks
        clean_df = apply_data_quality_checks(clean_df)
        
        logger.info(f"Successfully cleaned CSV data, resulting in {len(clean_df)} records")
        return clean_df
    
    except Exception as e:
        logger.error(f"Error cleaning CSV data: {str(e)}")
        return pd.DataFrame()

def apply_data_quality_checks(df):
    """
    Apply common data quality checks and corrections
    
    Args:
        df (pd.DataFrame): DataFrame to check
        
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    if df.empty:
        return df
    
    # Make a copy to avoid modifying the original
    clean_df = df.copy()
    
    # Check for negative case counts and replace with 0
    numeric_columns = clean_df.select_dtypes(include=['number']).columns
    for col in numeric_columns:
        if col.startswith(('total_', 'new_', 'active_', 'daily_')):
            if (clean_df[col] < 0).any():
                neg_count = (clean_df[col] < 0).sum()
                logger.warning(f"Found {neg_count} negative values in {col}, replacing with 0")
                clean_df.loc[clean_df[col] < 0, col] = 0
    
    # Check for sudden spikes (outliers)
    for col in numeric_columns:
        if col.startswith(('new_', 'daily_')):
            # Use IQR method to detect outliers
            Q1 = clean_df[col].quantile(0.25)
            Q3 = clean_df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            # Define bounds for outliers (using a more lenient threshold for COVID data)
            lower_bound = Q1 - 3 * IQR
            upper_bound = Q3 + 3 * IQR
            
            outliers = clean_df[(clean_df[col] < lower_bound) | (clean_df[col] > upper_bound)][col]
            if len(outliers) > 0:
                outlier_count = len(outliers)
                outlier_percent = (outlier_count / len(clean_df)) * 100
                
                # Only log if significant number of outliers
                if outlier_percent > 1:
                    logger.warning(f"Potential outliers in {col}: {outlier_count} values ({outlier_percent:.1f}%)")
    
    # Handle missing values
    for col in clean_df.columns:
        missing = clean_df[col].isna().sum()
        if missing > 0:
            missing_percent = (missing / len(clean_df)) * 100
            logger.info(f"Column {col} has {missing} missing values ({missing_percent:.1f}%)")
            
            # For numeric columns, fill missing values with 0
            if col in numeric_columns and col.startswith(('total_', 'new_', 'active_', 'daily_')):
                clean_df[col] = clean_df[col].fillna(0)
    
    return clean_df

def calculate_trends_and_metrics(df, date_col='date', group_col='country'):
    """
    Calculate additional trend metrics for time-series data
    
    Args:
        df (pd.DataFrame): DataFrame with time-series data
        date_col (str): Name of date column
        group_col (str): Name of grouping column (e.g., country)
        
    Returns:
        pd.DataFrame: DataFrame with added metrics
    """
    if df.empty or date_col not in df.columns or group_col not in df.columns:
        return df
    
    result_df = df.copy()
    
    try:
        # Ensure date is in datetime format
        result_df[date_col] = pd.to_datetime(result_df[date_col])
        
        # Sort by group and date
        result_df = result_df.sort_values([group_col, date_col])
        
        # Calculate metrics for columns starting with 'new_' or 'daily_'
        metric_cols = [col for col in result_df.columns if col.startswith(('new_', 'daily_'))]
        
        for col in metric_cols:
            # Calculate 7-day moving average
            result_df[f'{col}_7day_avg'] = result_df.groupby(group_col)[col].transform(
                lambda x: x.rolling(7, min_periods=1).mean()
            ).round(2)
            
            # Calculate 14-day moving average
            result_df[f'{col}_14day_avg'] = result_df.groupby(group_col)[col].transform(
                lambda x: x.rolling(14, min_periods=1).mean()
            ).round(2)
            
            # Calculate percent change from previous day
            result_df[f'{col}_pct_change'] = result_df.groupby(group_col)[col].pct_change() * 100
            
            # Clean up percent change (replace inf with NaN and then 0)
            result_df[f'{col}_pct_change'] = result_df[f'{col}_pct_change'].replace([np.inf, -np.inf], np.nan)
            result_df[f'{col}_pct_change'] = result_df[f'{col}_pct_change'].fillna(0).round(2)
        
        logger.info(f"Successfully calculated trends and metrics for {len(result_df)} records")
        return result_df
    
    except Exception as e:
        logger.error(f"Error calculating trends and metrics: {str(e)}")
        return df

def merge_covid_data(cases_df, vaccines_df=None, covid19api_df=None):
    """
    Merge different COVID-19 data sources
    
    Args:
        cases_df (pd.DataFrame): Cases data
        vaccines_df (pd.DataFrame, optional): Not used, kept for compatibility  
        covid19api_df (pd.DataFrame, optional): COVID19API data
        
    Returns:
        pd.DataFrame: Merged data
    """
    logger.info("Merging COVID-19 case data from different sources...")
    
    try:
        # Start with cases data
        if cases_df.empty:
            logger.warning("Empty cases dataframe provided for merging")
            return pd.DataFrame()
        
        merged_df = cases_df.copy()
        
        # Prepare date field for merging
        if 'date' not in merged_df.columns:
            merged_df['date'] = datetime.now().strftime('%Y-%m-%d')
        
        merged_df['date'] = pd.to_datetime(merged_df['date'])
        
        # Add data from covid19api if available
        if covid19api_df is not None and not covid19api_df.empty:
            # Only use the most recent data
            if 'date' in covid19api_df.columns:
                covid19api_df['date'] = pd.to_datetime(covid19api_df['date'])
                latest_date = covid19api_df['date'].max()
                recent_data = covid19api_df[covid19api_df['date'] == latest_date]
                
                # Select columns that don't overlap with existing data
                existing_cols = set(merged_df.columns)
                covid_cols = [col for col in recent_data.columns if col not in existing_cols and col not in ['date']]
                
                if 'country' in recent_data.columns and covid_cols:
                    merged_df = merged_df.merge(
                        recent_data[['country'] + covid_cols],
                        on='country',
                        how='left'
                    )
        
        logger.info(f"Successfully merged COVID-19 data, resulting in {len(merged_df)} records with {merged_df.shape[1]} columns")
        return merged_df
    
    except Exception as e:
        logger.error(f"Error merging COVID-19 data: {str(e)}")
        return cases_df  # Return the original cases data if merging fails

def save_processed_data(df, filename_prefix="covid_data"):
    """
    Save processed data to the processed data folder
    
    Args:
        df (pd.DataFrame): Data to save
        filename_prefix (str): Prefix for the filename
        
    Returns:
        str: Path to saved file
    """
    if df.empty:
        logger.warning("Empty dataframe provided, nothing to save")
        return None
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = PROCESSED_DATA_PATH / f"{filename_prefix}_{timestamp}.csv"
        
        # Ensure directory exists
        os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
        
        # Save to CSV
        df.to_csv(file_path, index=False)
        logger.info(f"Successfully saved processed data to {file_path}")
        
        return str(file_path)
    
    except Exception as e:
        logger.error(f"Error saving processed data: {str(e)}")
        return None

# Import at the end to avoid circular imports
import os
