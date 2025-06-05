"""
Parallel data loading module for improved ETL performance

This module provides functions for loading COVID-19 data into the PostgreSQL database
using parallel processing techniques for improved performance.
"""

import pandas as pd
import numpy as np
import logging
import os
import sys
import hashlib
import psycopg2
import psycopg2.extras
import multiprocessing
from datetime import datetime
from sqlalchemy import create_engine, text
from pathlib import Path
from io import StringIO
from concurrent.futures import ProcessPoolExecutor, as_completed

# Ensure paths are set correctly
sys.path.append(str(Path(__file__).parent.parent.parent.absolute()))
from src.utils import get_db_connection_string

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent.parent.parent.absolute() / "etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("parallel_loading")

def set_db_optimizations(conn_string):
    """Apply PostgreSQL optimizations for faster bulk loading"""
    try:
        engine = create_engine(conn_string)
        with engine.connect() as connection:
            # Increase work memory (if permissions allow)
            try:
                connection.execute(text("SET work_mem = '64MB';"))
                logger.info("Set work_mem to 64MB")
            except Exception as e:
                logger.warning(f"Could not set work_mem: {str(e)}")
            
            try:
                connection.execute(text("SET maintenance_work_mem = '128MB';"))
                logger.info("Set maintenance_work_mem to 128MB")
            except Exception as e:
                logger.warning(f"Could not set maintenance_work_mem: {str(e)}")
                
            # Note: We're skipping session_replication_role due to permission issues
            # This would disable constraints but requires superuser privileges
            connection.commit()
            
        logger.info("Database optimizations applied for faster loading (with limited permissions)")
        return True
    except Exception as e:
        logger.error(f"Failed to apply database optimizations: {str(e)}")
        return False

def restore_db_settings(conn_string):
    """Restore PostgreSQL default settings after bulk loading"""
    try:
        engine = create_engine(conn_string)
        with engine.connect() as connection:
            # Reset to default values (if permissions allow)
            try:
                connection.execute(text("SET work_mem = '4MB';"))
                logger.info("Reset work_mem to default")
            except Exception as e:
                logger.warning(f"Could not reset work_mem: {str(e)}")
            
            try:
                connection.execute(text("SET maintenance_work_mem = '64MB';"))
                logger.info("Reset maintenance_work_mem to default")
            except Exception as e:
                logger.warning(f"Could not reset maintenance_work_mem: {str(e)}")
            
            # Note: We're skipping session_replication_role due to permission issues
              # Analyze tables to update statistics after bulk load
            try:
                connection.execute(text("ANALYZE countries;"))
                connection.execute(text("ANALYZE covid_cases;"))
                logger.info("Analyzed tables to update database statistics")
            except Exception as e:
                logger.warning(f"Could not analyze tables: {str(e)}")
                
            connection.commit()
            
        logger.info("Database settings restored after bulk loading (with limited permissions)")
        return True
    except Exception as e:
        logger.error(f"Failed to restore database settings: {str(e)}")
        return False

def process_chunk(args):
    """
    Process a chunk of data in a separate process
    
    Args:
        args (tuple): Tuple containing chunk_id, chunk_df, country_map, and source
        
    Returns:
        tuple: (chunk_id, processed_df)
    """
    chunk_id, chunk_df, country_map, source = args
    return chunk_id, process_case_chunk(chunk_df, country_map, source)

def process_case_chunk(df, country_map, source):
    """Process a chunk of case data"""
    # Add country_id column
    df['country_id'] = df['country'].apply(lambda x: country_map.get(x, None))
    
    # Filter out entries with missing country mappings
    df = df[df['country_id'].notna()]
    
    if df.empty:
        return df
    
    # Ensure all required columns exist, add with default values if missing
    required_columns = ['total_cases', 'new_cases', 'total_deaths', 'new_deaths', 
                       'total_recovered', 'new_recovered', 'active_cases', 
                       'critical_cases', 'cases_per_million', 'deaths_per_million']
    
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
    
    # Convert integer columns to proper integer types
    int_columns = ['total_cases', 'new_cases', 'total_deaths', 'new_deaths', 
                  'total_recovered', 'new_recovered', 'active_cases', 'critical_cases']
    
    for col in int_columns:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(np.int64)
    
    # Convert float columns to proper float types
    float_columns = ['cases_per_million', 'deaths_per_million', 'case_fatality_rate',
                    'new_cases_7day_avg', 'new_deaths_7day_avg']
    
    for col in float_columns:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(float)
    
    # Generate data_hash values with proper error handling
    try:
        data_hash_vals = (df['total_cases'].astype(str) + '|' + 
                         df['new_cases'].astype(str) + '|' + 
                         df['total_deaths'].astype(str))
        df['data_hash'] = data_hash_vals.apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    except KeyError:
        # Fallback if any column is missing
        df['data_hash'] = [hashlib.md5(str(i).encode()).hexdigest() for i in range(len(df))]
        logger.warning("Using row indices for data_hash due to missing columns")
    
    # Add additional required fields
    df['created_at'] = datetime.now().date()
    df['source'] = source
    
    return df

# Vaccination processing function has been removed for better performance

def parallel_load_copy(processed_df, table_name, conn_string, columns_to_use=None):
    """
    Load processed data into the database using fast COPY method
    
    Args:
        processed_df (pd.DataFrame): Processed dataframe to load
        table_name (str): Target table name
        conn_string (str): Database connection string
        columns_to_use (list): List of columns to include in the COPY operation
    
    Returns:
        int: Number of records loaded
    """
    if processed_df.empty:
        return 0
        
    if columns_to_use is None:
        columns_to_use = processed_df.columns
    
    conn = None
    try:
        # Parse connection string to get parameters
        conn_parts = conn_string.split("://")[1].split("@")
        user_pass = conn_parts[0].split(":")
        host_db = conn_parts[1].split("/")
        
        username = user_pass[0]
        password = user_pass[1]
        host = host_db[0]
        dbname = host_db[1].split("?")[0]
        
        # Create direct psycopg2 connection for COPY
        conn = psycopg2.connect(
            dbname=dbname,
            user=username,
            password=password,
            host=host,
            sslmode='require'
        )
        
        # Create cursor
        cur = conn.cursor()
        
        # Make a copy of the dataframe with only the columns we need
        final_df = processed_df[columns_to_use].copy()
        
        # Apply proper data type conversions for database compatibility
        if table_name == 'vaccinations':
            # Ensure integer columns are properly formatted for COPY
            if 'total_vaccinations' in final_df.columns:
                final_df['total_vaccinations'] = final_df['total_vaccinations'].fillna(0).astype(np.int64)
                
            if 'daily_vaccinations' in final_df.columns:
                final_df['daily_vaccinations'] = final_df['daily_vaccinations'].fillna(0).astype(np.int64)
                
        elif table_name == 'covid_cases':
            # Ensure integer columns are properly formatted for COPY
            int_columns = ['total_cases', 'new_cases', 'total_deaths', 'new_deaths', 
                          'total_recovered', 'new_recovered', 'active_cases', 'critical_cases']
            
            for col in int_columns:
                if col in final_df.columns:
                    final_df[col] = final_df[col].fillna(0).astype(np.int64)
        
        # Create a buffer for CSV data
        buffer = StringIO()
        
        # Export with proper formatting for PostgreSQL COPY
        final_df.to_csv(
            buffer, 
            index=False, 
            header=False, 
            sep='\t', 
            na_rep='\\N',
            date_format='%Y-%m-%d'  # Ensure dates are formatted correctly
        )
        buffer.seek(0)
        
        # Log first few lines for debugging
        buffer_preview = buffer.getvalue()[:1000]
        logger.debug(f"COPY buffer preview: {buffer_preview}")
        buffer.seek(0)
        
        # Execute the COPY command
        cur.copy_from(
            buffer,
            table_name,
            columns=columns_to_use,
            null='\\N'
        )
        
        # Commit the transaction
        conn.commit()
        
        rows_loaded = len(final_df)
        logger.info(f"Successfully loaded {rows_loaded} records to {table_name} using COPY method")
        return rows_loaded
    
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error during COPY operation: {str(e)}")
        
        # Fallback to individual row inserts if COPY fails
        logger.info(f"Falling back to batch INSERT for {table_name}")
        return _fallback_batch_insert(processed_df, table_name, conn_string, columns_to_use)
    
    finally:
        if conn:
            cur.close()
            conn.close()
            
def _fallback_batch_insert(df, table_name, conn_string, columns_to_use):
    """Fallback method using batch inserts when COPY fails"""
    try:
        engine = create_engine(conn_string)
        
        # Insert in smaller batches
        batch_size = 500
        total_loaded = 0
        
        for i in range(0, len(df), batch_size):
            batch = df[i:i+batch_size]
            if len(batch) == 0:
                continue
            
            # Use DataFrame.to_sql() for this smaller batch
            batch[columns_to_use].to_sql(
                table_name,
                engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=50  # Use smaller chunks
            )
            
            total_loaded += len(batch)
            logger.info(f"Loaded batch {i//batch_size + 1} with fallback method, {total_loaded} records so far")
            
        return total_loaded
        
    except Exception as e:
        logger.error(f"Fallback batch insert also failed: {str(e)}")
        return 0

def parallel_load_data(df, table_name, country_map, source):
    """
    Load data into database using parallel processing
    
    Args:
        df (pd.DataFrame): DataFrame with data to load
        table_name (str): Target table name
        country_map (dict): Mapping of country names to country IDs
        source (str): Data source identifier
        
    Returns:
        int: Number of records loaded
    """
    if df.empty:
        return 0
    
    conn_string = get_db_connection_string()
    
    # Apply database optimizations
    set_db_optimizations(conn_string)
    
    try:
        # Determine number of workers based on CPU cores
        num_workers = min(multiprocessing.cpu_count(), 4)  # Limit to 4 workers to avoid DB connection issues
        
        # Split the dataframe into chunks
        chunk_size = max(1000, len(df) // (num_workers * 2))  # Ensure at least 1000 records per chunk
        chunks = [df.iloc[i:i+chunk_size] for i in range(0, len(df), chunk_size)]
        
        logger.info(f"Processing {len(df)} records in {len(chunks)} chunks using {num_workers} workers")
          # Prepare arguments for parallel processing
        process_args = [
            (i, chunk, country_map, source) 
            for i, chunk in enumerate(chunks)
        ]
        
        # Process chunks in parallel
        results = []
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(process_chunk, arg) for arg in process_args]
            
            for future in as_completed(futures):
                chunk_id, processed_chunk = future.result()
                if not processed_chunk.empty:
                    results.append((chunk_id, processed_chunk))
                    logger.debug(f"Processed chunk {chunk_id} with {len(processed_chunk)} records")
        
        # Sort results by chunk_id to maintain order
        results.sort(key=lambda x: x[0])
        
        # Determine column order for cases data
        column_order = [
            'country_id', 'date', 'total_cases', 'new_cases', 'total_deaths', 'new_deaths',
            'total_recovered', 'new_recovered', 'active_cases', 'critical_cases',
            'cases_per_million', 'deaths_per_million', 'case_fatality_rate', 
            'new_cases_7day_avg', 'new_deaths_7day_avg', 'data_hash', 'created_at', 'source'
        ]
        
        # Concatenate processed chunks
        if results:
            final_df = pd.concat([chunk for _, chunk in results])
            
            # Filter for columns that are in the data
            columns_to_use = [col for col in column_order if col in final_df.columns]
            
            # Load to database using fast COPY method
            total_loaded = parallel_load_copy(final_df, table_name, conn_string, columns_to_use)
            logger.info(f"Successfully loaded {total_loaded} records to {table_name} table")
            
            return total_loaded
        else:
            logger.warning(f"No records processed for {table_name}")
            return 0
    
    except Exception as e:
        logger.error(f"Error in parallel data loading: {str(e)}")
        return 0
    
    finally:
        # Restore database settings
        restore_db_settings(conn_string)

def parallel_load_cases(cases_df, country_map, source="disease.sh"):
    """
    Load COVID-19 cases data into database using parallel processing
    
    Args:
        cases_df (pd.DataFrame): DataFrame with cases data
        country_map (dict): Mapping of country names to IDs
        source (str): Data source identifier
        
    Returns:
        int: Number of records loaded
    """
    logger.info("Loading cases data using parallel processing...")
    
    if cases_df.empty or 'country' not in cases_df.columns or 'date' not in cases_df.columns:
        logger.warning("Empty or invalid cases dataframe provided")
        return 0
    
    return parallel_load_data(cases_df, 'covid_cases', country_map, source)

# Vaccination loading function has been removed for better performance
