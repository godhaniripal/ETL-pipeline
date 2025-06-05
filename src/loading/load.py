import pandas as pd
import psycopg2
from sqlalchemy import create_engine, Table, Column, Integer, BigInteger, String, Float, Date, MetaData, text
from sqlalchemy.dialects.postgresql import insert
import logging
import sys
from datetime import datetime
from pathlib import Path
import hashlib

# Ensure paths are set correctly
sys.path.append(str(Path(__file__).parent.parent.absolute()))
from src.utils import get_db_connection_string

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent.parent.absolute() / "etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("loading")

def initialize_database():
    """
    Initialize the database schema
    
    Creates the necessary tables if they don't exist
    """
    logger.info("Initializing database schema...")
    
    conn_string = get_db_connection_string()
    
    try:
        # Create engine
        engine = create_engine(conn_string)
        
        # Define metadata
        metadata = MetaData()
        
        # Define countries table
        countries = Table(
            'countries', metadata,
            Column('country_id', Integer, primary_key=True),
            Column('country_name', String, unique=True, nullable=False),
            Column('country_code', String(3)),
            Column('continent', String),
            Column('population', Integer)
        )
        
        # Define covid_cases table with historical data capability
        covid_cases = Table(
            'covid_cases', metadata,
            Column('case_id', Integer, primary_key=True),
            Column('country_id', Integer),
            Column('date', Date, nullable=False),
            Column('total_cases', Integer),
            Column('new_cases', Integer),
            Column('total_deaths', Integer),
            Column('new_deaths', Integer),
            Column('total_recovered', Integer),
            Column('new_recovered', Integer),
            Column('active_cases', Integer),
            Column('critical_cases', Integer),
            Column('cases_per_million', Float),
            Column('deaths_per_million', Float),
            Column('case_fatality_rate', Float),
            Column('new_cases_7day_avg', Float),
            Column('new_deaths_7day_avg', Float),
            Column('data_hash', String, nullable=False),  # For tracking changes
            Column('created_at', Date, nullable=False),
            Column('source', String)
        )
        
        # Define vaccinations table with historical data
        vaccinations = Table(
            'vaccinations', metadata,
            Column('vaccination_id', Integer, primary_key=True),
            Column('country_id', Integer),
            Column('date', Date, nullable=False),
            Column('total_vaccinations', BigInteger),  # Changed to BigInteger for larger values
            Column('daily_vaccinations', BigInteger),  # Changed to BigInteger for larger values
            Column('vaccination_rate', Float),
            Column('data_hash', String, nullable=False),  # For tracking changes
            Column('created_at', Date, nullable=False),
            Column('source', String)
        )
        
        # Create tables
        metadata.create_all(engine)
        logger.info("Database schema initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

def get_country_mappings():
    """
    Get existing country ID mappings from the database
    
    Returns:
        dict: Mapping of country names to IDs
    """
    conn_string = get_db_connection_string()
    
    try:
        # Create engine
        engine = create_engine(conn_string)
        
        # Query existing countries
        query = text("SELECT country_id, country_name FROM countries")
        with engine.connect() as connection:
            result = connection.execute(query)
            country_map = {row.country_name: row.country_id for row in result}
        
        logger.info(f"Retrieved {len(country_map)} country mappings from database")
        return country_map
    
    except Exception as e:
        logger.error(f"Error getting country mappings: {str(e)}")
        return {}

def load_country_data(countries_df):
    """
    Load or update country data in the database
    
    Args:
        countries_df (pd.DataFrame): DataFrame with country data
    
    Returns:
        dict: Mapping of country names to IDs
    """
    logger.info("Loading country data to database...")
    
    if countries_df.empty or 'country' not in countries_df.columns:
        logger.warning("Empty or invalid countries dataframe provided")
        return {}
    
    conn_string = get_db_connection_string()
    
    try:
        # Create engine
        engine = create_engine(conn_string)
        
        # Extract unique country data
        unique_countries = countries_df.drop_duplicates('country')
        
        # Prepare data for insert
        country_data = []
        for _, row in unique_countries.iterrows():
            country = {
                'country_name': row['country'],
                'country_code': row.get('country_code', None),
                'continent': row.get('continent', None),
                'population': row.get('population', None)
            }
            country_data.append(country)
            
        # Add missing countries that might be in vaccination data but not cases data
        missing_countries = ['Guernsey', 'Turkmenistan']
        for country_name in missing_countries:
            if not any(c['country_name'] == country_name for c in country_data):
                logger.info(f"Adding missing country: {country_name}")
                country_data.append({
                    'country_name': country_name,
                    'country_code': None,
                    'continent': None,
                    'population': None
                })
          # Create DataFrame
        country_df = pd.DataFrame(country_data)
        
        # Get existing countries to avoid duplicate entries
        existing_country_map = get_country_mappings()
        logger.info(f"Found {len(existing_country_map)} existing countries in database")
        
        # Filter out countries that already exist in the database
        new_countries = []
        for country in country_data:
            if country['country_name'] not in existing_country_map:
                new_countries.append(country)
        
        if new_countries:
            # Create DataFrame with only new countries
            new_country_df = pd.DataFrame(new_countries)
            
            # Insert only new countries to database
            new_country_df.to_sql(
                'countries', 
                engine, 
                if_exists='append', 
                index=False, 
                method='multi',
                chunksize=1000
            )
            logger.info(f"Successfully inserted {len(new_countries)} new countries")
        else:
            logger.info("No new countries to insert")
        
        # Get updated country mappings after insert
        country_map = get_country_mappings()
        logger.info(f"Total countries in database: {len(country_map)}")
        
        return country_map
    
    except Exception as e:
        logger.error(f"Error loading country data: {str(e)}")
        return {}

def load_cases_data(cases_df, country_map, source="disease.sh"):
    """
    Load COVID-19 cases data into database using efficient COPY method
    
    Args:
        cases_df (pd.DataFrame): DataFrame with cases data
        country_map (dict): Mapping of country names to IDs
        source (str): Data source identifier
        
    Returns:
        int: Number of records loaded
    """
    logger.info("Loading cases data to database...")
    
    if cases_df.empty or 'country' not in cases_df.columns:
        logger.warning("Empty or invalid cases dataframe provided")
        return 0
    
    conn_string = get_db_connection_string()
    
    try:
        # Create engine
        engine = create_engine(conn_string)
        
        # Required case columns
        required_cols = ['country', 'date']
        if not all(col in cases_df.columns for col in required_cols):
            logger.error(f"Missing required columns in cases data: {', '.join(required_cols)}")
            return 0
        
        # Identify missing countries
        missing_countries = []
        missing_country_set = set()
        for country in cases_df['country'].unique():
            if country not in country_map and country not in missing_country_set:
                missing_country_set.add(country)
                missing_countries.append({
                    'country_name': country,
                    'country_code': None,
                    'continent': None,
                    'population': None
                })
        
        # Add missing countries if needed
        if missing_countries:
            logger.info(f"Adding {len(missing_countries)} missing countries to database...")
            
            # Insert missing countries
            missing_df = pd.DataFrame(missing_countries)
            missing_df.to_sql(
                'countries',
                engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            # Update country map with newly added countries
            new_country_map = get_country_mappings()
            country_map.update(new_country_map)
            logger.info(f"Added {len(missing_countries)} missing countries to database")
        
        # Process data directly without creating a huge list of dictionaries
        logger.info(f"Processing {len(cases_df)} case records...")
        processed_df = cases_df.copy()
        
        # Add country_id column and convert countries to IDs
        processed_df['country_id'] = processed_df['country'].apply(lambda x: country_map.get(x, None))
        
        # Filter out entries with missing country mappings
        missing_countries = processed_df[processed_df['country_id'].isna()]['country'].unique()
        if len(missing_countries) > 0:
            logger.warning(f"Skipping {len(missing_countries)} countries with no mapping: {', '.join(missing_countries[:5])}" + 
                         (f" and {len(missing_countries) - 5} more" if len(missing_countries) > 5 else ""))
            
        processed_df = processed_df[processed_df['country_id'].notna()]
        
        if processed_df.empty:
            logger.warning("No valid case records to insert after country mapping")
            return 0
        
        # Generate data_hash values vectorized for better performance
        data_hash_df = processed_df[['total_cases', 'new_cases', 'total_deaths']].fillna(0).astype(str)
        data_hash_values = data_hash_df['total_cases'] + '|' + data_hash_df['new_cases'] + '|' + data_hash_df['total_deaths']
        processed_df['data_hash'] = data_hash_values.apply(lambda x: hashlib.md5(x.encode()).hexdigest())
        
        # Add additional required fields
        processed_df['created_at'] = datetime.now().date()
        processed_df['source'] = source
        
        # Select and order columns needed for the database table
        column_order = [
            'country_id', 'date', 'total_cases', 'new_cases', 'total_deaths', 'new_deaths',
            'total_recovered', 'new_recovered', 'active_cases', 'critical_cases',
            'cases_per_million', 'deaths_per_million', 'case_fatality_rate', 
            'new_cases_7day_avg', 'new_deaths_7day_avg', 'data_hash', 'created_at', 'source'
        ]
        
        # Filter for columns that are actually in the dataframe
        columns_to_use = [col for col in column_order if col in processed_df.columns]
        final_df = processed_df[columns_to_use]
        
        # Try COPY method first (much faster)
        logger.info("Using optimized copy method for loading case data...")
        from io import StringIO
        
        # Create a raw connection
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cursor:
                # Get column names formatted properly for COPY command
                columns = ', '.join(columns_to_use)
                
                # Create a buffer for CSV data
                buffer = StringIO()
                final_df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
                buffer.seek(0)
                
                # Execute the COPY command for direct, fast insertion
                cursor.copy_from(
                    buffer,
                    'covid_cases',
                    columns=columns_to_use,
                    null='\\N'
                )
                
                # Commit the transaction
                conn.commit()
                
                total_loaded = len(final_df)
                logger.info(f"Successfully loaded {total_loaded} case records using fast COPY method")
                return total_loaded
                
        except Exception as copy_error:
            conn.rollback()
            logger.error(f"Error during COPY operation: {str(copy_error)}")
            
            # Fall back to batch insert if COPY fails
            logger.info("Falling back to optimized batch insertion method...")
            return _batch_insert_cases(final_df, engine)
            
        finally:
            conn.close()
    
    except Exception as e:
        logger.error(f"Error loading cases data: {str(e)}")
        return 0
        
def _batch_insert_cases(df, engine, batch_size=1000):
    """
    Helper function to insert case data in small batches using raw SQL
    to avoid parameter limit issues
    
    Args:
        df (pd.DataFrame): DataFrame with processed case data
        engine: SQLAlchemy engine
        batch_size (int): Size of each batch
    
    Returns:
        int: Number of records inserted
    """
    total_loaded = 0
    
    # Get column names
    columns = ", ".join(df.columns)
    
    try:
        # Create raw connection for better performance
        conn = engine.raw_connection()
        cursor = conn.cursor()
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            if len(batch) == 0:
                continue
                
            try:
                # Prepare values for insertion
                values_list = []
                placeholders = []
                
                # Generate placeholder string like (%s, %s, %s, ...) for each row
                for _, row in batch.iterrows():
                    # Create tuple of values
                    row_values = tuple(None if pd.isna(v) else v for v in row)
                    values_list.append(row_values)
                    placeholders.append("(" + ", ".join(["%s"] * len(row)) + ")")
                
                # Create SQL statement with all placeholders joined
                sql = f"INSERT INTO covid_cases ({columns}) VALUES {', '.join(placeholders)}"
                
                # Execute with flattened values
                cursor.execute(sql, [item for row in values_list for item in row])
                conn.commit()
                
                total_loaded += len(batch)
                logger.info(f"Loaded batch {i//batch_size + 1}, total records loaded so far: {total_loaded}")
                
            except Exception as batch_error:
                conn.rollback()
                logger.error(f"Error in batch {i//batch_size + 1}: {str(batch_error)}")
                # Try with smaller batch size for this problematic chunk
                if batch_size > 100:
                    logger.info(f"Trying again with smaller batch size for problematic chunk...")
                    smaller_batch_loaded = _batch_insert_cases(batch, engine, batch_size=batch_size // 5)
                    total_loaded += smaller_batch_loaded
        
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully loaded {total_loaded} case records using batch insert")
        return total_loaded
        
    except Exception as e:
        logger.error(f"Error in batch insertion: {str(e)}")
        return total_loaded

def load_vaccination_data(vaccines_df, country_map, source="disease.sh"):
    """
    Load vaccination data into database using efficient copy method
    
    Args:
        vaccines_df (pd.DataFrame): DataFrame with vaccination data
        country_map (dict): Mapping of country names to IDs
        source (str): Data source identifier
        
    Returns:
        int: Number of records loaded
    """
    logger.info("Loading vaccination data to database...")
    
    if vaccines_df.empty or 'country' not in vaccines_df.columns or 'date' not in vaccines_df.columns:
        logger.warning("Empty or invalid vaccination dataframe provided")
        return 0
    
    conn_string = get_db_connection_string()
    
    try:
        # Create engine
        engine = create_engine(conn_string)
        
        # Identify missing countries
        missing_countries = []
        missing_country_set = set()
        for country in vaccines_df['country'].unique():
            if country not in country_map and country not in missing_country_set:
                missing_country_set.add(country)
                missing_countries.append({
                    'country_name': country,
                    'country_code': None,
                    'continent': None,
                    'population': None
                })
        
        # Add missing countries if needed
        if missing_countries:
            logger.info(f"Adding {len(missing_countries)} missing countries to database...")
            
            # Insert missing countries
            missing_df = pd.DataFrame(missing_countries)
            missing_df.to_sql(
                'countries',
                engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            # Update country map with newly added countries
            new_country_map = get_country_mappings()
            country_map.update(new_country_map)
            logger.info(f"Added {len(missing_countries)} missing countries to database")
        
        # Prepare data for insert with proper country_id mapping
        logger.info(f"Processing {len(vaccines_df)} vaccination records...")
        
        # Process data directly without creating a huge list of dictionaries
        processed_df = vaccines_df.copy()
        
        # Add country_id column and convert countries to IDs
        processed_df['country_id'] = processed_df['country'].apply(lambda x: country_map.get(x, None))
        
        # Filter out entries with missing country mappings
        missing_countries = processed_df[processed_df['country_id'].isna()]['country'].unique()
        if len(missing_countries) > 0:
            logger.warning(f"Skipping {len(missing_countries)} countries with no mapping: {', '.join(missing_countries[:5])}" + 
                         (f" and {len(missing_countries) - 5} more" if len(missing_countries) > 5 else ""))
            
        processed_df = processed_df[processed_df['country_id'].notna()]
        
        if processed_df.empty:
            logger.warning("No valid vaccination records to insert after country mapping")
            return 0
        
        # Handle potential integer overflow
        max_bigint = 9223372036854775807
        
        # Check and cap total_vaccinations
        if 'total_vaccinations' in processed_df.columns:
            overflow_count = (processed_df['total_vaccinations'] > max_bigint).sum()
            if overflow_count > 0:
                logger.warning(f"Capping {overflow_count} total_vaccinations values that exceed BIGINT range")
                processed_df.loc[processed_df['total_vaccinations'] > max_bigint, 'total_vaccinations'] = max_bigint
        
        # Check and cap daily_vaccinations
        if 'daily_vaccinations' in processed_df.columns:
            overflow_count = (processed_df['daily_vaccinations'] > max_bigint).sum()
            if overflow_count > 0:
                logger.warning(f"Capping {overflow_count} daily_vaccinations values that exceed BIGINT range")
                processed_df.loc[processed_df['daily_vaccinations'] > max_bigint, 'daily_vaccinations'] = max_bigint
        
        # Create data_hash values for all rows at once using vectorized operations
        data_hash_values = (processed_df['total_vaccinations'].fillna(0).astype(str) + '|' + 
                           processed_df['daily_vaccinations'].fillna(0).astype(str))
        processed_df['data_hash'] = data_hash_values.apply(lambda x: hashlib.md5(x.encode()).hexdigest())
        
        # Add additional required fields
        processed_df['created_at'] = datetime.now().date()
        processed_df['source'] = source
        
        # Select and order columns for the database table
        column_order = [
            'country_id', 'date', 'total_vaccinations', 'daily_vaccinations', 
            'vaccination_rate', 'data_hash', 'created_at', 'source'
        ]
        
        # Filter for columns that are actually in the dataframe
        columns_to_use = [col for col in column_order if col in processed_df.columns]
        final_df = processed_df[columns_to_use]
        
        # Create temporary file for COPY FROM operation
        from io import StringIO
        import tempfile
        
        logger.info("Using optimized copy method for loading vaccination data...")
        
        # Create a raw connection
        conn = engine.raw_connection()
        try:
            with conn.cursor() as cursor:
                # Get column names formatted properly for COPY command
                columns = ', '.join(columns_to_use)
                
                # Create a buffer for CSV data
                buffer = StringIO()
                final_df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
                buffer.seek(0)
                
                # Execute the COPY command for direct, fast insertion
                cursor.copy_from(
                    buffer,
                    'vaccinations',
                    columns=columns_to_use,
                    null='\\N'
                )
                
                # Commit the transaction
                conn.commit()
                
                total_loaded = len(final_df)
                logger.info(f"Successfully loaded {total_loaded} vaccination records using fast COPY method")
                return total_loaded
                
        except Exception as copy_error:
            conn.rollback()
            logger.error(f"Error during COPY operation: {str(copy_error)}")
            
            # Fall back to batch insert if COPY fails
            logger.info("Falling back to batch insertion method...")
            return _batch_insert_vaccines(final_df, engine)
            
        finally:
            conn.close()
    
    except Exception as e:
        logger.error(f"Error loading vaccination data: {str(e)}")
        return 0

def _batch_insert_vaccines(df, engine, batch_size=1000):
    """
    Helper function to insert vaccine data in small batches using raw SQL
    to avoid parameter limit issues
    
    Args:
        df (pd.DataFrame): DataFrame with processed vaccination data
        engine: SQLAlchemy engine
        batch_size (int): Size of each batch
    
    Returns:
        int: Number of records inserted
    """
    total_loaded = 0
    
    # Get column names
    columns = ", ".join(df.columns)
    
    try:
        # Create raw connection for better performance
        conn = engine.raw_connection()
        cursor = conn.cursor()
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            if len(batch) == 0:
                continue
                
            try:
                # Prepare values for insertion
                values_list = []
                placeholders = []
                
                # Generate placeholder string like (%s, %s, %s, ...) for each row
                for _, row in batch.iterrows():
                    # Create tuple of values
                    row_values = tuple(None if pd.isna(v) else v for v in row)
                    values_list.append(row_values)
                    placeholders.append("(" + ", ".join(["%s"] * len(row)) + ")")
                
                # Create SQL statement with all placeholders joined
                sql = f"INSERT INTO vaccinations ({columns}) VALUES {', '.join(placeholders)}"
                
                # Execute with flattened values
                cursor.execute(sql, [item for row in values_list for item in row])
                conn.commit()
                
                total_loaded += len(batch)
                logger.info(f"Loaded batch {i//batch_size + 1}, total records loaded so far: {total_loaded}")
                
            except Exception as batch_error:
                conn.rollback()
                logger.error(f"Error in batch {i//batch_size + 1}: {str(batch_error)}")
                # Try with smaller batch size for this problematic chunk
                if batch_size > 100:
                    logger.info(f"Trying again with smaller batch size for problematic chunk...")
                    smaller_batch_loaded = _batch_insert_vaccines(batch, engine, batch_size=batch_size // 5)
                    total_loaded += smaller_batch_loaded
        
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully loaded {total_loaded} vaccination records using batch insert")
        return total_loaded
        
    except Exception as e:
        logger.error(f"Error in batch insertion: {str(e)}")
        return total_loaded

def check_database_connection():
    """
    Check if database connection is working
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    logger.info("Checking database connection...")
    
    conn_string = get_db_connection_string()
    
    try:
        # Create engine
        engine = create_engine(conn_string)
        
        # Test connection
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            if result.scalar() == 1:
                logger.info("Database connection successful")
                return True
        
        logger.error("Database connection test failed")
        return False
    
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return False
