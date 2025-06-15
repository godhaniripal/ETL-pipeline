#!/usr/bin/env python
"""
Database reset and initialization module for the COVID-19 ETL pipeline
"""

import sys
from pathlib import Path
import logging
from sqlalchemy import create_engine, Table, Column, Integer, BigInteger, String, Float, Date, MetaData, text

# Ensure paths are set correctly
sys.path.append(str(Path(__file__).absolute().parent))
from src.utils import get_db_connection_string

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("reset_db.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("reset_db")

def drop_all_tables():
    """
    Drop all existing tables in the database to start fresh
    """
    logger.info("Dropping all existing tables...")
    
    conn_string = get_db_connection_string()
    
    try:
        # Create engine
        engine = create_engine(conn_string)
        
        # Drop all tables - removed vaccinations table
        with engine.connect() as connection:
            connection.execute(text("DROP TABLE IF EXISTS covid_cases CASCADE;"))
            connection.execute(text("DROP TABLE IF EXISTS countries CASCADE;"))
            connection.commit()
        
        logger.info("All tables dropped successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error dropping tables: {str(e)}")
        return False

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
        # Removed vaccinations table for better performance
        
        # Create tables
        metadata.create_all(engine)
        logger.info("Database schema initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

def reset_database():
    """
    Reset the database by dropping all tables and re-initializing the schema
    
    Also optimizes the database for bulk loading by setting appropriate
    configuration parameters. Removed vaccination table for better performance.
    """
    if drop_all_tables():
        logger.info("Re-initializing database schema...")
        initialize_database()
        
        # Add foreign key constraints after tables are created
        conn_string = get_db_connection_string()
        try:
            engine = create_engine(conn_string)
            with engine.connect() as connection:
                # Add foreign key from covid_cases to countries
                connection.execute(text("""
                    ALTER TABLE covid_cases 
                    ADD CONSTRAINT fk_covid_cases_country 
                    FOREIGN KEY (country_id) 
                    REFERENCES countries(country_id);
                """))
                
                # Add indexes to improve query performance
                connection.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_covid_cases_country_date 
                    ON covid_cases(country_id, date);
                """))
                
                # Set database parameters for faster bulk loading
                # Note: These settings are temporary for the current session
                connection.execute(text("SET work_mem = '128MB';"))
                connection.execute(text("SET maintenance_work_mem = '256MB';"))
                connection.execute(text("SET temp_buffers = '64MB';"))
                
                # Increased these parameters for better performance
                connection.execute(text("SET effective_cache_size = '1GB';"))
                connection.execute(text("SET random_page_cost = 1.1;"))  # Assumes SSD storage
                
                connection.commit()
                
            logger.info("Foreign key constraints and performance indexes added successfully")
            logger.info("Database optimized for bulk loading")
        except Exception as e:
            logger.error(f"Error configuring database: {str(e)}")
        logger.info("Database reset completed successfully")
        return True
    return False

# Export functions for module imports
__all__ = ['drop_all_tables', 'initialize_database', 'reset_database']

if __name__ == "__main__":
    reset_database()
