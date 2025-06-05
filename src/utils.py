import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Ensure paths are set correctly
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

# Load environment variables
load_dotenv(PROJECT_ROOT / '.env')

# Database configuration
DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')

# API URLs
DISEASE_SH_API = os.getenv('DISEASE_SH_API')
COVID19_API = os.getenv('COVID19_API')

# Data paths
RAW_DATA_PATH = PROJECT_ROOT / os.getenv('RAW_DATA_PATH')
PROCESSED_DATA_PATH = PROJECT_ROOT / os.getenv('PROCESSED_DATA_PATH')

def get_db_connection_string():
    """Return the database connection string."""
    return DB_CONNECTION_STRING
