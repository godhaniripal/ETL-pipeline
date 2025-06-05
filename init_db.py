import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.absolute()))

from src.loading.load import initialize_database, check_database_connection

def main():
    """
    Initialize the database schema needed for the COVID-19 ETL pipeline.
    """
    print("Checking database connection...")
    if check_database_connection():
        print("Database connection successful!")
        
        print("\nInitializing database schema...")
        try:
            initialize_database()
            print("Database schema initialized successfully!")
        except Exception as e:
            print(f"Error initializing database schema: {str(e)}")
    else:
        print("Database connection failed. Please check your connection string.")

if __name__ == "__main__":
    main()
