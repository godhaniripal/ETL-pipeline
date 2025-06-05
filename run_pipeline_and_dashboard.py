#!/usr/bin/env python
# filepath: c:\Users\ripal\Desktop\ETL\run_pipeline_and_dashboard.py

"""
Run the entire ETL pipeline and launch the dashboard.
This script:
1. Resets the database schema
2. Runs the ETL pipeline to fetch and load data
3. Launches the dashboard
"""

import os
import sys
import logging
import subprocess
from pathlib import Path

# Ensure paths are set correctly
sys.path.append(str(Path(__file__).absolute().parent))

# Import modules
from reset_db import reset_database
from etl_pipeline import run_etl_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("run_full_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("run_pipeline_and_dashboard")

def run_full_pipeline():
    """
    Runs the complete pipeline: database reset, ETL, and dashboard
    """
    logger.info("Starting full COVID-19 ETL pipeline and dashboard...")
    
    try:
        # Step 1: Reset the database
        logger.info("Step 1: Resetting database schema...")
        if not reset_database():
            logger.error("Failed to reset database schema")
            return False
        logger.info("Database schema reset successfully")
        
        # Step 2: Run ETL pipeline
        logger.info("Step 2: Running ETL pipeline...")
        if not run_etl_pipeline():
            logger.error("ETL pipeline failed")
            return False
        logger.info("ETL pipeline completed successfully")
        
        # Step 3: Launch the dashboard
        logger.info("Step 3: Launching dashboard...")
        dashboard_script = Path(__file__).parent / "run_dashboard.py"
        
        cmd = [sys.executable, str(dashboard_script)]
        process = subprocess.Popen(cmd)
        
        logger.info("Full pipeline completed and dashboard launched!")
        logger.info("Dashboard available at http://localhost:8501")
        
        # Keep the script running while the dashboard is active
        process.wait()
        
        return True
    
    except Exception as e:
        logger.error(f"Error in full pipeline: {str(e)}")
        return False

if __name__ == "__main__":
    run_full_pipeline()
