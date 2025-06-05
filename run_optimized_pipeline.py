#!/usr/bin/env python
"""
Run the entire ETL pipeline and launch the dashboard with performance optimization.
This script:
1. Resets the database schema
2. Runs the ETL pipeline using parallel loading for better performance
3. Launches the dashboard
"""

import os
import sys
import time
import logging
import subprocess
import argparse
from pathlib import Path

# Ensure paths are set correctly
sys.path.append(str(Path(__file__).absolute().parent))

# Import modules
import reset_db
from etl_pipeline import run_etl_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("run_optimized_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("run_optimized_pipeline")

def run_full_pipeline(reset_database=True, use_parallel=True):
    """
    Runs the complete pipeline: database reset, ETL, and dashboard
    
    Args:
        reset_database (bool): Whether to reset the database schema
        use_parallel (bool): Whether to use parallel processing for loading
        
    Returns:
        bool: True if successful, False otherwise
    """
    start_time = time.time()
    logger.info("Starting optimized COVID-19 ETL pipeline and dashboard...")
    
    try:
        # Step 1: Reset the database if requested
        if reset_database:
            logger.info("Step 1: Resetting database schema...")
            if not reset_db.reset_database():
                logger.error("Failed to reset database schema")
                return False
            logger.info("Database schema reset successfully")
        else:
            logger.info("Skipping database schema reset")
        
        # Step 2: Run ETL pipeline with optimized loading
        logger.info(f"Step 2: Running ETL pipeline with {'parallel' if use_parallel else 'standard'} loading...")
        etl_start = time.time()
        if not run_etl_pipeline(use_parallel=use_parallel):
            logger.error("ETL pipeline failed")
            return False
        etl_time = time.time() - etl_start
        logger.info(f"ETL pipeline completed successfully in {etl_time:.2f} seconds")
        
        # Step 3: Launch the dashboard
        logger.info("Step 3: Launching dashboard...")
        dashboard_script = Path(__file__).parent / "run_dashboard.py"
        
        cmd = [sys.executable, str(dashboard_script)]
        process = subprocess.Popen(cmd)
        
        total_time = time.time() - start_time
        logger.info(f"Full pipeline completed in {total_time:.2f} seconds!")
        logger.info("Dashboard available at http://localhost:8501")
        
        # Keep the script running while the dashboard is active
        process.wait()
        
        return True
    
    except Exception as e:
        logger.error(f"Error in full pipeline: {str(e)}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run COVID-19 ETL Pipeline and Dashboard")
    parser.add_argument("--no-reset", action="store_true", help="Skip database reset")
    parser.add_argument("--standard", action="store_true", help="Use standard (non-parallel) loading")
    args = parser.parse_args()
    
    run_full_pipeline(reset_database=not args.no_reset, use_parallel=not args.standard)
