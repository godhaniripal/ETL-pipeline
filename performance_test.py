#!/usr/bin/env python
"""
COVID-19 ETL Performance Testing Script

This script runs the ETL pipeline with different loading methods and reports performance metrics.
It compares standard loading vs parallel loading to help identify the most efficient approach.
"""

import os
import sys
import time
import logging
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
from datetime import datetime

# Ensure paths are set correctly
sys.path.append(str(Path(__file__).absolute().parent))

# Import modules
from etl_pipeline import run_etl_pipeline
from reset_db import reset_database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("performance_test.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("performance_test")

def run_performance_test():
    """
    Run ETL pipeline with different loading methods and compare performance
    """
    results = []
    
    # Method 1: Standard Loading
    logger.info("Running ETL pipeline with standard loading...")
    reset_database()  # Reset database before test
    
    start_time = time.time()
    success = run_etl_pipeline(use_parallel=False)
    elapsed_time = time.time() - start_time
    
    if success:
        logger.info(f"Standard loading completed in {elapsed_time:.2f} seconds")
        results.append(("Standard", elapsed_time))
    else:
        logger.error("Standard loading failed")
    
    # Method 2: Parallel Loading
    logger.info("Running ETL pipeline with parallel loading...")
    reset_database()  # Reset database before test
    
    start_time = time.time()
    success = run_etl_pipeline(use_parallel=True)
    elapsed_time = time.time() - start_time
    
    if success:
        logger.info(f"Parallel loading completed in {elapsed_time:.2f} seconds")
        results.append(("Parallel", elapsed_time))
    else:
        logger.error("Parallel loading failed")
    
    # Report results
    if results:
        logger.info("Performance comparison results:")
        for method, time_taken in results:
            logger.info(f"{method} loading: {time_taken:.2f} seconds")
        
        # Create simple performance chart
        methods = [r[0] for r in results]
        times = [r[1] for r in results]
        
        plt.figure(figsize=(10, 6))
        plt.bar(methods, times)
        plt.title('ETL Pipeline Loading Performance Comparison')
        plt.ylabel('Time (seconds)')
        plt.xlabel('Loading Method')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Add time values on top of bars
        for i, v in enumerate(times):
            plt.text(i, v + 0.5, f"{v:.2f}s", ha='center')
        
        # Save chart
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        chart_path = f"data/performance_comparison_{timestamp}.png"
        os.makedirs(os.path.dirname(chart_path), exist_ok=True)
        plt.tight_layout()
        plt.savefig(chart_path)
        logger.info(f"Performance chart saved to {chart_path}")

if __name__ == "__main__":
    run_performance_test()
