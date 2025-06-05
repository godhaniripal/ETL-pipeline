#!/usr/bin/env python
# filepath: c:\Users\ripal\Desktop\ETL\run_dashboard.py

"""
Run the COVID-19 and Vaccination Dashboard.
This script launches the Streamlit dashboard application.
"""

import os
import sys
import subprocess
import logging
from pathlib import Path

# Ensure paths are set correctly
sys.path.append(str(Path(__file__).absolute().parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dashboard.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("run_dashboard")

def run_dashboard():
    """
    Runs the COVID-19 dashboard using Streamlit
    """
    logger.info("Starting COVID-19 and Vaccination Dashboard...")
    
    # Path to the dashboard script
    dashboard_script = Path(__file__).parent / "dashboard" / "dashboard.py"
    
    if not dashboard_script.exists():
        logger.error(f"Dashboard script not found at {dashboard_script}")
        return False
    
    try:
        # Check if streamlit is installed
        try:
            import streamlit
            logger.info("Streamlit is installed")
        except ImportError:
            logger.warning("Streamlit not found, installing...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit"])
            logger.info("Streamlit installed successfully")
        
        # Launch the dashboard
        cmd = [sys.executable, "-m", "streamlit", "run", str(dashboard_script), "--server.port=8501"]
        logger.info(f"Running command: {' '.join(cmd)}")
        
        # Use subprocess to run the dashboard
        process = subprocess.Popen(cmd)
        
        logger.info("Dashboard started! Open your browser at http://localhost:8501")
        
        # Wait for the process to complete (if Ctrl+C is pressed, for example)
        process.wait()
        
        return True
        
    except Exception as e:
        logger.error(f"Error starting dashboard: {str(e)}")
        return False

if __name__ == "__main__":
    run_dashboard()
