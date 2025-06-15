# Run script for COVID-19 & Vaccination Tracker
# Can be run with PowerShell by executing: .\run.ps1

# Function to show the menu
function Show-Menu {
    Clear-Host
    Write-Host "================ COVID-19 & Vaccination Tracker ================"
    Write-Host "1: Run ETL Pipeline (fetch new data from APIs)"
    Write-Host "2: Process CSV file with ETL Pipeline"
    Write-Host "3: Launch Streamlit Dashboard"
    Write-Host "4: Check Database Connection"
    Write-Host "5: Exit"
    Write-Host "6: Run Optimized Pipeline"
    Write-Host "=============================================================="
}

# Function to run ETL Pipeline
function Run-ETL {
    Write-Host "Running ETL Pipeline..."
    python etl_pipeline.py
    Write-Host "Press any key to continue..."
    $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
}

# Function to process CSV file
function Process-CSV {
    Write-Host "Enter the path to your CSV file:"
    $csv_path = Read-Host
    
    if (Test-Path $csv_path) {
        Write-Host "Processing CSV file: $csv_path"
        python etl_pipeline.py $csv_path
    } else {
        Write-Host "File not found: $csv_path"
    }
    
    Write-Host "Press any key to continue..."
    $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
}

# Function to launch Streamlit Dashboard
function Launch-Dashboard {
    Write-Host "Launching Streamlit Dashboard..."
    Start-Process powershell -ArgumentList "-NoExit -Command streamlit run dashboard/dashboard.py"
}

# Function to check database connection
function Check-Database {
    Write-Host "Checking database connection..."
    python -c "from src.loading.load import check_database_connection; print('Connection successful!' if check_database_connection() else 'Connection failed!')"
    Write-Host "Press any key to continue..."
    $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
}

# Function to run the optimized pipeline
function Run-OptimizedPipeline {
    Write-Host "Running Optimized Pipeline..."
    python run_optimized_pipeline.py
    Write-Host "Press any key to continue..."
    $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
}

# Main loop
do {
    Show-Menu
    $selection = Read-Host "Please make a selection"
    
    switch ($selection) {
        '1' { Run-ETL }
        '2' { Process-CSV }
        '3' { Launch-Dashboard }
        '4' { Check-Database }
        '5' { 
            Write-Host "Exiting..."
            exit 
        }
        '6' { Run-OptimizedPipeline }
        default { 
            Write-Host "Invalid selection. Please try again."
            Write-Host "Press any key to continue..."
            $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
        }
    }
} while ($selection -ne '5')
