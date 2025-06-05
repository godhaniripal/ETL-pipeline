import os
from pathlib import Path

# Define project directory structure
directories = [
    "data/raw",
    "data/processed",
    "src/extraction",
    "src/transformation",
    "src/loading",
    "dashboard"
]

# Create directories if they don't exist
for directory in directories:
    dir_path = Path(directory)
    if not dir_path.exists():
        print(f"Creating directory: {directory}")
        os.makedirs(dir_path, exist_ok=True)
    else:
        print(f"Directory already exists: {directory}")

print("\nAll directories created successfully!")
