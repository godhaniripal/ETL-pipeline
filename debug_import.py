# Simple script to debug the module import issue
import sys
import inspect

# First try to import the module
print("Attempting to import reset_db module...")
import reset_db
print(f"Module imported: {reset_db}")
print(f"Module attributes: {dir(reset_db)}")

# Look at the module source code
print("\nModule source code:")
try:
    print(inspect.getsource(reset_db))
except Exception as e:
    print(f"Error getting source: {e}")

# Try to directly access functions
print("\nTrying to access functions directly:")
try:
    from reset_db import drop_all_tables, initialize_database, reset_database
    print("Successfully imported functions")
    print(f"reset_database type: {type(reset_database)}")
except Exception as e:
    print(f"Error importing functions: {e}")
