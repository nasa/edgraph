#!/usr/bin/env python3
"""
Script to remove personal information columns from the EOSDIS DOIs CSV file.
This script removes columns containing names and email addresses while preserving
essential data needed for the knowledge graph ingestion process.
"""

import pandas as pd
import os
from pathlib import Path

def clean_personal_data(input_file: str, output_file: str = None) -> None:
    """
    Remove personal information columns from the CSV file.
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path to the output CSV file (optional, will overwrite input if not specified)
    """
    print(f"Loading data from {input_file}...")
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    print(f"Original columns: {list(df.columns)}")
    print(f"Original shape: {df.shape}")
    
    # Define columns to keep (essential for the application)
    essential_columns = [
        'RESOURCE_TYPE',
        'DOI_NAME',  # This is the only column actually used by get_collections_cmr.py
        'DOI_TITLE',
        'CREATOR',
        'DISTRIBUTOR',
        'YEAR',
        'URL',
        'SPECIAL',
        'MISSION',
        'INSTRUMENT',
        'PROVIDER',
        'TYPE',
        'READY_FLAG',
        'SUBMIT_DATE',
        'RESERVE_DATE',
        'REGISTER_DATE',
        'STATUS'
    ]
    
    # Define columns to remove (personal information)
    personal_columns = [
        'FIRSTNAME',
        'LASTNAME',
        'EMAIL',
        'LP_AGENCY',
        'LP_FIRSTNAME',
        'LP_LASTNAME',
        'LP_EMAIL'
    ]
    
    # Check which personal columns exist in the dataframe
    existing_personal_columns = [col for col in personal_columns if col in df.columns]
    
    if existing_personal_columns:
        print(f"Removing personal information columns: {existing_personal_columns}")
        df_cleaned = df.drop(columns=existing_personal_columns)
    else:
        print("No personal information columns found to remove.")
        df_cleaned = df
    
    # Ensure all essential columns are present
    missing_essential = [col for col in essential_columns if col not in df_cleaned.columns]
    if missing_essential:
        print(f"Warning: Missing essential columns: {missing_essential}")
    
    print(f"Cleaned columns: {list(df_cleaned.columns)}")
    print(f"Cleaned shape: {df_cleaned.shape}")
    
    # Determine output file path
    if output_file is None:
        output_file = input_file
        print(f"Overwriting original file: {output_file}")
    else:
        print(f"Saving to: {output_file}")
    
    # Save the cleaned data
    df_cleaned.to_csv(output_file, index=False)
    print(f"Successfully cleaned personal data from {input_file}")

def main():
    """Main function to clean the EOSDIS DOIs CSV file."""
    # Path to the data file
    data_dir = Path(__file__).parent / "data"
    input_file = data_dir / "eosdis_dois_02_19_2025.csv"
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return
    
    # Create backup of original file
    backup_file = data_dir / "eosdis_dois_02_19_2025.csv.backup"
    if not backup_file.exists():
        print(f"Creating backup: {backup_file}")
        import shutil
        shutil.copy2(input_file, backup_file)
    
    # Clean the personal data
    clean_personal_data(str(input_file))
    
    print("\nPersonal data cleaning completed successfully!")
    print(f"Backup of original file saved as: {backup_file}")

if __name__ == "__main__":
    main()
