#!/usr/bin/env python3
"""
Download Open Target Platform parquet data for loading into Snowflake.

This script downloads the latest Open Target Platform data in parquet format
from Google Cloud Storage and prepares it for loading into Snowflake using dbt.
"""

import os
import sys
import argparse
import logging
import subprocess
from pathlib import Path
import requests
from tqdm import tqdm
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('opentarget-downloader')

# Open Target Platform data release version
DEFAULT_OT_VERSION = '23.09'  # Update this to the latest version as needed

# Base URL for Open Target Platform data
BASE_URL = 'https://storage.googleapis.com/open-targets-data-releases'

# Data types to download
DATA_TYPES = {
    'evidence': 'evidence/sourceId=',
    'targets': 'parquet/targets',
    'diseases': 'parquet/diseases',
    'associations': 'parquet/associationByDatasource',
    'interactions': 'parquet/interactions',
}

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Download Open Target Platform data')
    parser.add_argument('--version', type=str, default=DEFAULT_OT_VERSION,
                        help=f'Open Target Platform data version (default: {DEFAULT_OT_VERSION})')
    parser.add_argument('--output-dir', type=str, default='data',
                        help='Directory to store downloaded data (default: data)')
    parser.add_argument('--data-types', nargs='+', choices=list(DATA_TYPES.keys()) + ['all'],
                        default=['all'], help='Data types to download')
    parser.add_argument('--list-only', action='store_true',
                        help='Only list available files without downloading')
    return parser.parse_args()

def ensure_directory(directory):
    """Ensure the directory exists."""
    Path(directory).mkdir(parents=True, exist_ok=True)
    return directory

def list_available_files(version, data_type):
    """List available files for a given data type and version."""
    if data_type not in DATA_TYPES:
        logger.error(f"Unknown data type: {data_type}")
        return []
    
    url = f"{BASE_URL}/{version}/{DATA_TYPES[data_type]}"
    
    try:
        # For evidence data, we need to list the source IDs first
        if data_type == 'evidence':
            # This is a simplified approach - in production, you'd need to use gsutil or GCS API
            # to list the actual files
            source_ids = [
                'chembl', 'europepmc', 'eva', 'eva_somatic', 'gene2phenotype',
                'genomics_england', 'intogen', 'phenodigm', 'phewas_catalog',
                'progeny', 'reactome', 'slapenrich', 'sysbio', 'uniprot', 'uniprot_literature',
                'uniprot_somatic', 'crispr'
            ]
            files = []
            for source_id in source_ids:
                files.append(f"{url}{source_id}")
            return files
        else:
            # For other data types, just return the base URL
            return [url]
    except Exception as e:
        logger.error(f"Error listing files for {data_type}: {e}")
        return []

def download_file(url, output_path):
    """Download a file from a URL to the specified output path."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024  # 1 KB
        
        with open(output_path, 'wb') as f, tqdm(
            desc=os.path.basename(output_path),
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for data in response.iter_content(block_size):
                f.write(data)
                bar.update(len(data))
        
        logger.info(f"Downloaded {url} to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return False

def download_parquet_data(version, output_dir, data_types):
    """Download Open Target Platform parquet data."""
    if 'all' in data_types:
        data_types = list(DATA_TYPES.keys())
    
    for data_type in data_types:
        logger.info(f"Processing {data_type} data...")
        
        # Create data type specific directory
        data_dir = ensure_directory(os.path.join(output_dir, data_type))
        
        # List available files
        files = list_available_files(version, data_type)
        
        if not files:
            logger.warning(f"No files found for {data_type}")
            continue
        
        for file_url in files:
            # Extract filename from URL
            filename = os.path.basename(file_url)
            if not filename:
                filename = f"{data_type}.parquet"
            
            output_path = os.path.join(data_dir, filename)
            
            # Download the file
            download_file(file_url, output_path)

def main():
    """Main entry point."""
    args = parse_args()
    
    logger.info(f"Open Target Platform data downloader - version {args.version}")
    
    # Ensure output directory exists
    output_dir = ensure_directory(args.output_dir)
    logger.info(f"Output directory: {output_dir}")
    
    if args.list_only:
        logger.info("Listing available files...")
        for data_type in args.data_types:
            if data_type == 'all':
                for dt in DATA_TYPES:
                    files = list_available_files(args.version, dt)
                    logger.info(f"{dt}: {len(files)} file(s)")
            else:
                files = list_available_files(args.version, data_type)
                logger.info(f"{data_type}: {len(files)} file(s)")
    else:
        logger.info("Downloading data...")
        download_parquet_data(args.version, output_dir, args.data_types)
        logger.info("Download completed.")

if __name__ == "__main__":
    main()
