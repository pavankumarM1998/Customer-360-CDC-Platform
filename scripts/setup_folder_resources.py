"""
Setup Script: Databricks Unity Catalog Volume Folder Hierarchy
Platform: Customer 360 CDC Platform

This script provisions the required landing/staging subdirectories inside 
the Databricks Unity Catalog Volume `/Volumes/customer360_dev/staging/source_files/`.
"""

import os
import sys

BASE_VOLUME_PATH = "/Volumes/customer360_dev/staging/source_files"

ENTITIES = [
    "customers",
    "addresses",
    "subscriptions",
    "products",
    "orders",
    "order_items",
    "payments",
    "support_tickets"
]

SUBFOLDERS = ["initial", "incremental", "archive"]

def setup_volume_directories():
    print(f"Provisioning Unity Catalog Volume directories under: {BASE_VOLUME_PATH}\n" + "="*70)
    
    # Check if running in Databricks environment with dbutils
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        is_databricks = True
    except ImportError:
        is_databricks = False
        
    created_count = 0
    for entity in ENTITIES:
        for sub in SUBFOLDERS:
            folder_path = f"{BASE_VOLUME_PATH}/{entity}/{sub}"
            if is_databricks:
                dbutils.fs.mkdirs(folder_path)
                print(f"[Databricks DBFS] Created Volume directory: {folder_path}")
            else:
                os.makedirs(folder_path, exist_ok=True)
                print(f"[Local Path] Created directory target: {folder_path}")
            created_count += 1
            
    print("="*70)
    print(f"Successfully initialized {created_count} volume subdirectories across {len(ENTITIES)} entities.")

if __name__ == "__main__":
    setup_volume_directories()
