"""
delete_data.py

This script deletes rows matching a specific predicate (SQL WHERE clause) from a Delta Lake table on Google Cloud Storage (GCS).
This is useful for simulating CDC (Change Data Capture) delete events, allowing integration tests
to verify that delete operations are correctly tracked and propagated through the pipeline.

Usage:
    python delete_data.py --predicate <where-clause> --table-path <gcs-path> --gcp-key <path-to-json-key>

Arguments:
    --predicate:   SQL WHERE clause predicate for rows to delete (e.g., 'id = 3' or "role = 'Manager'").
                   This is a required parameter.
    --table-path:  GCS path to the Delta Lake table.
                   Defaults to gs://apache-beam-testing-chamikara/delta_lake/cdc_repo_1/.
    --gcp-key:     Path to GCP service account key JSON file.
                   Defaults to /Users/chamikara/testing/delta_lake/cdc_test/apache-beam-testing-d09dad6d5500.json.
"""

import argparse
import os
from deltalake import DeltaTable

def main():
    parser = argparse.ArgumentParser(description="Delete rows from Delta Lake table on GCS.")
    parser.add_argument("--predicate", type=str, required=True,
                        help="SQL WHERE clause predicate for rows to delete (e.g. 'id = 3' or 'role = \\'Manager\\'')")
    parser.add_argument("--table-path", type=str, default="gs://apache-beam-testing-chamikara/delta_lake/cdc_repo_1/",
                        help="GCS path to the Delta Lake table")
    parser.add_argument("--gcp-key", type=str, default="/Users/chamikara/testing/delta_lake/cdc_test/apache-beam-testing-d09dad6d5500.json",
                        help="Path to GCP service account key JSON file")
    
    args = parser.parse_args()
    
    storage_options = {}
    if os.path.exists(args.gcp_key):
        print(f"Using GCP service account key: {args.gcp_key}")
        storage_options["google_service_account"] = args.gcp_key
    elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        print("Using GOOGLE_APPLICATION_CREDENTIALS environment variable.")
    else:
        print("Warning: No GCP credentials found. Attempting connection using default/anonymous credentials.")
        
    print(f"Loading Delta Table from {args.table_path}...")
    try:
        dt = DeltaTable(args.table_path, storage_options=storage_options)
    except Exception as e:
        print(f"Error loading Delta Table: {e}")
        return

    print("Table contents before deletion:")
    print(dt.to_pandas())
    
    print(f"Deleting rows where: {args.predicate}")
    try:
        metrics = dt.delete(predicate=args.predicate)
        print("Delete operation successful!")
        print("Metrics:")
        print(metrics)
    except Exception as e:
        print(f"Error executing delete: {e}")
        return
        
    dt.update_incremental()
    print("Table contents after deletion:")
    print(dt.to_pandas())

if __name__ == "__main__":
    main()
