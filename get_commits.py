"""
get_commits.py

This script retrieves the history and commit logs of a Delta Lake table located on Google Cloud Storage (GCS).
It lists the versions and timestamps for all commits, helping to track changes and identify specific
versions (commit IDs) of interest, which is useful when reading Delta Lake change data feed (CDF).

Usage:
    python get_commits.py --table-path <gcs-path> --gcp-key <path-to-json-key> --output-file <output-txt-file>

Arguments:
    --table-path:  GCS path to the Delta Lake table (e.g., gs://my-bucket/delta-table).
                   Defaults to gs://apache-beam-testing-chamikara/delta_lake/cdc_repo_1/.
    --gcp-key:     Path to GCP service account key JSON file.
                   Defaults to /Users/chamikara/testing/delta_lake/cdc_test/apache-beam-testing-d09dad6d5500.json.
    --output-file: Text file where the commit history (versions and timestamps) will be saved.
                   Defaults to commit_ids.txt.
"""

import argparse
import os
from datetime import datetime, timezone
from deltalake import DeltaTable

def main():
    parser = argparse.ArgumentParser(description="Get history/commits of Delta Lake table on GCS.")
    parser.add_argument("--table-path", type=str, default="gs://apache-beam-testing-chamikara/delta_lake/cdc_repo_1/",
                        help="GCS path to the Delta Lake table")
    parser.add_argument("--gcp-key", type=str, default="/Users/chamikara/testing/delta_lake/cdc_test/apache-beam-testing-d09dad6d5500.json",
                        help="Path to GCP service account key JSON file")
    parser.add_argument("--output-file", type=str, default="commit_ids.txt",
                        help="Output text file path for saving commit IDs and timestamps")
    
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
        history = dt.history()
        
        # Sort history by version in ascending order (chronological)
        sorted_history = sorted(history, key=lambda x: x.get('version', 0))
        
        with open(args.output_file, "w") as f:
            for commit in sorted_history:
                version = commit.get('version')
                timestamp_ms = commit.get('timestamp')
                if timestamp_ms is not None:
                    # Convert milliseconds to datetime object in UTC
                    dt_obj = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                    timestamp_str = dt_obj.strftime('%Y-%m-%d %H:%M:%S %Z')
                else:
                    timestamp_str = "N/A"
                    
                f.write(f"Commit ID (Version): {version}, Timestamp: {timestamp_str} ({timestamp_ms} ms)\n")
                
        print(f"Commit IDs and timestamps successfully written to {args.output_file}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
