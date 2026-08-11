"""
write_data.py

This script writes (appends or overwrites) rows to a Delta Lake table located on Google Cloud Storage (GCS).
This is useful for writing test data payload to simulate insert or update events in the Delta Lake table,
enabling integration tests to trace incremental commit changes (CDC changes).

Usage:
    python write_data.py --table-path <gcs-path> --gcp-key <path-to-json-key> --mode <append|overwrite> (--data <json-payload> | --data-file <path-to-json-file>)

Arguments:
    --table-path:  GCS path to the Delta Lake table.
                   Defaults to gs://apache-beam-testing-chamikara/delta_lake/cdc_repo_1/.
    --gcp-key:     Path to GCP service account key JSON file.
                   Defaults to /Users/chamikara/testing/delta_lake/cdc_test/apache-beam-testing-d09dad6d5500.json.
    --mode:        Write mode: 'append' to add rows or 'overwrite' to replace all rows.
                   Defaults to append.
    --data:        JSON string representing the pandas DataFrame/data payload to write.
                   Example: '{"id": [10, 20], "name": ["Alice", "Bob"], "role": ["Eng", "Analyst"]}'
                   Cannot be used together with --data-file.
    --data-file:   Path to a JSON file containing the data payload to write.
                   Cannot be used together with --data.
                   If both --data and --data-file are omitted, a default data payload will be written.
    --print-contents: Print the full contents of the Delta table to standard output before and after writing.
                   Disabled (False) by default.
"""

import argparse
import json
import os
import pandas as pd
from deltalake import write_deltalake
from deltalake import DeltaTable

def main():
    parser = argparse.ArgumentParser(description="Write rows to Delta Lake table on GCS.")
    parser.add_argument("--table-path", type=str, default="gs://apache-beam-testing-chamikara/delta_lake/cdc_repo_1/",
                        help="GCS path to the Delta Lake table")
    parser.add_argument("--gcp-key", type=str, default="/Users/chamikara/testing/delta_lake/cdc_test/apache-beam-testing-d09dad6d5500.json",
                        help="Path to GCP service account key JSON file")
    parser.add_argument("--mode", type=str, choices=["append", "overwrite"], default="append",
                        help="Write mode: append or overwrite")
    parser.add_argument("--data", type=str,
                        help="JSON string representing the data to write, e.g. "
                             "'{\"id\": [10, 20], \"name\": [\"Alice\", \"Bob\"], \"role\": [\"Eng\", \"Analyst\"]}'")
    parser.add_argument("--data-file", type=str,
                        help="Path to a JSON file containing the data to write.")
    parser.add_argument("--print-contents", action="store_true",
                        help="Print table contents before and after writing (default: False)")

    args = parser.parse_args()

    if args.data and args.data_file:
        print("Error: Specify either --data or --data-file, not both.")
        return

    if args.data:
        try:
            data = json.loads(args.data)
        except Exception as e:
            print(f"Error parsing JSON data string: {e}")
            return
    elif args.data_file:
        try:
            with open(args.data_file, "r") as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading or parsing JSON file {args.data_file}: {e}")
            return
    else:
        # Default fallback data
        data = {
            "id": [10, 20, 30],
            "name": ["Alice1", "Bob2", "Charlie3"],
            "role": ["Engineer1", "Analyst2", "Manager3"]
        }

    df = pd.DataFrame(data)

    storage_options = {}
    if os.path.exists(args.gcp_key):
        print(f"Using GCP service account key: {args.gcp_key}")
        storage_options["google_service_account"] = args.gcp_key
    elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        print("Using GOOGLE_APPLICATION_CREDENTIALS environment variable.")
    else:
        print("Warning: No GCP credentials found. Attempting connection using default/anonymous credentials.")

    if args.print_contents:
        print(f"Loading Delta Table from {args.table_path}...")
        try:
            dt = DeltaTable(args.table_path, storage_options=storage_options)
            print("Table contents before write:")
            print(dt.to_pandas())
        except Exception as e:
            print(f"Error loading Delta Table before write (table might not exist yet): {e}")

    print(f"Formatting transaction logs and saving delta blocks to GCS using mode: {args.mode}...")
    try:
        write_deltalake(args.table_path, df, mode=args.mode, storage_options=storage_options)
        print("Delta Lake table successfully generated in GCS!")
    except Exception as e:
        print(f"Error writing to Delta Table: {e}")
        return

    if args.print_contents:
        print("Table contents after insertion:")
        try:
            dt = DeltaTable(args.table_path, storage_options=storage_options)
            print(dt.to_pandas())
        except Exception as e:
            print(f"Error loading Delta Table after write: {e}")

if __name__ == "__main__":
    main()