"""
setup_table_properties.py

This script updates or verifies properties of a Delta Lake table on GCS using PySpark.
In particular, it is used to enable Change Data Feed (CDF) on the table by setting the table property
`delta.enableChangeDataFeed` to `true`. This property is required for Beam's Delta Lake CDC connector
to read incremental change logs.

Usage:
    python setup_table_properties.py --table-path <gcs-path> --gcp-key <path-to-json-key> --property-key <property-key> --property-value <property-val> [--view-only]

Arguments:
    --table-path:     GCS path to the Delta Lake table.
                      Defaults to gs://apache-beam-testing-chamikara/delta_lake/cdc_repo_1.
    --gcp-key:        Path to GCP service account key JSON file.
                      Defaults to /Users/chamikara/testing/delta_lake/cdc_test/apache-beam-testing-d09dad6d5500.json.
    --property-key:   The Delta table property key to modify.
                      Defaults to delta.enableChangeDataFeed.
    --property-value: The value to set for the specified property key.
                      Defaults to true.
    --view-only:      If set, only print the current properties without performing any updates.
"""

import argparse
import sys
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description="Setup or verify Delta Lake table properties using Spark.")
    parser.add_argument("--table-path", type=str, default="gs://apache-beam-testing-chamikara/delta_lake/cdc_repo_1",
                        help="GCS path to the Delta Lake table")
    parser.add_argument("--gcp-key", type=str, default="/Users/chamikara/testing/delta_lake/cdc_test/apache-beam-testing-d09dad6d5500.json",
                        help="Path to GCP service account key JSON file")
    parser.add_argument("--property-key", type=str, default="delta.enableChangeDataFeed",
                        help="Table property key to set")
    parser.add_argument("--property-value", type=str, default="true",
                        help="Value to set for the property")
    parser.add_argument("--view-only", action="store_true",
                        help="Only view/describe the table properties without modifying them")

    args = parser.parse_args()

    # Initialize Spark Session Builder with GCS connector configurations
    builder = SparkSession.builder \
        .appName("DeltaLakeTablePropertyUpdater") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.auth.type", "SERVICE_ACCOUNT_KEY_FILE") \
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", args.gcp_key)

    # Configure Delta Lake compatibility and packages dynamically
    spark = configure_spark_with_delta_pip(
        builder,
        extra_packages=["com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22"]
    ).getOrCreate()

    print(f"Checking if path is a Delta table: {args.table_path}")
    if DeltaTable.isDeltaTable(spark, args.table_path):
        if not args.view_only:
            print(f"Setting table property '{args.property_key}' to '{args.property_value}'...")
            spark.sql(f"""
                ALTER TABLE delta.`{args.table_path}`
                SET TBLPROPERTIES ('{args.property_key}' = '{args.property_value}')
            """)
            print("Property updated successfully.")

        # Verification step
        print("Verifying properties:")
        details_df = spark.sql(f"DESCRIBE DETAIL delta.`{args.table_path}`")
        details_df.select("properties").show(truncate=False)
    else:
        print(f"Error: Path {args.table_path} is not recognized as a Delta table.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
