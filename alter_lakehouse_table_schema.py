#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Alter GCP Lakehouse (Iceberg) Table Schema using Apache Beam SQL DDL.

This script executes Beam SQL Data Definition Language (DDL) statements
to evolve table schemas (add columns, drop columns, alter properties)
for GCP Lakehouse tables managed by BigLake REST Catalog / GCS.

Reference: https://beam.apache.org/documentation/dsls/sql/ddl/
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_GCP_KEY = "/Users/chamikara/testing/delta_lake/cdc_test/apache-beam-testing-d09dad6d5500.json"
DEFAULT_TABLE = "apache-beam-testing.apache-beam-testing-chamikara.ns1.gcs_test_10"
DEFAULT_CATALOG_URI = "https://biglake.googleapis.com/iceberg/v1beta/restcatalog"
DEFAULT_CATALOG_NAME = "lakehouse_catalog"


def parse_table_identifier(table_id: str):
    """
    Parses full or partial table identifiers like:
      - project.warehouse_bucket.namespace.table
      - warehouse_bucket.namespace.table
      - namespace.table
      - table
    """
    parts = table_id.split(".")
    project = None
    warehouse = None
    database = "ns1"
    table = "gcs_test_10"

    if len(parts) == 4:
        project, warehouse_name, database, table = parts
        warehouse = f"gs://{warehouse_name}" if not warehouse_name.startswith("gs://") else warehouse_name
    elif len(parts) == 3:
        warehouse_name, database, table = parts
        warehouse = f"gs://{warehouse_name}" if not warehouse_name.startswith("gs://") else warehouse_name
    elif len(parts) == 2:
        database, table = parts
    elif len(parts) == 1:
        table = parts[0]

    return project, warehouse, database, table


def inspect_gcs_metadata(warehouse: str, database: str, table: str, gcp_key: Optional[str] = None):
    """Reads latest Iceberg metadata JSON directly from GCS to inspect table schema."""
    try:
        from google.cloud import storage

        if gcp_key and os.path.exists(gcp_key):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_key

        bucket_name = warehouse.replace("gs://", "").rstrip("/")
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        prefix = f"{database}/{table}/metadata/"
        blobs = list(client.list_blobs(bucket, prefix=prefix))
        meta_blobs = sorted(
            [b for b in blobs if b.name.endswith(".metadata.json")],
            key=lambda x: x.time_created,
            reverse=True,
        )

        if not meta_blobs:
            logger.warning("No Iceberg metadata JSON files found at %s%s", warehouse, prefix)
            return None

        latest_blob = meta_blobs[0]
        meta_json = json.loads(latest_blob.download_as_text())
        return {
            "metadata_file": latest_blob.name,
            "created_time": str(latest_blob.time_created),
            "table_uuid": meta_json.get("table-uuid"),
            "current_schema_id": meta_json.get("current-schema-id"),
            "schemas": meta_json.get("schemas", []),
            "current_snapshot_id": meta_json.get("current-snapshot-id"),
        }
    except Exception as e:
        logger.warning("Could not inspect metadata on GCS: %s", e)
        return None


def print_schema_details(title: str, metadata_info: Optional[dict]):
    """Pretty prints the schema fields from Iceberg metadata."""
    if not metadata_info:
        return

    print("=" * 80)
    print(f"{title}")
    print(f"Metadata File: {metadata_info.get('metadata_file')}")
    print(f"Current Schema ID: {metadata_info.get('current_schema_id')}")
    print("-" * 80)
    print(f"{'#':<4} | {'Field Name':<25} | {'Type':<15} | {'Required':<10} | {'Doc / Comment'}")
    print("-" * 80)

    current_id = metadata_info.get("current_schema_id", 0)
    schemas = metadata_info.get("schemas", [])
    target_schema = next((s for s in schemas if s.get("schema-id") == current_id), schemas[-1] if schemas else None)

    if target_schema:
        for idx, field in enumerate(target_schema.get("fields", []), start=1):
            f_name = field.get("name", "")
            f_type = field.get("type", "")
            if isinstance(f_type, dict):
                f_type = f_type.get("type", str(f_type))
            f_req = "Yes" if field.get("required", False) else "No"
            f_doc = field.get("doc", "") or ""
            print(f"{idx:<4} | {f_name:<25} | {str(f_type):<15} | {f_req:<10} | {f_doc}")
    print("=" * 80)


def execute_beam_sql_alter(
    maven_project_dir: Path,
    gcp_key: str,
    project: str,
    catalog_name: str,
    catalog_uri: str,
    warehouse: str,
    database: str,
    table: str,
    sql: Optional[str] = None,
    add_columns: Optional[str] = None,
    drop_columns: Optional[str] = None,
    view_only: bool = False,
):
    """Executes the Beam SQL DDL command via the Maven project runner."""
    if not maven_project_dir.exists():
        raise FileNotFoundError(f"Maven project directory not found: {maven_project_dir}")

    # Build exec.args
    args_list = [
        f"--gcpProject={project}",
        f"--catalogName={catalog_name}",
        f"--catalogUri={catalog_uri}",
        f"--warehouse={warehouse}",
        f"--database={database}",
        f"--table={table}",
    ]

    if view_only:
        args_list.append("--viewOnly=true")
    else:
        if sql:
            # Escape quotes if necessary
            args_list.append(f'--sql="{sql}"')
        elif add_columns:
            args_list.append(f'--addColumns="{add_columns}"')
        elif drop_columns:
            args_list.append(f'--dropColumns="{drop_columns}"')
        else:
            raise ValueError("Must specify one of --sql, --add-columns, --drop-columns, or --view-only")

    env = os.environ.copy()
    if gcp_key:
        env["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(gcp_key)

    exec_args = " ".join(args_list)
    cmd = [
        "mvn",
        "exec:java",
        "-Dexec.mainClass=org.apache.beam.examples.AlterLakehouseTableSchema",
        f"-Dexec.args={exec_args}",
    ]

    logger.info("Running Beam SQL alteration via Maven in: %s", maven_project_dir)
    logger.info("Command: %s", " ".join(cmd))

    result = subprocess.run(cmd, cwd=maven_project_dir, env=env, check=False)
    if result.returncode != 0:
        logger.error("Beam SQL execution failed with return code %d", result.returncode)
        sys.exit(result.returncode)

    logger.info("Beam SQL execution completed successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="Alter Schema of GCP Lakehouse (Iceberg) Table using Apache Beam SQL DDL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  1. View current table schema:
     python alter_lakehouse_table_schema.py --table apache-beam-testing.apache-beam-testing-chamikara.ns1.gcs_test_10 --view-only

  2. Add new column to GCP Lakehouse table:
     python alter_lakehouse_table_schema.py \\
       --table apache-beam-testing.apache-beam-testing-chamikara.ns1.gcs_test_10 \\
       --add-columns "rating DOUBLE COMMENT 'Mascot popularity rating', reviewer VARCHAR"

  3. Drop column from GCP Lakehouse table:
     python alter_lakehouse_table_schema.py \\
       --table apache-beam-testing.apache-beam-testing-chamikara.ns1.gcs_test_10 \\
       --drop-columns "rating, reviewer"

  4. Execute arbitrary Beam SQL DDL statement:
     python alter_lakehouse_table_schema.py \\
       --table apache-beam-testing.apache-beam-testing-chamikara.ns1.gcs_test_10 \\
       --sql "ALTER TABLE gcs_test_10 ADD COLUMNS (audit_ts TIMESTAMP)"
""",
    )

    parser.add_argument(
        "--table",
        type=str,
        default=DEFAULT_TABLE,
        help=f"Fully qualified or partial table identifier. Default: {DEFAULT_TABLE}",
    )
    parser.add_argument(
        "--gcp-key",
        type=str,
        default=DEFAULT_GCP_KEY,
        help=f"Path to GCP Service Account JSON Key file. Default: {DEFAULT_GCP_KEY}",
    )
    parser.add_argument(
        "--gcp-project",
        type=str,
        default="apache-beam-testing",
        help="GCP Project ID for BigLake/Lakehouse. Default: apache-beam-testing",
    )
    parser.add_argument(
        "--catalog-name",
        type=str,
        default=DEFAULT_CATALOG_NAME,
        help=f"Catalog identifier name in Beam SQL session. Default: {DEFAULT_CATALOG_NAME}",
    )
    parser.add_argument(
        "--catalog-uri",
        type=str,
        default=DEFAULT_CATALOG_URI,
        help=f"BigLake Iceberg REST Catalog URI. Default: {DEFAULT_CATALOG_URI}",
    )
    parser.add_argument(
        "--warehouse",
        type=str,
        default="gs://apache-beam-testing-chamikara",
        help="GCS Warehouse path (gs://bucket). Default: gs://apache-beam-testing-chamikara",
    )
    parser.add_argument(
        "--database",
        "--namespace",
        dest="database",
        type=str,
        default="ns1",
        help="Database / Namespace name. Default: ns1",
    )
    parser.add_argument(
        "--table-name",
        type=str,
        default="gcs_test_10",
        help="Table name. Default: gcs_test_10",
    )
    parser.add_argument(
        "--sql",
        type=str,
        default=None,
        help="Full ALTER TABLE SQL statement (e.g. \"ALTER TABLE gcs_test_10 ADD COLUMNS (notes VARCHAR)\")",
    )
    parser.add_argument(
        "--add-columns",
        type=str,
        default=None,
        help="Columns definition to add (e.g. \"notes VARCHAR COMMENT 'Notes', score INT\")",
    )
    parser.add_argument(
        "--drop-columns",
        type=str,
        default=None,
        help="Columns to drop (e.g. \"notes, score\")",
    )
    parser.add_argument(
        "--view-only",
        action="store_true",
        help="View current table schema and metadata without making any modifications",
    )
    parser.add_argument(
        "--maven-project-dir",
        type=str,
        default=None,
        help="Path to maven-project directory containing Beam SQL runner. Default: <script_dir>/maven-project",
    )

    args = parser.parse_args()

    # Determine Maven project directory
    if args.maven_project_dir:
        maven_dir = Path(args.maven_project_dir).resolve()
    else:
        script_dir = Path(__file__).parent.resolve()
        maven_dir = script_dir / "maven-project"
        if not maven_dir.exists():
            maven_dir = script_dir

    # Parse table identifier
    parsed_project, parsed_warehouse, parsed_db, parsed_table = parse_table_identifier(args.table)

    project = args.gcp_project or parsed_project or "apache-beam-testing"
    warehouse = args.warehouse or parsed_warehouse or "gs://apache-beam-testing-chamikara"
    database = args.database if args.database != "ns1" or not parsed_db else parsed_db
    table = args.table_name if args.table_name != "gcs_test_10" or not parsed_table else parsed_table

    if parsed_project:
        project = parsed_project
    if parsed_warehouse:
        warehouse = parsed_warehouse
    if parsed_db:
        database = parsed_db
    if parsed_table:
        table = parsed_table

    logger.info("Target GCP Lakehouse Table: %s.%s.%s (Warehouse: %s)", project, database, table, warehouse)
    logger.info("Authentication Key File: %s", args.gcp_key)

    # 1. Show Before Schema
    meta_before = inspect_gcs_metadata(warehouse, database, table, args.gcp_key)
    print_schema_details("BEFORE: Current GCP Lakehouse Table Schema", meta_before)

    if args.view_only:
        logger.info("View-only mode completed.")
        return

    # 2. Execute Beam SQL Alteration
    execute_beam_sql_alter(
        maven_project_dir=maven_dir,
        gcp_key=args.gcp_key,
        project=project,
        catalog_name=args.catalog_name,
        catalog_uri=args.catalog_uri,
        warehouse=warehouse,
        database=database,
        table=table,
        sql=args.sql,
        add_columns=args.add_columns,
        drop_columns=args.drop_columns,
        view_only=args.view_only,
    )

    # 3. Show After Schema
    meta_after = inspect_gcs_metadata(warehouse, database, table, args.gcp_key)
    print_schema_details("AFTER: Updated GCP Lakehouse Table Schema", meta_after)


if __name__ == "__main__":
    main()
