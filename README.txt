Apache Beam Delta Lake & GCP Lakehouse Example Project
======================================================

This project contains Python scripts and a Maven Java pipeline designed to showcase and test:
1. Reading Change Data Feed (CDC) changes from Delta Lake tables hosted on Google Cloud Storage (GCS) using Apache Beam.
2. Altering and evolving the schema of GCP Lakehouse (Iceberg) tables on GCS / BigLake REST Catalog using Apache Beam SQL DDL.

Prerequisites
-------------
- Java JDK 11 or 17 (recommended/tested with JDK 17)
- Maven 3.x
- Python 3.10+ (tested with Python 3.13)
- Google Cloud Platform (GCP) credentials (service account JSON key file) with permissions to read/write to GCS and BigLake.

--------------------------------------------------------------------------------

Python Setup and Execution
--------------------------

1. Set up a virtual environment:
   $ python3 -m venv beam_sql_venv
   $ source beam_sql_venv/bin/activate	

2. Install dependencies:
   $ pip install --upgrade pip
   $ pip install "apache-beam[gcp]>=2.60.0" deltalake pandas pyspark delta-spark pyarrow google-cloud-storage google-cloud-bigquery

3. Description of Python Scripts:

   * alter_lakehouse_table_schema.py:
     Alters the schema of a GCP Lakehouse (Iceberg) table on GCS / BigLake REST Catalog using Apache Beam SQL DDL.
     Supports viewing schemas, adding columns, dropping columns, and executing arbitrary Beam SQL statements.
     Usage:
       $ python alter_lakehouse_table_schema.py --table <table-id> --gcp-key <path-to-gcp-key-json> (--add-columns "<col-defs>" | --drop-columns "<col-names>" | --sql "<sql-ddl>" | --view-only)
     Examples:
       # View current table schema
       $ python alter_lakehouse_table_schema.py --table apache-beam-testing.apache-beam-testing-chamikara.ns1.gcs_test_10 --view-only

       # Add columns via Beam SQL DDL
       $ python alter_lakehouse_table_schema.py --table apache-beam-testing.apache-beam-testing-chamikara.ns1.gcs_test_10 --add-columns "conference VARCHAR COMMENT 'Athletic Conference', rating DOUBLE"

       # Drop columns via Beam SQL DDL
       $ python alter_lakehouse_table_schema.py --table apache-beam-testing.apache-beam-testing-chamikara.ns1.gcs_test_10 --drop-columns "conference, rating"

   * get_commits.py:
     Retrieves the history/commit logs of a Delta Lake table on GCS, and saves them to a file. Useful for finding start and end versions of interest.
     Usage:
       $ python get_commits.py --table-path <gcs-path> --gcp-key <path-to-gcp-key-json> --output-file <output-file-name>
     Example (using default arguments):
       $ python get_commits.py

   * write_data.py:
     Writes (appends or overwrites) rows to a Delta Lake table on GCS. Useful for simulating insert and update mutations.
     Usage:
       $ python write_data.py --table-path <gcs-path> --gcp-key <path-to-gcp-key-json> --mode <append|overwrite> (--data <json-payload> | --data-file <path-to-json-file>) [--print-contents]
     Examples:
       $ python write_data.py --data '{"id":[999],"name":["David"],"role":["Lead"]}' --print-contents
       $ python write_data.py --data-file /path/to/data.json

   * delete_data.py:
     Deletes matching rows from a Delta Lake table on GCS using a SQL WHERE-like predicate. Useful for simulating delete mutations.
     Usage:
       $ python delete_data.py --predicate <where-clause> --table-path <gcs-path> --gcp-key <path-to-gcp-key-json>
     Example:
       $ python delete_data.py --predicate "id = 999"

   * setup_table_properties.py:
     Sets or verifies Delta Lake table properties (using PySpark). Specifically, this enables Change Data Feed (CDF) using the property `delta.enableChangeDataFeed = true`.
     Usage:
       $ python setup_table_properties.py --table-path <gcs-path> --gcp-key <path-to-gcp-key-json> --property-key <key> --property-value <val> [--view-only]
     Example:
       $ python setup_table_properties.py

--------------------------------------------------------------------------------

Java/Maven Program Setup and Execution
--------------------------------------

1. Navigate to the Maven project directory:
   $ cd maven-project/

2. Compile the project:
   $ mvn clean compile

3. Run the "AlterLakehouseTableSchema" program:
   The Java program executes Beam SQL DDL commands (CREATE CATALOG, USE CATALOG, USE DATABASE, ALTER TABLE) to alter the schema of GCP Lakehouse (Iceberg) tables.
   Set the environment variable GOOGLE_APPLICATION_CREDENTIALS to authorize BigLake REST catalog and GCS access.

   Usage:
     $ GOOGLE_APPLICATION_CREDENTIALS=<path-to-gcp-key-json> mvn exec:java \
       -Dexec.mainClass=org.apache.beam.examples.AlterLakehouseTableSchema \
       -Dexec.args="--gcpProject=<project> --warehouse=<gcs-bucket> --database=<db> --table=<table> (--addColumns='<cols>' | --dropColumns='<cols>' | --sql='<sql>' | --viewOnly=true)"

   Example:
     $ GOOGLE_APPLICATION_CREDENTIALS=/Users/chamikara/testing/delta_lake/cdc_test/apache-beam-testing-d09dad6d5500.json mvn exec:java \
       -Dexec.mainClass=org.apache.beam.examples.AlterLakehouseTableSchema \
       -Dexec.args="--gcpProject=apache-beam-testing --warehouse=gs://apache-beam-testing-chamikara --database=ns1 --table=gcs_test_10 --addColumns=\"season_year INT, division VARCHAR\""

4. Run the "DeltaLakeCDCRead" program:
   The Java program reads Change Data Feed changes within a range of commit versions and writes the formatted log output to a file or GCS path.

   Usage:
     $ GOOGLE_APPLICATION_CREDENTIALS=<path-to-gcp-key-json> mvn exec:java \
       -Dexec.mainClass=org.apache.beam.examples.DeltaLakeCDCRead \
       -Dexec.args="--repoPath=<gcs-path> --output=<local-or-gcs-output-path> --startCommitId=<start-id> --endCommitId=<end-id>"

   Example:
     $ GOOGLE_APPLICATION_CREDENTIALS=/Users/chamikara/testing/delta_lake/cdc_test/apache-beam-testing-d09dad6d5500.json mvn exec:java \
       -Dexec.mainClass=org.apache.beam.examples.DeltaLakeCDCRead \
       -Dexec.args="--repoPath=gs://apache-beam-testing-chamikara/delta_lake/cdc_repo_1/ --output=../output_java_test.txt --startCommitId=0 --endCommitId=16"

5. Running on different Beam Runners:
   You can specify active Maven profiles to run the application using different Beam runners (e.g. dataflow-runner, flink-runner, spark-runner).
   
   Example using Apache Spark:
     $ GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json mvn exec:java -Pspark-runner \
       -Dexec.mainClass=org.apache.beam.examples.DeltaLakeCDCRead \
       -Dexec.args="--runner=SparkRunner --repoPath=gs://my-bucket/delta-table/ --output=../spark_output.txt --startCommitId=0 --endCommitId=5"
