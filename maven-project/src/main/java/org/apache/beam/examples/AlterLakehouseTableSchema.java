/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.extensions.sql.meta.provider.iceberg.IcebergCatalog;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility using Beam SQL DDL to alter the schema of a GCP Lakehouse (Iceberg) table.
 *
 * <p>Supports adding columns, dropping columns, setting table properties, and modifying partition specs
 * via standard Beam SQL DDL statements (CREATE CATALOG, USE CATALOG, USE DATABASE, ALTER TABLE).
 */
public class AlterLakehouseTableSchema {
  private static final Logger LOG = LoggerFactory.getLogger(AlterLakehouseTableSchema.class);

  public interface AlterSchemaOptions extends PipelineOptions {
    @Description("Name to give to the Beam SQL catalog instance")
    @Default.String("lakehouse_catalog")
    String getCatalogName();

    void setCatalogName(String value);

    @Description("GCP Project ID for BigLake/Lakehouse")
    @Default.String("apache-beam-testing")
    String getGcpProject();

    void setGcpProject(String value);

    @Description("BigLake Iceberg REST Catalog URI")
    @Default.String("https://biglake.googleapis.com/iceberg/v1beta/restcatalog")
    String getCatalogUri();

    void setCatalogUri(String value);

    @Description("GCS Warehouse location (e.g. gs://bucket-name)")
    @Default.String("gs://apache-beam-testing-chamikara")
    String getWarehouse();

    void setWarehouse(String value);

    @Description("Lakehouse database / namespace name (e.g. ns1)")
    @Default.String("ns1")
    String getDatabase();

    void setDatabase(String value);

    @Description("Lakehouse table name (e.g. gcs_test_10)")
    @Default.String("gcs_test_10")
    String getTable();

    void setTable(String value);

    @Description("Full ALTER TABLE SQL statement, or columns to add")
    String getSql();

    void setSql(String value);

    @Description("Columns to add (e.g. 'new_col VARCHAR, score INTEGER') if --sql is not directly provided")
    String getAddColumns();

    void setAddColumns(String value);

    @Description("Columns to drop (e.g. 'old_col1, old_col2') if --sql is not directly provided")
    String getDropColumns();

    void setDropColumns(String value);

    @Description("Only view current schema and metadata without altering")
    @Default.Boolean(false)
    boolean getViewOnly();

    void setViewOnly(boolean value);
  }

  private static String cleanSql(String sql) {
    if (sql == null) {
      return null;
    }
    String trimmed = sql.trim();
    while (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
    }
    return trimmed;
  }

  public static void main(String[] args) {
    AlterSchemaOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AlterSchemaOptions.class);

    String catalogName = options.getCatalogName();
    String catalogUri = options.getCatalogUri();
    String warehouse = options.getWarehouse();
    String project = options.getGcpProject();
    String database = options.getDatabase();
    String table = options.getTable();

    LOG.info("Initializing InMemoryCatalogManager and BeamSqlCli...");
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlCli cli = new BeamSqlCli().catalogManager(catalogManager);

    // 1. Construct CREATE CATALOG DDL
    String createCatalogDdl =
        cleanSql(
            String.format(
                "CREATE CATALOG `%s`\n"
                    + "TYPE iceberg\n"
                    + "PROPERTIES (\n"
                    + "  'type' = 'rest',\n"
                    + "  'uri' = '%s',\n"
                    + "  'warehouse' = '%s',\n"
                    + "  'header.x-goog-user-project' = '%s',\n"
                    + "  'rest.auth.type' = 'org.apache.iceberg.gcp.auth.GoogleAuthManager',\n"
                    + "  'io-impl' = 'org.apache.iceberg.gcp.gcs.GCSFileIO',\n"
                    + "  'rest-metrics-reporting-enabled' = 'false'\n"
                    + ")",
                catalogName, catalogUri, warehouse, project));

    LOG.info("Executing CREATE CATALOG DDL:\n{}", createCatalogDdl);
    cli.execute(createCatalogDdl);

    // 2. Switch Catalog & Database
    String useCatalogSql = cleanSql(String.format("USE CATALOG `%s`", catalogName));
    LOG.info("Executing: {}", useCatalogSql);
    cli.execute(useCatalogSql);

    String useDatabaseSql = cleanSql(String.format("USE DATABASE `%s`", database));
    LOG.info("Executing: {}", useDatabaseSql);
    cli.execute(useDatabaseSql);

    // Inspect table before altering
    IcebergCatalog icebergCatalog = (IcebergCatalog) catalogManager.getCatalog(catalogName);
    if (icebergCatalog != null) {
      try {
        Table beforeTable = icebergCatalog.metaStore(database).getTable(table);
        if (beforeTable != null) {
          System.out.println("=================================================");
          System.out.println("CURRENT TABLE SCHEMA FOR " + database + "." + table + ":");
          System.out.println(beforeTable.getSchema());
          System.out.println("=================================================");
        }
      } catch (Exception e) {
        LOG.warn("Could not load table before alteration for schema inspection: {}", e.getMessage());
      }
    }

    if (options.getViewOnly()) {
      LOG.info("View-only mode specified. Skipping alterations.");
      return;
    }

    // 3. Determine ALTER TABLE SQL
    String alterSql = cleanSql(options.getSql());
    if (alterSql == null || alterSql.trim().isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("ALTER TABLE `").append(table).append("`");
      boolean hasClause = false;
      if (options.getDropColumns() != null && !options.getDropColumns().trim().isEmpty()) {
        sb.append(" DROP COLUMNS (").append(options.getDropColumns().trim()).append(")");
        hasClause = true;
      }
      if (options.getAddColumns() != null && !options.getAddColumns().trim().isEmpty()) {
        sb.append(" ADD COLUMNS (").append(options.getAddColumns().trim()).append(")");
        hasClause = true;
      }
      if (!hasClause) {
        throw new IllegalArgumentException(
            "Must provide either --sql, or --addColumns, or --dropColumns.");
      }
      alterSql = sb.toString();
    }

    LOG.info("Executing ALTER TABLE DDL:\n{}", alterSql);
    cli.execute(alterSql);

    // 4. Verify updated table schema with fresh catalog manager
    try {
      InMemoryCatalogManager verifyManager = new InMemoryCatalogManager();
      BeamSqlCli verifyCli = new BeamSqlCli().catalogManager(verifyManager);
      verifyCli.execute(createCatalogDdl);
      IcebergCatalog freshCatalog = (IcebergCatalog) verifyManager.getCatalog(catalogName);
      if (freshCatalog != null) {
        Table afterTable = freshCatalog.metaStore(database).getTable(table);
        if (afterTable != null) {
          System.out.println("=================================================");
          System.out.println("UPDATED TABLE SCHEMA FOR " + database + "." + table + ":");
          System.out.println(afterTable.getSchema());
          System.out.println("=================================================");
        }
      }
      LOG.info("Successfully executed schema alteration for table {}.{}.{}", catalogName, database, table);
    } catch (Exception e) {
      LOG.error("Failed to reload table after alteration: {}", e.getMessage(), e);
    }
  }
}
