package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.delta.DeltaIO;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * An example Apache Beam pipeline that reads Change Data Feed (CDC) records
 * from a Delta Lake table
 * on Google Cloud Storage (GCS) and writes the output to a text file.
 *
 * <p>
 * This pipeline utilizes Beam's
 * {@link org.apache.beam.sdk.managed.Managed#read(String)} with
 * {@code Managed.DELTA_LAKE_CDC} configuration.
 *
 * <h3>Requirements</h3>
 * <ul>
 * <li>Enable Change Data Feed on the source Delta Lake table (e.g. using
 * `delta.enableChangeDataFeed = true`).</li>
 * <li>Set the environment variable {@code GOOGLE_APPLICATION_CREDENTIALS} to
 * authorize the pipeline to access GCS.</li>
 * </ul>
 *
 * <h3>Running the Example</h3>
 * 
 * <pre>{@code
 * GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json mvn compile exec:java \
 *   -Dexec.mainClass=org.apache.beam.examples.DeltaLakeCDCRead \
 *   -Dexec.args="--repoPath=gs://my-bucket/delta-table --output=./output.txt --startCommitId=0 --endCommitId=5"
 * }</pre>
 */
public class DeltaLakeCDCRead {

        public interface DeltaLakeCDCOptions extends PipelineOptions {

                /**
                 * The GCS path of the Delta Lake table repository to read from (e.g.
                 * gs://my-bucket/delta-table/).
                 */
                @Description("Path of the Delta Lake table repository")
                @Validation.Required
                String getRepoPath();

                void setRepoPath(String repoPath);

                /**
                 * The local or GCS path prefix of the text file where output rows will be
                 * written.
                 */
                @Description("Path of the output file to write to")
                @Validation.Required
                String getOutput();

                void setOutput(String value);

                /**
                 * The starting commit version (inclusive) to read from the Delta Lake Change
                 * Data Feed.
                 */
                @Description("Start commit version (inclusive)")
                @Validation.Required
                Integer getStartCommitId();

                void setStartCommitId(Integer startCommitId);

                /**
                 * The ending commit version (inclusive) to read from the Delta Lake Change Data
                 * Feed.
                 */
                @Description("End commit version (inclusive)")
                @Validation.Required
                Integer getEndCommitId();

                void setEndCommitId(Integer endCommitId);
        }

        static void runDeltaLakeCDCRead(DeltaLakeCDCOptions options) {
                Pipeline pipeline = Pipeline.create(options);

                Map<String, String> hadoopConfig = new HashMap<>();
                hadoopConfig.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
                hadoopConfig.put(
                                "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
                hadoopConfig.put("fs.gs.auth.type", "APPLICATION_DEFAULT");
                String project = pipeline
                                .getOptions()
                                .as(org.apache.beam.sdk.extensions.gcp.options.GcpOptions.class)
                                .getProject();
                if (project != null) {
                        hadoopConfig.put("fs.gs.project.id", project);
                }

                Map<String, Object> readConfig = new HashMap<>();
                readConfig.put("table", options.getRepoPath());
                readConfig.put("start_version", options.getStartCommitId());
                readConfig.put("end_version", options.getEndCommitId());
                readConfig.put("hadoop_config", hadoopConfig);
                readConfig.put(
                                "include_metadata_columns",
                                java.util.Arrays.asList(
                                                DeltaIO.CHANGE_TYPE_COLUMN,
                                                DeltaIO.COMMIT_VERSION_COLUMN,
                                                DeltaIO.COMMIT_TIMESTAMP_COLUMN));

                PCollection<Row> output = pipeline
                                .apply(Managed.read(Managed.DELTA_LAKE_CDC).withConfig(readConfig))
                                .getSinglePCollection();

                PCollection<String> formattedOutput = output.apply("Format Row with Metadata",
                                ParDo.of(new FormatITRowWithMetadata()));

                formattedOutput.apply("WriteCounts", TextIO.write().to(options.getOutput()));

                pipeline.run().waitUntilFinish();
        }

        private static final class FormatITRowWithMetadata extends DoFn<Row, String> {
                @DoFn.ProcessElement
                public void process(@Element Row row, OutputReceiver<String> out) {
                        out.output(
                                        String.format(
                                                        "%d:%s:%s:%s:v%d",
                                                        row.getInt64("id"),
                                                        row.getString("name"),
                                                        row.getString("role"),
                                                        row.getString(DeltaIO.CHANGE_TYPE_COLUMN),
                                                        row.getInt64(DeltaIO.COMMIT_VERSION_COLUMN)));
                }
        }

        public static void main(String[] args) {
                DeltaLakeCDCOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                                .as(DeltaLakeCDCOptions.class);
                runDeltaLakeCDCRead(options);
        }

}
