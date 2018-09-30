package org.iotope.beam.demo;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.iotope.beam.demo.splittable.BoundedThousandFn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class DemoPipelineBase implements Serializable {

    private static Logger LOG = Logger.getLogger(DemoPipelineBase.class.toString());
    public static final String GOOGLE_PROJECT_ID = "GOOGLE_PROJECT_ID";
    public static final String MONGO_URL = "MONGO_URL";
    public static final String BQ_OUT_TABLE = "BQ_OUT_TABLE";
    public static final String BQ_GCS_TEMP = "BQ_GCS_TEMP";

    protected void fail(String message) {
        throw new RuntimeException(message);
    }

    protected String env(String key) {
        String value = System.getenv(key);
        if ((value == null) || (value.length() == 0)) {
            fail("Environment variable [" + key + "] is set.");
        }
        return value;
    }

    private PipelineOptions createDirect() {
        System.setProperty("java.util.logging.SimpleFormatter.format","%5$s%n");
        DirectOptions flinkOptions = PipelineOptionsFactory.as(DirectOptions.class);

        return flinkOptions;
    }

    private PipelineOptions createDataflow() {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setStreaming(true);
        options.setProject(env(GOOGLE_PROJECT_ID));
        options.setRunner(DataflowRunner.class);
        return options;
    }

    protected Pipeline createPipeline() {
        return Pipeline.create(createDirect());
    }

    protected DoFn<String, TableRow> stringToTableRowFn() {
        return new DoFn<String, TableRow>() {

            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                TableRow row = new TableRow();
                row.set("out", c.element().toString());
                c.output(row);
                LOG.info(row.toString());
            }
        };
    }

    protected DoFn<Long, String> longToStringFn() {
        return new DoFn<Long, String>() {

            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                c.output(c.element().toString());
            }
        };
    }

    protected BigQueryIO.Write<TableRow> bigqueryIO() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("out").setType("STRING").setMode("NULLABLE"));
        TableSchema schema = new TableSchema().setFields(fields);

        ValueProvider<String> gcs = new ValueProvider<String>() {
            @Override
            public String get() {
                return env(BQ_GCS_TEMP);
            }

            @Override
            public boolean isAccessible() {
                return true;
            }
        };

        return BigQueryIO
                .writeTableRows()
                .to(env(BQ_OUT_TABLE))
                .withSchema(schema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCustomGcsTempLocation(gcs);
    }

}
