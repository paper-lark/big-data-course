package second_task;

import models.AppConfiguration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AggregateOutputFormat extends FileOutputFormat<AggregateKey, DoubleWritable> {

    static class Writer extends RecordWriter<AggregateKey, DoubleWritable> {
        protected DataOutputStream out;
        private final String tag;
        private final String valueFormat;
        private final byte[] recordSeparator;
        private final byte[] fieldSeparator;

        public Writer(String tag, String valueFormat, DataOutputStream out, String fieldSeparator, String recordSeparator) {
            this.tag = tag;
            this.out = out;
            this.valueFormat = valueFormat;
            this.fieldSeparator = fieldSeparator.getBytes(StandardCharsets.UTF_8);
            this.recordSeparator = recordSeparator.getBytes(StandardCharsets.UTF_8);
        }

        public Writer(String tag, String valueFormat, DataOutputStream out) {
            this(tag, valueFormat, out, "\t","\n");
        }

        public synchronized void write(AggregateKey key, DoubleWritable value) throws IOException {
            if (value != null && key != null) {
                this.out.write(tag.getBytes(StandardCharsets.UTF_8));
                this.out.write(fieldSeparator);
                this.out.write(Integer.toString(key.getI()).getBytes(StandardCharsets.UTF_8));
                this.out.write(fieldSeparator);
                this.out.write(Integer.toString(key.getJ()).getBytes(StandardCharsets.UTF_8));
                this.out.write(fieldSeparator);
                this.out.write(String.format(valueFormat, value.get()).getBytes(StandardCharsets.UTF_8));
                this.out.write(recordSeparator);
            }
        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            this.out.close();
        }
    }

    @Override
    public RecordWriter<AggregateKey, DoubleWritable> getRecordWriter(TaskAttemptContext context) throws IOException {
        String matrixTag = AppConfiguration.getResultMatrixTag(context.getConfiguration());
        String valueFormat =  AppConfiguration.getValueFormat(context.getConfiguration());

        Path file = getDefaultWorkFile(context, "");
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataOutputStream fileOut = fs.create(file, false);
        return new Writer(matrixTag, valueFormat, fileOut);
    }
}
