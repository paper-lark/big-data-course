import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class CandlestickRecordWriter extends RecordWriter<LongWritable, CandlestickDescription> {
    protected DataOutputStream out;
    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    private final byte[] recordSeparator;
    private final byte[] fieldSeparator;

    public CandlestickRecordWriter(DataOutputStream out, String fieldSeparator, String recordSeparator) {
        this.out = out;
        this.fieldSeparator = fieldSeparator.getBytes(StandardCharsets.UTF_8);
        this.recordSeparator = recordSeparator.getBytes(StandardCharsets.UTF_8);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public CandlestickRecordWriter(DataOutputStream out) {
        this(out, ",","\n");
    }

    public synchronized void write(LongWritable key, CandlestickDescription value) throws IOException {
        if (value != null) {
            this.out.write(value.getSymbol().getBytes(StandardCharsets.UTF_8));
            this.out.write(fieldSeparator);
            this.out.write(dateFormat.format(value.getTimestamp()).getBytes(StandardCharsets.UTF_8));
            this.out.write(fieldSeparator);
            this.out.write(String.format("%.1f", value.getOpen()).getBytes(StandardCharsets.UTF_8));
            this.out.write(fieldSeparator);
            this.out.write(String.format("%.1f", value.getHigh()).getBytes(StandardCharsets.UTF_8));
            this.out.write(fieldSeparator);
            this.out.write(String.format("%.1f", value.getLow()).getBytes(StandardCharsets.UTF_8));
            this.out.write(fieldSeparator);
            this.out.write(String.format("%.1f", value.getClose()).getBytes(StandardCharsets.UTF_8));
            this.out.write(recordSeparator);//write custom record separator instead of NEW_LINE
        }
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.out.close();
    }
}
