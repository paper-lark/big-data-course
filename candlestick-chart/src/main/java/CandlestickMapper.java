import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class CandlestickMapper extends Mapper<Object, Text, LongWritable, FloatWritable> {
    private Header columnIndices = null;
    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMddhhmmssSSS");
    private static final long binSize = 10000;

    private static class Header {
        public final int symbolIdx;
        public final int priceIdx;
        public final int momentIdx;

        public Header(List<String> header) throws IllegalArgumentException {
            this.symbolIdx = header.indexOf("#SYMBOL");
            this.priceIdx = header.indexOf("PRICE_DEAL");
            this.momentIdx = header.indexOf("MOMENT");

            ArrayList<String> missingHeaders = new ArrayList<String>();
            if (this.symbolIdx == -1) {
                missingHeaders.add("#SYMBOL");
            }
            if (this.priceIdx == -1) {
                missingHeaders.add("#PRICE_DEAL");
            }
            if (this.momentIdx == -1) {
                missingHeaders.add("#MOMENT");
            }
            if (!missingHeaders.isEmpty()) {
                throw new IllegalArgumentException(String.format("File does not contain required columns: %s", missingHeaders));
            }
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer lines = new StringTokenizer(value.toString(), "\n");

        while (lines.hasMoreTokens()) {
            List<String> values = Arrays.asList(lines.nextToken().split(","));
            if (columnIndices == null) {
                // Header row
                columnIndices = new Header(values);
            } else {
                // Data row
                try {
                    OperationRecord record = new OperationRecord(
                            values.get(columnIndices.symbolIdx),
                            dateFormat.parse(values.get(columnIndices.momentIdx)),
                            Float.parseFloat(values.get(columnIndices.priceIdx))
                    );

                    // TODO: apply filters
                    long bin = record.ts.getTime() / binSize * binSize;
                    context.write(new LongWritable(bin), new FloatWritable(record.dealPrice));
                } catch (ParseException exc) {
                    throw new IllegalArgumentException("Failed to parse row", exc);
                }
            }
        }

    }
}