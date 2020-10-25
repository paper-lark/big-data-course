import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class CandlestickMapper extends Mapper<Object, Text, TimestampBinPair, FloatWritable> {
    private static final Logger logger = Logger.getLogger(CandlestickMapper.class);
    private Header header = null;
    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMddhhmmssSSS");
    private static final long binSize = 10000; // in milliseconds

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
            if (header == null) {
                // Header row
                header = new Header(values);
                logger.info(String.format("Header columns: symbol in %d, price in %d, moment in %d", header.symbolIdx, header.priceIdx, header.momentIdx));
            } else {
                // Data row
                try {
                    OperationRecord record = new OperationRecord(
                            values.get(header.symbolIdx),
                            dateFormat.parse(values.get(header.momentIdx)),
                            Float.parseFloat(values.get(header.priceIdx))
                    );

                    // TODO: apply filters
                    long bin = record.ts.getTime() / binSize * binSize;
                    context.write(new TimestampBinPair(bin, record.ts), new FloatWritable(record.dealPrice));
                } catch (ParseException exc) {
                    throw new IllegalArgumentException("Failed to parse row", exc);
                }
            }
        }

    }
}