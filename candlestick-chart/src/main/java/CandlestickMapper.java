import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class CandlestickMapper extends Mapper<Object, Text, CandlestickKey, FloatWritable> {
    private static final Logger logger = Logger.getLogger(CandlestickMapper.class);
    private static final DateFormat dateTimeFormat = new SimpleDateFormat("yyyyMMddhhmmssSSS");
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private static final DateFormat timeFormat = new SimpleDateFormat("hhmm");

    private InputFileHeader header;
    private long binSize = 300000; // in milliseconds
    private Pattern symbolRegex;
    private Date fromDate;
    private Date toDate;
    private Date fromTime;
    private Date toTime;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            binSize = context.getConfiguration().getLong("candle.width", binSize);
            symbolRegex = Pattern.compile(context.getConfiguration().getStrings("candle.securities", ".*")[0], 0);
            fromDate = dateFormat.parse(context.getConfiguration().getStrings("candle.date.from", "19000101")[0]);
            toDate = dateFormat.parse(context.getConfiguration().getStrings("candle.date.to", "19000101")[0]);
            fromTime = timeFormat.parse(context.getConfiguration().getStrings("candle.time.from", "1000")[0]);
            toTime = timeFormat.parse(context.getConfiguration().getStrings("candle.time.to", "1800")[0]);
        } catch (ParseException exc) {
            throw new IllegalArgumentException("Failed to configuration options", exc);
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer lines = new StringTokenizer(value.toString(), "\n");

        while (lines.hasMoreTokens()) {
            List<String> values = Arrays.asList(lines.nextToken().split(","));
            if (header == null) {
                // Header row
                header = new InputFileHeader(values);
                logger.info(String.format("Header columns: symbol in %d, price in %d, moment in %d", header.symbolIdx, header.priceIdx, header.momentIdx));
            } else {
                // Data row
                try {
                    InputOperationRecord record = new InputOperationRecord(
                            values.get(header.symbolIdx),
                            dateTimeFormat.parse(values.get(header.momentIdx)),
                            Float.parseFloat(values.get(header.priceIdx)),
                            Long.parseLong(values.get(header.dealIDIdx))
                    );

                    // FIXME: time filter works wrong
                    Date date = DateUtils.truncate(record.ts, Calendar.DATE);
                    Date timeOfDay = new Date(record.ts.getTime() - date.getTime());
                    if (
                        !date.before(fromDate) && !date.after(toDate) &&
                        !timeOfDay.before(fromTime) && !timeOfDay.after(toTime) &&
                        symbolRegex.matcher(record.symbol).matches()
                    ) {
                        long binIdx = (record.ts.getTime() - date.getTime()) / binSize;
                        Date bin = new Date(date.getTime() + binIdx * binSize);
                        context.write(new CandlestickKey(record.ts, bin, record.symbol, record.dealID), new FloatWritable(record.dealPrice));
                    } else {
                        logger.info(String.format("Skipping record with ts=%s", record.ts));
                    }
                } catch (ParseException exc) {
                    throw new IllegalArgumentException("Failed to parse row", exc);
                }
            }
        }

    }
}