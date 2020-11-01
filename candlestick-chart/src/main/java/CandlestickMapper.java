import models.CandlestickDescription;
import models.CandlestickKey;
import models.InputFileHeader;
import models.InputRecord;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
import java.util.TimeZone;
import java.util.regex.Pattern;

public class CandlestickMapper extends Mapper<Object, Text, CandlestickKey, CandlestickDescription> {
    private static final Logger logger = Logger.getLogger(CandlestickMapper.class);

    private final DateFormat dateTimeFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    private final DateFormat printFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");
    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private final DateFormat timeFormat = new SimpleDateFormat("HHmm");

    private InputFileHeader header;
    private long binSize; // in milliseconds
    private Pattern symbolRegex;
    private Date fromDate;
    private Date toDate;
    private Date fromTime;
    private Date toTime;

    CandlestickMapper() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        printFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            binSize = context.getConfiguration().getLong("candle.width", 300000);
            symbolRegex = Pattern.compile(context.getConfiguration().getStrings("candle.securities", ".*")[0], 0);
            fromDate = dateFormat.parse(context.getConfiguration().getStrings("candle.date.from", "19000101")[0]);
            toDate = dateFormat.parse(context.getConfiguration().getStrings("candle.date.to", "20200101")[0]);
            fromTime = timeFormat.parse(context.getConfiguration().getStrings("candle.time.from", "1000")[0]);
            toTime = timeFormat.parse(context.getConfiguration().getStrings("candle.time.to", "1800")[0]);
            logger.info(String.format("Time filter: [%s, %s)", printFormat.format(fromTime), printFormat.format(toTime)));
        } catch (ParseException exc) {
            throw new IllegalArgumentException("Failed to configuration options", exc);
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer lines = new StringTokenizer(value.toString(), "\n");
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

        while (lines.hasMoreTokens()) {
            String line = lines.nextToken();
            if (line.isEmpty()) {
                continue;
            }
            List<String> values = Arrays.asList(line.split(","));
            if (header == null) {
                // Header row
                header = new InputFileHeader(filename, values);
                logger.info(String.format("Header columns in file=%s: symbol at %d, price at %d, moment at %d", filename, header.symbolIdx, header.priceIdx, header.momentIdx));
            } else {
                // Data row
                try {
                    InputRecord record = new InputRecord(
                            values.get(header.symbolIdx),
                            dateTimeFormat.parse(values.get(header.momentIdx)),
                            Float.parseFloat(values.get(header.priceIdx)),
                            Long.parseLong(values.get(header.dealIDIdx))
                    );

                    Date date = DateUtils.truncate(record.ts, Calendar.DATE);
                    Date timeSinceDayStart = new Date(record.ts.getTime() - date.getTime());
                    if (
                        !date.before(fromDate) && date.before(toDate) &&
                        !timeSinceDayStart.before(fromTime) &&
                        timeSinceDayStart.before(toTime) &&
                        symbolRegex.matcher(record.symbol).matches()
                    ) {
                        long binIdx = (record.ts.getTime() - date.getTime()) / binSize;
                        Date bin = new Date(date.getTime() + binIdx * binSize);
                        context.write(
                                new CandlestickKey(bin, record.symbol),
                                new CandlestickDescription(
                                        record.ts,
                                        record.dealID,
                                        record.dealPrice,
                                        record.ts,
                                        record.dealID,
                                        record.dealPrice,
                                        record.dealPrice,
                                        record.dealPrice
                                )
                        );
                    } else {
                        logger.debug(String.format("Skipping record at %s", printFormat.format(record.ts)));
                    }
                } catch (ParseException exc) {
                    throw new IllegalArgumentException("Failed to parse row", exc);
                }
            }
        }

    }
}