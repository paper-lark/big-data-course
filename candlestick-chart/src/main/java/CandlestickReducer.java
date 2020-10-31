import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class CandlestickReducer extends Reducer<CandlestickKey, FloatWritable, NullWritable, CandlestickDescription> {
    private static final Logger logger = Logger.getLogger(CandlestickReducer.class);
    private final DateFormat printFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");

    CandlestickReducer() {
        printFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public void reduce(CandlestickKey key, Iterable<FloatWritable> prices, CandlestickReducer.Context context) throws IOException, InterruptedException {
        logger.debug(String.format("Writing candle for symbol=%s, timestamp=%s", key.getSymbol(), printFormat.format(key.getBin())));

        float high = -1;
        float low = -1;
        float open = -1;
        float close = -1;
        for (FloatWritable price: prices) {
            high = high == -1 ? price.get() : Math.max(high, price.get());
            low = low == -1 ? price.get() : Math.min(low, price.get());
            open = open == -1 ? price.get() : open;
            close = price.get();
        }

        context.write(null, new CandlestickDescription(key.getBin(), key.getSymbol(), open, close, high, low));
    }
}