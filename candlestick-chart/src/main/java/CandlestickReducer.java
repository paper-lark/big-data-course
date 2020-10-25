import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;

public class CandlestickReducer extends Reducer<TimestampBinPair, FloatWritable,LongWritable, Text> {
    private static final Logger logger = Logger.getLogger(CandlestickMapper.class);

    public void reduce(TimestampBinPair key, Iterable<FloatWritable> prices, CandlestickReducer.Context context) throws IOException, InterruptedException {
        logger.info(String.format("Writing candle for %s", new Date(key.getBin().get()).toString()));
        Float high = null;
        Float low = null;
        Float open = null;
        Float close = null;
        for (FloatWritable price: prices) {
            high = high == null ? price.get() : Math.max(high, price.get());
            low = low == null ? price.get() : Math.min(low, price.get());
            open = open == null ? price.get() : open;
            close = price.get();
        }

        context.write(key.getBin(), new Text(String.format("open=%f, high=%f, low=%f, close=%f", open, high, low, close)));
    }
}