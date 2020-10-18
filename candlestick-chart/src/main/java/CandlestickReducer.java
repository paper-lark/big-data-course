import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CandlestickReducer extends Reducer<LongWritable, FloatWritable,LongWritable, Text> {
    public void reduce(LongWritable key, Iterable<FloatWritable> prices, CandlestickReducer.Context context) throws IOException, InterruptedException {
        Float high = null;
        Float low = null;
        Float open = null;
        Float close = null;
        for (FloatWritable price: prices) {
            high = high == null ? price.get() : Math.max(high, price.get());
            low = low == null ? price.get() : Math.min(low, price.get());
            open = open == null ? price.get() : open;
            close = price.get();
            // TODO: sort by moment too so that open and close are correct
        }

        context.write(key, new Text(String.format("open=%f, high=%f, low=%f, close=%f", open, high, low, close)));
    }
}