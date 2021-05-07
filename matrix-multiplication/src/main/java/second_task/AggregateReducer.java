package second_task;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class AggregateReducer extends Reducer<AggregateKey, DoubleWritable, AggregateKey, DoubleWritable> {
    private static final Logger logger = Logger.getLogger(AggregateReducer.class);

    @Override
    public void reduce(AggregateKey key, Iterable<DoubleWritable> values, AggregateReducer.Context context) throws IOException, InterruptedException {
        double result = 0;
        for (DoubleWritable v: values) {
            result += v.get();
        }
        context.write(key, new DoubleWritable(result));
    }
}
