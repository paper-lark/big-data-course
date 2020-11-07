import models.CandlestickDescription;
import models.CandlestickKey;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class CandlestickReducer extends Reducer<CandlestickKey, CandlestickDescription, CandlestickKey, CandlestickDescription> {
    private static final Logger logger = Logger.getLogger(CandlestickReducer.class);
    private final DateFormat printFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");
    private MultipleOutputs<CandlestickKey, CandlestickDescription> mos;

    CandlestickReducer() {
        printFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        super.cleanup(context);
    }

    public void reduce(CandlestickKey key, Iterable<CandlestickDescription> values, CandlestickReducer.Context context) throws IOException, InterruptedException {
        logger.debug(String.format("Writing candle for symbol=%s, timestamp=%s", key.getSymbol(), printFormat.format(key.getBin())));

        CandlestickDescription result = null;
        for (CandlestickDescription value: values) {
            result = result == null ? new CandlestickDescription(value) : result.combine(value);
        }

        if (result != null) {
            mos.write("main", key, result, key.getSymbol());
        }
    }
}