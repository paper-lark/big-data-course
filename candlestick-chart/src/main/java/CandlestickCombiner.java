import models.CandlestickDescription;
import models.CandlestickKey;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class CandlestickCombiner extends Reducer<CandlestickKey, CandlestickDescription, CandlestickKey, CandlestickDescription> {
    private static final Logger logger = Logger.getLogger(CandlestickCombiner.class);

    CandlestickCombiner() {
        logger.info("Combiner created");
    }

    public void reduce(CandlestickKey key, Iterable<CandlestickDescription> values, CandlestickReducer.Context context) throws IOException, InterruptedException {
        CandlestickDescription result = null;
        for (CandlestickDescription value: values) {
            result = result == null ? new CandlestickDescription(value) : result.combine(value);
        }
        if (result != null) {
            context.write(key, result);
        }
    }
}
