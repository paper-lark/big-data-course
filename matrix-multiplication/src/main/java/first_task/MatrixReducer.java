package first_task;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MatrixReducer extends Reducer<MatrixMapperKey, MatrixMapperValue, MatrixReducerKey, DoubleWritable> {
    private static final Logger logger = Logger.getLogger(MatrixReducer.class);
    private final Map<Integer, Double> rowToFirstValue = new HashMap<>();
    private final Map<Integer, Double> columnToSecondValue = new HashMap<>();
    private final Map<Pair<Integer, Integer>, Double> x = new HashMap<>();

    @Override
    public void reduce(MatrixMapperKey key, Iterable<MatrixMapperValue> values, MatrixReducer.Context context) throws IOException, InterruptedException {
        int currentJ = -1;
        rowToFirstValue.clear();
        columnToSecondValue.clear();
        x.clear();

        for (MatrixMapperValue v: values) {
            if (key.getJ() != currentJ) {
                updateXElements();
            }
            currentJ = key.getJ();
            if (v.isFirstMatrix()) {
                rowToFirstValue.put(v.getI(), v.getValue());
            } else {
                columnToSecondValue.put(v.getJ(), v.getValue());
            }
        }
        if (currentJ != -1) {
            updateXElements();
        }

        for (Map.Entry<Pair<Integer, Integer>, Double> kv : x.entrySet()) {
            Pair<Integer, Integer> k = kv.getKey();
            double v = kv.getValue();
            context.write(new MatrixReducerKey(k.getKey(), k.getValue()), new DoubleWritable(v));
        }
    }

    private void updateXElements() {
        for (Map.Entry<Integer, Double> first : rowToFirstValue.entrySet()) {
            for (Map.Entry<Integer, Double> second : columnToSecondValue.entrySet()) {
                double increment = first.getValue() * second.getValue();
                Pair<Integer, Integer> k = new Pair<>(first.getKey(), second.getKey());
                Double current = x.get(k);
                x.put(k, current == null ? increment : current + increment);
            }
        }
        rowToFirstValue.clear();
        columnToSecondValue.clear();
    }
}
