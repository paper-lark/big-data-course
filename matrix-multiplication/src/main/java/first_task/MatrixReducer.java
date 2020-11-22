package first_task;

import models.AppConfiguration;
import models.MatrixSize;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

public class MatrixReducer extends Reducer<MatrixMapperKey, MatrixMapperValue, MatrixReducerKey, DoubleWritable> {
    private static final Logger logger = Logger.getLogger(MatrixReducer.class);

    private double[][] x;
    private double[] a;
    private double[] b;
    private int rowSize;
    private int columnSize;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        int groupCount = AppConfiguration.getGroupCount(context.getConfiguration());
        MatrixSize firstSize = AppConfiguration.getFirstMatrixSize(context.getConfiguration());
        MatrixSize secondSize = AppConfiguration.getSecondMatrixSize(context.getConfiguration());
        rowSize = (int) Math.ceil((float) firstSize.m / groupCount);
        columnSize = (int) Math.ceil((float) secondSize.n / groupCount);
        x = new double[rowSize][columnSize];
        a = new double[columnSize];
        b = new double[rowSize];
    }

    @Override
    public void reduce(MatrixMapperKey key, Iterable<MatrixMapperValue> values, MatrixReducer.Context context) throws IOException, InterruptedException {
        clearXElements();
        clearArrays();
        int currentJ = -1;

        for (MatrixMapperValue v: values) {
            if (key.getJ() != currentJ && currentJ != -1) {
                updateXElements();
            }
            currentJ = key.getJ();
            if (v.isFirstMatrix()) {
                a[v.getI() % rowSize] = v.getValue();
            } else {
                b[v.getJ() % columnSize] = v.getValue();
            }
        }
        if (currentJ != -1) {
            updateXElements();
        }

        for (int i = 0; i < rowSize; i++) {
            for (int k = 0; k < columnSize; k++) {
                if (x[i][k] != 0) {
                    context.write(new MatrixReducerKey(i + rowSize * key.getGroupI(), k + rowSize * key.getGroupK()), new DoubleWritable(x[i][k]));
                }
            }
        }
    }

    private void updateXElements() {
        for (int i = 0; i < rowSize; i++) {
            for (int k = 0; k < columnSize; k++) {
                x[i][k] += a[i] * b[k];
            }
        }
        clearArrays();
    }

    private void clearXElements() {
        for (double[] row : x) {
            Arrays.fill(row, 0);
        }
    }

    private void clearArrays() {
        Arrays.fill(a, 0);
        Arrays.fill(b, 0);
    }
}
