package first_task;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatrixMapperValue implements Writable {
    private final Text matrixTag = new Text();
    private final IntWritable i = new IntWritable();
    private final IntWritable j = new IntWritable();
    private final DoubleWritable value = new DoubleWritable();

    public MatrixMapperValue() {}

    public MatrixMapperValue(String matrixTag, int i, int j, double value) {
        this.matrixTag.set(matrixTag);
        this.i.set(i);
        this.j.set(j);
        this.value.set(value);
    }

    public String getMatrixTag() {
        return matrixTag.toString();
    }

    public int getI() {
        return i.get();
    }

    public int getJ() {
        return j.get();
    }

    public double getValue() {
        return value.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        matrixTag.write(out);
        i.write(out);
        j.write(out);
        value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        matrixTag.readFields(in);
        i.readFields(in);
        j.readFields(in);
        value.readFields(in);
    }
}
