package first_task;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatrixReducerKey implements WritableComparable<MatrixReducerKey> {
    private final IntWritable i = new IntWritable();
    private final IntWritable j = new IntWritable();

    MatrixReducerKey() {}

    MatrixReducerKey(int i, int j) {
        this.i.set(i);
        this.j.set(j);
    }

    public int getI() {
        return this.i.get();
    }

    public int getJ() {
        return this.j.get();
    }

    @Override
    public String toString() {
        return String.format("%d\t%d", i.get(), j.get());
    }

    @Override
    public int compareTo(MatrixReducerKey o) {
        int result = i.compareTo(o.i);
        if (result == 0) {
            result = j.compareTo(o.j);
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        i.write(out);
        j.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        i.readFields(in);
        j.readFields(in);
    }
}
