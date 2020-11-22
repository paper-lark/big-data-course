package second_task;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AggregateKey implements WritableComparable<AggregateKey> {
    private final IntWritable i = new IntWritable();
    private final IntWritable j = new IntWritable();

    AggregateKey() {}

    AggregateKey(int i, int j) {
        this.i.set(i);
        this.j.set(j);
    }

    public int getI() {
        return i.get();
    }

    public int getJ() {
        return j.get();
    }

    @Override
    public String toString() {
        return "" + i + "\t" + j;
    }

    @Override
    public int compareTo(AggregateKey o) {
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
