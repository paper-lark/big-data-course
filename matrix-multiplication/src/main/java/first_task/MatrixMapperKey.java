package first_task;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatrixMapperKey implements WritableComparable<MatrixMapperKey> {
    private final IntWritable groupI = new IntWritable();
    private final IntWritable groupJ = new IntWritable();
    private final IntWritable groupK = new IntWritable();
    private final IntWritable j = new IntWritable();

    MatrixMapperKey() {}

    MatrixMapperKey(int groupI, int groupJ, int groupK, int j) {
        this.groupI.set(groupI);
        this.groupJ.set(groupJ);
        this.groupK.set(groupK);
        this.j.set(j);
    }

    public int primaryKeyHashCode() {
        return new Integer(groupI.get() + groupJ.get() + groupK.get()).hashCode();
    }

    public int comparePrimaryKeyTo(MatrixMapperKey other) {
        int result = groupI.compareTo(other.groupI);
        if (result == 0) {
            result = groupJ.compareTo(other.groupJ);
            if (result == 0) {
                result = groupK.compareTo(other.groupK);
            }
        }
        return result;
    }

    @Override
    public int compareTo(MatrixMapperKey other) {
        int result = comparePrimaryKeyTo(other);
        if (result == 0) {
            result = j.compareTo(other.j);
        }

        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        groupI.write(out);
        groupJ.write(out);
        groupK.write(out);
        j.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        groupI.readFields(in);
        groupJ.readFields(in);
        groupK.readFields(in);
        j.readFields(in);
    }
}
