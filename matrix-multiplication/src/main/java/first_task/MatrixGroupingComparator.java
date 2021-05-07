package first_task;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MatrixGroupingComparator extends WritableComparator {
    protected MatrixGroupingComparator() {
        super(MatrixMapperKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MatrixMapperKey first = (MatrixMapperKey) a;
        MatrixMapperKey second = (MatrixMapperKey) b;

        return first.comparePrimaryKeyTo(second);
    }
}
