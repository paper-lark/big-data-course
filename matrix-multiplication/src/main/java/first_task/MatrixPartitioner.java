package first_task;

import org.apache.hadoop.mapreduce.Partitioner;

public class MatrixPartitioner extends Partitioner<MatrixMapperKey, MatrixMapperValue> {
    @Override
    public int getPartition(MatrixMapperKey key, MatrixMapperValue value, int numPartitions) {
        return Math.abs(key.primaryKeyHashCode() % numPartitions);
    }
}
