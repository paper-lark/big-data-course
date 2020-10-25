import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TimestampBinPartitioner extends Partitioner<TimestampBinPair, FloatWritable> {
    @Override
    public int getPartition(TimestampBinPair key, FloatWritable value, int numReduceTasks) {
        return (key.getBin().hashCode() % numReduceTasks);
    }
}
