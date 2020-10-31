import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CandlestickPartitioner extends Partitioner<CandlestickKey, FloatWritable> {
    @Override
    public int getPartition(CandlestickKey key, FloatWritable value, int numReduceTasks) {
        return Math.abs((key.getBin().hashCode() + key.getSymbol().hashCode()) % numReduceTasks);
    }
}
