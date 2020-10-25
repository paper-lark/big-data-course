import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Application {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: candlestick <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "candlestick-formatter");
        job.setJarByClass(Application.class);

        job.setMapperClass(CandlestickMapper.class);
        job.setMapOutputKeyClass(TimestampBinPair.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setPartitionerClass(TimestampBinPartitioner.class);
        job.setGroupingComparatorClass(TimestampBinGroupingComparator.class);
        job.setReducerClass(CandlestickReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
