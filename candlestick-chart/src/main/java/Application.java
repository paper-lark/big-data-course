import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Application extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: candlestick [params] <in> <out>");
            System.exit(2);
        }
        Path inputPath = new Path(otherArgs[0]);
        Path outputPath = new Path(otherArgs[1]);

        Job job = new Job(new JobConf(conf));
        job.setJarByClass(Application.class);
        job.setNumReduceTasks(conf.getInt("candle.num.reducers", 1));
        job.setInputFormatClass(HeaderInputFormat.class);

        job.setMapperClass(CandlestickMapper.class);
        job.setMapOutputKeyClass(CandlestickKey.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setPartitionerClass(CandlestickPartitioner.class);
        job.setGroupingComparatorClass(CandlestickGroupingComparator.class);
        job.setReducerClass(CandlestickReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(CandlestickDescription.class);
        job.setOutputFormatClass(CandlestickOutputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Application(), args);
        System.exit(res);
    }
}
