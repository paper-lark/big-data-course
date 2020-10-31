import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Application extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        // configuration
        Configuration conf = this.getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: candlestick [params] <in> <out>");
            System.exit(2);
        }
        Path inputPath = new Path(otherArgs[0]);
        Path outputPath = new Path(otherArgs[1]);

        // input
        Job job = new Job(new JobConf(conf));
        job.setJarByClass(Application.class);
        job.setNumReduceTasks(conf.getInt("candle.num.reducers", 1));
        job.setInputFormatClass(HeaderInputFormat.class);
        HeaderInputFormat.addInputPath(job, inputPath);

        // mapper
        job.setMapperClass(CandlestickMapper.class);
        job.setMapOutputKeyClass(CandlestickKey.class);
        job.setMapOutputValueClass(FloatWritable.class);

        // reducer
        job.setPartitionerClass(CandlestickPartitioner.class);
        job.setGroupingComparatorClass(CandlestickGroupingComparator.class);
        job.setReducerClass(CandlestickReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(CandlestickDescription.class);

        // output
        CandlestickOutputFormat.setOutputPath(job, outputPath);
        LazyOutputFormat.setOutputFormatClass(job, CandlestickOutputFormat.class); // prevents part-00000 files from being created
        MultipleOutputs.addNamedOutput(
                job,
                "main",
                CandlestickOutputFormat.class,
                NullWritable.class,
                CandlestickDescription.class
        );

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Application(), args);
        System.exit(res);
    }
}
