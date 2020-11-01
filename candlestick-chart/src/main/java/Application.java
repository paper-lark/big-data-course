import models.CandlestickDescription;
import models.CandlestickKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class Application extends Configured implements Tool {
    private final Logger logger = Logger.getLogger(Application.class);

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
        int reduceCount = conf.getInt("candle.num.reducers", 1);
        logger.info(String.format("Using %d reducers", reduceCount));

        // input
        Job job = new Job(conf);
        job.setJarByClass(Application.class);
        job.setNumReduceTasks(reduceCount);
        job.setInputFormatClass(CandlestickInputFormat.class);
        CandlestickInputFormat.addInputPath(job, inputPath);

        // mapper
        job.setMapperClass(CandlestickMapper.class);
        job.setMapOutputKeyClass(CandlestickKey.class);
        job.setMapOutputValueClass(CandlestickDescription.class);

        // reducer
        if (conf.getBoolean("candle.use.combiners", false)) {
            logger.info("Using combiners");
            job.setCombinerClass(CandlestickCombiner.class);
        }
        job.setReducerClass(CandlestickReducer.class);
        job.setOutputKeyClass(CandlestickKey.class);
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
