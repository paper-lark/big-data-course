import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CandlestickOutputFormat extends FileOutputFormat<LongWritable, CandlestickDescription> {
    @Override
    public RecordWriter<LongWritable, CandlestickDescription> getRecordWriter(TaskAttemptContext job) throws IOException {
        // TODO: write separate file for each symbol
        String file_extension = ".csv";
        Path file = getDefaultWorkFile(job, file_extension);
        FileSystem fs = file.getFileSystem(job.getConfiguration());
        FSDataOutputStream fileOut = fs.create(file, false);
        return new CandlestickRecordWriter(fileOut);
    }
}
