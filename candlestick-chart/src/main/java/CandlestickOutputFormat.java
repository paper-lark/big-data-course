import models.CandlestickDescription;
import models.CandlestickKey;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CandlestickOutputFormat extends FileOutputFormat<CandlestickKey, CandlestickDescription> {
    @Override
    public RecordWriter<CandlestickKey, CandlestickDescription> getRecordWriter(TaskAttemptContext job) throws IOException {
        String ext = ".csv";
        Path file = getDefaultWorkFile(job, ext);
        FileSystem fs = file.getFileSystem(job.getConfiguration());
        FSDataOutputStream fileOut = fs.create(file, false);
        return new CandlestickRecordWriter(fileOut);
    }
}
