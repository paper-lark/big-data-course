package second_task;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class AggregateMapper extends Mapper<LongWritable, Text, AggregateKey, DoubleWritable> {
    private static final Logger logger = Logger.getLogger(AggregateMapper.class);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer lines = new StringTokenizer(value.toString(), "\n");

        while (lines.hasMoreTokens()) {
            String line = lines.nextToken();
            if (line.isEmpty()) {
                continue;
            }
            List<String> values = Arrays.asList(line.split("\t"));
            if (values.size() != 3) {
                throw new IllegalArgumentException(String.format("Line in intermediate file has %d values, expected 4. Skipping…", values.size()));
            }
            try {
                int i = Integer.parseInt(values.get(0));
                int j = Integer.parseInt(values.get(1));
                double element = Double.parseDouble(values.get(2));

                context.write(new AggregateKey(i, j), new DoubleWritable(element));
            } catch (NumberFormatException exc) {
                throw new IllegalArgumentException("Failed to parse line in intermediate file. Skipping…", exc);
            }
        }
    }
}
