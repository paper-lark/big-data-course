package first_task;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;


public class MatrixMapper extends Mapper<LongWritable, Text, MatrixMapperKey, MatrixMapperValue> {
    private static final Logger logger = Logger.getLogger(MatrixMapper.class);

    private String firstMatrixTag = "";
    private int firstMatrixIGroupSize = 0;
    private int firstMatrixJGroupSize = 0;
    private String secondMatrixTag = "";
    private int secondMatrixIGroupSize = 0;
    private int secondMatrixJGroupSize = 0;
    private int groupCount = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger.info("Mapper created");
        String tags = context.getConfiguration().get("mm.tags");
        if (tags.length() != 3) {
            throw new IllegalArgumentException("mm.tags should contain 3 distinct charaters");
        }

        firstMatrixTag = tags.substring(0, 1);
        secondMatrixTag = tags.substring(1,2);
        groupCount = context.getConfiguration().getInt("mm.groups", 0);
        if (groupCount == 0) {
            throw new IllegalArgumentException("mm.groups is not specified");
        }
        int firstM = context.getConfiguration().getInt("matrix.first.m", 0);
        int firstN = context.getConfiguration().getInt("matrix.first.n", 0);
        int secondM = context.getConfiguration().getInt("matrix.second.m", 0);
        int secondN = context.getConfiguration().getInt("matrix.second.n", 0);
        if (firstM == 0 || firstN == 0 || secondM == 0 || secondN == 0) {
            throw new IllegalArgumentException("One of matrix sizes is zero");
        }
        firstMatrixIGroupSize = (int) Math.ceil((float) firstM / groupCount);
        firstMatrixJGroupSize = (int) Math.ceil((float) firstN / groupCount);
        secondMatrixIGroupSize = (int) Math.ceil((float) secondM / groupCount);
        secondMatrixJGroupSize = (int) Math.ceil((float) secondN / groupCount);
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer lines = new StringTokenizer(value.toString(), "\n");
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        int lineNumber = 0;

        while (lines.hasMoreTokens()) {
            String line = lines.nextToken();
            lineNumber += 1;
            if (line.isEmpty()) {
                continue;
            }
            List<String> values = Arrays.asList(line.split("\t"));
            if (values.size() != 4) {
                logger.error(String.format("Line %d in file '%s' has %d values, expected 4. Skipping…", lineNumber, filename, values.size()));
                continue;
            }
            try {
                String tag = values.get(0);
                double element = Double.parseDouble(values.get(3));
                int i = Integer.parseInt(values.get(1));
                int j = Integer.parseInt(values.get(2));
                MatrixMapperValue returnValue = new MatrixMapperValue(tag, i, j, element);

                if (tag.equals(firstMatrixTag)) {
                    for (int k = 0; k < groupCount; k++) {
                        context.write(new MatrixMapperKey(i / firstMatrixIGroupSize, j / firstMatrixJGroupSize, k, j), returnValue);
                    }
                } else if (tag.equals(secondMatrixTag)) {
                    for (int k = 0; k < groupCount; k++) {
                        context.write(new MatrixMapperKey(k, i / secondMatrixIGroupSize, j / secondMatrixJGroupSize, i), returnValue);
                    }
                } else {
                    logger.error(String.format("Line %d in file '%s' has unknown matrix tag: '%s'. Skipping…", lineNumber, filename, tag));
                }
            } catch (NumberFormatException exc) {
                logger.error(String.format("Failed to parse line %d in file '%s'. Skipping…", lineNumber, filename), exc);
            }
        }
    }
}
