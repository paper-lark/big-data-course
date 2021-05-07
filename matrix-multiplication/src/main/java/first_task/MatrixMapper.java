package first_task;

import models.AppConfiguration;
import models.MatrixSize;
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
    private int firstRowGroupSize = 0;
    private int firstColumnGroupSize = 0;
    private String secondMatrixTag = "";
    private int secondRowGroupSize = 0;
    private int secondColumnGroupSize = 0;
    private int groupCount = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger.info("Mapper created");

        firstMatrixTag = AppConfiguration.getFirstMatrixTag(context.getConfiguration());
        secondMatrixTag = AppConfiguration.getSecondMatrixTag(context.getConfiguration());
        groupCount = AppConfiguration.getGroupCount(context.getConfiguration());
        MatrixSize firstSize = AppConfiguration.getFirstMatrixSize(context.getConfiguration());
        MatrixSize secondSize = AppConfiguration.getSecondMatrixSize(context.getConfiguration());

        firstRowGroupSize = (int) Math.ceil((float) firstSize.m / groupCount);
        firstColumnGroupSize = (int) Math.ceil((float) firstSize.n / groupCount);
        secondRowGroupSize = (int) Math.ceil((float) secondSize.m / groupCount);
        secondColumnGroupSize = (int) Math.ceil((float) secondSize.n / groupCount);
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

                if (tag.equals(firstMatrixTag)) {
                    for (int k = 0; k < groupCount; k++) {
                        context.write(
                                new MatrixMapperKey(i / firstRowGroupSize, j / firstColumnGroupSize, k, j),
                                new MatrixMapperValue(true, i, j, element));
                    }
                } else if (tag.equals(secondMatrixTag)) {
                    for (int k = 0; k < groupCount; k++) {
                        context.write(
                                new MatrixMapperKey(k, i / secondRowGroupSize, j / secondColumnGroupSize, i),
                                new MatrixMapperValue(false, i, j, element));
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
