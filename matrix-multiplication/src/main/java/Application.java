import first_task.MatrixGroupingComparator;
import first_task.MatrixMapper;
import first_task.MatrixMapperKey;
import first_task.MatrixMapperValue;
import first_task.MatrixPartitioner;
import first_task.MatrixReducer;
import first_task.MatrixReducerKey;
import models.MatrixSize;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Application extends Configured implements Tool {
    private final Logger logger = Logger.getLogger(Application.class);

    public int run(String[] args) throws Exception {
        // configuration
        Configuration conf = this.getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: mm [params] <path_to_a> <path_to_b> <path_to_c>");
            System.exit(2);
        }
        Path firstMatrixPath = new Path(otherArgs[0]);
        Path secondMatrixPath = new Path(otherArgs[1]);
        Path resultMatrixPath = new Path(otherArgs[2]);
        Path intermediateResultPath = getIntermediateResultPath(resultMatrixPath);

        // get input matrix sizes
        MatrixSize firstMatrixSize = readSize(firstMatrixPath);
        MatrixSize secondMatrixSize = readSize(secondMatrixPath);
        if (firstMatrixSize.n != secondMatrixSize.m) {
            throw new Exception(String.format("Cannot multiply matrices of size %s and %s", firstMatrixSize, secondMatrixSize));
        }
        conf.setInt("matrix.first.m", firstMatrixSize.m);
        conf.setInt("matrix.first.n", firstMatrixSize.n);
        conf.setInt("matrix.second.m", secondMatrixSize.m);
        conf.setInt("matrix.second.n", secondMatrixSize.n);

        // write output matrix size
        MatrixSize resultMatrixSize = new MatrixSize(firstMatrixSize.m, secondMatrixSize.n);
        writeSize(resultMatrixPath, resultMatrixSize);

        // first job
        JobConf jobConf = new JobConf(conf);
        jobConf.setJarByClass(Application.class);
        Job firstJob = Job.getInstance(jobConf, "mm-step-1");
        TextInputFormat.addInputPath(firstJob, getMatrixDataPath(firstMatrixPath));
        TextInputFormat.addInputPath(firstJob, getMatrixDataPath(secondMatrixPath));
        FileOutputFormat.setOutputPath(firstJob, intermediateResultPath);
        firstJob.setJarByClass(Application.class);
        firstJob.setInputFormatClass(TextInputFormat.class);
        firstJob.setMapperClass(MatrixMapper.class);
        firstJob.setMapOutputKeyClass(MatrixMapperKey.class);
        firstJob.setMapOutputValueClass(MatrixMapperValue.class);
        firstJob.setPartitionerClass(MatrixPartitioner.class);
        firstJob.setGroupingComparatorClass(MatrixGroupingComparator.class);
        firstJob.setReducerClass(MatrixReducer.class);
        firstJob.setOutputKeyClass(MatrixReducerKey.class);
        firstJob.setOutputValueClass(DoubleWritable.class);

        if (!firstJob.waitForCompletion(true)) {
            logger.error("First step failed");
            return 1;
        }

        // second job
        // TODO:
        Job secondJob = Job.getInstance(jobConf, "mm-step-2");
        TextInputFormat.addInputPath(firstJob, intermediateResultPath);
        FileOutputFormat.setOutputPath(firstJob, getMatrixDataPath(resultMatrixPath));
        //        firstJob.setMapperClass(CandlestickMapper.class);
        //        firstJob.setMapOutputKeyClass(CandlestickKey.class);
        //        firstJob.setMapOutputValueClass(CandlestickDescription.class);
        //        firstJob.setReducerClass(CandlestickReducer.class);
        //        firstJob.setOutputKeyClass(CandlestickKey.class);
        //        firstJob.setOutputValueClass(CandlestickDescription.class);

        boolean isSuccess = secondJob.waitForCompletion(true);
        removeIntermediateResult(intermediateResultPath);
        return isSuccess ? 0 : 1;
    }

    private MatrixSize readSize(Path pathToMatrix) throws Exception {
        Path sizePath = getMatrixSizePath(pathToMatrix);
        FileSystem fs = FileSystem.get(this.getConf());

        FSDataInputStream inputStream = fs.open(sizePath);
        String contents = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        inputStream.close();
        fs.close();
        String[] parts = contents.split("\t");
        if (parts.length != 2) {
            throw new Exception(String.format("Matrix at %s size is incorrect", pathToMatrix));
        }
        int rows = Integer.parseInt(parts[0].trim());
        int columns = Integer.parseInt(parts[1].trim());

        return new MatrixSize(rows, columns);
    }

    private void writeSize(Path pathToMatrix, MatrixSize matrixSize) throws IOException {
        FileSystem fs = FileSystem.get(this.getConf());
        if(!fs.exists(pathToMatrix)) {
            fs.mkdirs(pathToMatrix);
            logger.info("Path " + pathToMatrix + " created.");
        }

        Path sizePath = getMatrixSizePath(pathToMatrix);
        FSDataOutputStream outputStream = fs.create(sizePath);
        outputStream.writeBytes("" + matrixSize.m + "\t" + matrixSize.n + "\n");
        outputStream.close();
        fs.close();
    }

    private void removeIntermediateResult(Path pathToIntermediateResult) throws IOException {
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(pathToIntermediateResult, true);
        fs.close();
    }

    private Path getMatrixDataPath(Path pathToMatrix) {
        String matrixDataPath = "data";
        return new Path(pathToMatrix + File.separator + matrixDataPath);
    }

    private Path getMatrixSizePath(Path pathToMatrix) {
        String matrixSizePath = "size";
        return new Path(pathToMatrix + File.separator + matrixSizePath);
    }

    private Path getIntermediateResultPath(Path pathToResult) {
        String intermediateResultPath = "intermediate";
        return new Path(pathToResult + File.separator + intermediateResultPath);
    }
}
