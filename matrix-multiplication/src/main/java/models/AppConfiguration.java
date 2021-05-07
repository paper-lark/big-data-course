package models;

import org.apache.hadoop.conf.Configuration;

public class AppConfiguration {
    public static String getFirstMatrixTag(Configuration configuration) {
        return AppConfiguration.getMatrixTags(configuration).substring(0, 1);
    }

    public static String getSecondMatrixTag(Configuration configuration) {
        return AppConfiguration.getMatrixTags(configuration).substring(1, 2);
    }

    public static MatrixSize getFirstMatrixSize(Configuration configuration) {
        int m = configuration.getInt("matrix.first.m", 0);
        int n = configuration.getInt("matrix.first.n", 0);
        if (m == 0 || n == 0) {
            throw new IllegalArgumentException("One of first matrix dimensions is zero");
        }
        return new MatrixSize(m, n);
    }

    public static MatrixSize getSecondMatrixSize(Configuration configuration) {
        int m = configuration.getInt("matrix.second.m", 0);
        int n = configuration.getInt("matrix.second.n", 0);
        if (m == 0 || n == 0) {
            throw new IllegalArgumentException("One of second matrix dimensions is zero");
        }
        return new MatrixSize(m, n);
    }

    public static String getResultMatrixTag(Configuration configuration) {
        return AppConfiguration.getMatrixTags(configuration).substring(2, 3);
    }

    public static String getValueFormat(Configuration configuration) {
        return configuration.get("mm.float-format", "%.3f");
    }

    public static int getGroupCount(Configuration configuration) {
        return configuration.getInt("mm.groups", 1);
    }

    private static String getMatrixTags(Configuration configuration) {
        String tags = configuration.get("mm.tags", "ABC");
        if (tags.length() != 3) {
            throw new IllegalArgumentException("mm.tags should contain 3 distinct characters");
        }
        return tags;
    }
}
