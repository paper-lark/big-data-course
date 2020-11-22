package models;

import org.apache.hadoop.conf.Configuration;

public class AppConfiguration {
    public static String getFirstMatrixTag(Configuration configuration) {
        return AppConfiguration.getMatrixTags(configuration).substring(0, 1);
    }

    public static String getSecondMatrixTag(Configuration configuration) {
        return AppConfiguration.getMatrixTags(configuration).substring(1, 2);
    }

    public static String getResultMatrixTag(Configuration configuration) {
        return AppConfiguration.getMatrixTags(configuration).substring(2, 3);
    }

    public static String getValueFormat(Configuration configuration) {
        return configuration.get("mm.float-format", "%.3f");
    }

    public static int getGroupCount(Configuration configuration) {
        int value = configuration.getInt("mm.groups", 0);
        if (value == 0) {
            throw new IllegalArgumentException("mm.groups is not specified");
        }
        return value;
    }

    private static String getMatrixTags(Configuration configuration) {
        String tags = configuration.get("mm.tags", "ABC");
        if (tags.length() != 3) {
            throw new IllegalArgumentException("mm.tags should contain 3 distinct characters");
        }
        return tags;
    }
}
