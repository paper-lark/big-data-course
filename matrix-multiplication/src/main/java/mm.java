import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class mm {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Application(), args);
        System.exit(res);
    }
}
