import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TimestampBinGroupingComparator extends WritableComparator {
    public TimestampBinGroupingComparator() {
        super(TimestampBinPair.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        TimestampBinPair pair = (TimestampBinPair) wc1;
        TimestampBinPair pair2 = (TimestampBinPair) wc2;
        return pair.getBin().compareTo(pair2.getBin());
    }
}
