import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CandlestickGroupingComparator extends WritableComparator {
    public CandlestickGroupingComparator() {
        super(CandlestickKey.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        CandlestickKey pair = (CandlestickKey) wc1;
        CandlestickKey pair2 = (CandlestickKey) wc2;

        int result = pair.getBin().compareTo(pair2.getBin());
        if (result == 0) {
            result = pair.getSymbol().compareTo(pair2.getSymbol());
        }
        return result;
    }
}
