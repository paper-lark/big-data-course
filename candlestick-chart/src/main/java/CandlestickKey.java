import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class CandlestickKey implements WritableComparable<CandlestickKey> {
    private final LongWritable bin;
    private final LongWritable timestamp;
    private final Text symbol;
    private final LongWritable dealID;

    public CandlestickKey() {
        this.timestamp = new LongWritable();
        this.bin = new LongWritable();
        this.symbol = new Text();
        this.dealID = new LongWritable();
    }

    public CandlestickKey(Date timestamp, Date bin, String symbol, Long dealID) {
        this.timestamp = new LongWritable(timestamp.getTime());
        this.bin = new LongWritable(bin.getTime());
        this.symbol = new Text(symbol);
        this.dealID = new LongWritable(dealID);
    }

    public Date getTimestamp() {
        return new Date(timestamp.get());
    }

    public String getSymbol() {
        return symbol.toString();
    }

    public Date getBin() {
        return new Date(bin.get());
    }

    @Override
    public int compareTo(CandlestickKey other) {
        int result = symbol.compareTo(other.symbol);
        if (result == 0) {
            result = bin.compareTo(other.bin);
            if (result == 0) {
                result = timestamp.compareTo(other.timestamp);
                if (result == 0) {
                    result = dealID.compareTo(other.dealID);
                }
            }
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        symbol.write(dataOutput);
        bin.write(dataOutput);
        timestamp.write(dataOutput);
        dealID.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        symbol.readFields(dataInput);
        bin.readFields(dataInput);
        timestamp.readFields(dataInput);
        dealID.readFields(dataInput);
    }
}
