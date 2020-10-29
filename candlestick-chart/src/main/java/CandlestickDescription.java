import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class CandlestickDescription implements WritableComparable<CandlestickDescription>  {
    final private LongWritable ts;
    final private Text symbol;
    final private FloatWritable open;
    final private FloatWritable close;
    final private FloatWritable high;
    final private FloatWritable low;

    public CandlestickDescription(Date ts, String symbol, float open, float close, float high, float low) {
        this.ts = new LongWritable(ts.getTime());
        this.open = new FloatWritable(open);
        this.symbol = new Text(symbol);
        this.close = new FloatWritable(close);
        this.high = new FloatWritable(high);
        this.low = new FloatWritable(low);
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.ts.write(dataOutput);
        this.open.write(dataOutput);
        this.symbol.write(dataOutput);
        this.close.write(dataOutput);
        this.high.write(dataOutput);
        this.low.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.ts.readFields(dataInput);
        this.open.readFields(dataInput);
        this.symbol.readFields(dataInput);
        this.close.readFields(dataInput);
        this.high.readFields(dataInput);
        this.low.readFields(dataInput);
    }

    public int compareTo(CandlestickDescription other) {
        return this.ts.compareTo(other.ts);
    }

    public Date getTimestamp() {
        return new Date(ts.get());
    }

    public String getSymbol() {
        return symbol.toString();
    }

    public float getOpen() {
        return open.get();
    }

    public float getClose() {
        return close.get();
    }

    public float getHigh() {
        return high.get();
    }

    public float getLow() {
        return low.get();
    }
}
