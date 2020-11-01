package models;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public class CandlestickKey implements WritableComparable<CandlestickKey> {
    private final LongWritable bin;
    private final Text symbol;

    public CandlestickKey() {
        this.bin = new LongWritable();
        this.symbol = new Text();
    }

    public CandlestickKey(Date bin, String symbol) {
        this.bin = new LongWritable(bin.getTime());
        this.symbol = new Text(symbol);
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
        }
        return result;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bin.get(), symbol.toString());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        symbol.write(dataOutput);
        bin.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        symbol.readFields(dataInput);
        bin.readFields(dataInput);
    }
}
