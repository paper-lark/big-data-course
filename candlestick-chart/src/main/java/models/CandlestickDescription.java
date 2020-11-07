package models;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CandlestickDescription implements Writable {
    private static final DateFormat printFormat = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");

    final private LongWritable openTs;
    final private LongWritable openDealID;
    final private LongWritable closeTs;
    final private LongWritable closeDealID;
    final private FloatWritable open;
    final private FloatWritable close;
    final private FloatWritable high;
    final private FloatWritable low;

    public CandlestickDescription() {
        this.openTs = new LongWritable();
        this.closeTs = new LongWritable();
        this.openDealID = new LongWritable();
        this.closeDealID = new LongWritable();
        this.open = new FloatWritable();
        this.close = new FloatWritable();
        this.high = new FloatWritable();
        this.low = new FloatWritable();
    }

    public CandlestickDescription(CandlestickDescription other) {
        this.openTs = new LongWritable(other.openTs.get());
        this.closeTs = new LongWritable(other.closeTs.get());
        this.openDealID = new LongWritable(other.openDealID.get());
        this.closeDealID = new LongWritable(other.closeDealID.get());
        this.open = new FloatWritable(other.open.get());
        this.close = new FloatWritable(other.close.get());
        this.high = new FloatWritable(other.high.get());
        this.low = new FloatWritable(other.low.get());
    }

    public CandlestickDescription(Date openTs, long openDealID, float open, Date closeTs,  long closeDealID, float close, float high, float low) {
        this.openTs = new LongWritable(openTs.getTime());
        this.closeTs = new LongWritable(closeTs.getTime());
        this.openDealID = new LongWritable(openDealID);
        this.closeDealID = new LongWritable(closeDealID);
        this.open = new FloatWritable(open);
        this.close = new FloatWritable(close);
        this.high = new FloatWritable(high);
        this.low = new FloatWritable(low);
    }

    public CandlestickDescription combine(CandlestickDescription o) {
        // select open
        float resultOpen = open.get();
        long resultOpenTs = openTs.get();
        long resultOpenDealID = openDealID.get();
        if (o.openTs.get() < openTs.get() || o.openTs.get() == openTs.get() && o.openDealID.get() < openDealID.get()) {
            resultOpen = o.open.get();
            resultOpenTs = o.openTs.get();
            resultOpenDealID = o.openDealID.get();
        }

        // select close
        float resultClose = close.get();
        long resultCloseTs = closeTs.get();
        long resultCloseDealID = closeDealID.get();
        if (o.closeTs.get() > closeTs.get() || o.closeTs.get() == closeTs.get() && o.closeDealID.get() > closeDealID.get()) {
            resultClose = o.close.get();
            resultCloseTs = o.closeTs.get();
            resultCloseDealID = o.closeDealID.get();
        }

        return new CandlestickDescription(
                new Date(resultOpenTs),
                resultOpenDealID,
                resultOpen,
                new Date(resultCloseTs),
                resultCloseDealID,
                resultClose,
                Math.max(high.get(), o.high.get()),
                Math.min(low.get(), o.low.get())
        );
    }

    @Override
    public String toString() {
        return "CandlestickDescription{" +
                "openTs=" + printFormat.format(new Date(openTs.get())) +
                ", openDealID=" + openDealID.get() +
                ", closeTs=" + printFormat.format(new Date(closeTs.get())) +
                ", closeDealID=" + closeDealID.get() +
                ", open=" + open.get() +
                ", close=" + close.get() +
                ", high=" + high.get() +
                ", low=" + low.get() +
                '}';
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.openTs.write(dataOutput);
        this.closeTs.write(dataOutput);
        this.openDealID.write(dataOutput);
        this.closeDealID.write(dataOutput);
        this.open.write(dataOutput);
        this.close.write(dataOutput);
        this.high.write(dataOutput);
        this.low.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.openTs.readFields(dataInput);
        this.closeTs.readFields(dataInput);
        this.openDealID.readFields(dataInput);
        this.closeDealID.readFields(dataInput);
        this.open.readFields(dataInput);
        this.close.readFields(dataInput);
        this.high.readFields(dataInput);
        this.low.readFields(dataInput);
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
