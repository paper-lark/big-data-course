import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class TimestampBinPair implements Writable, WritableComparable<TimestampBinPair> {
    private final LongWritable bin;
    private final LongWritable timestamp;

    public TimestampBinPair() {
        this.bin = new LongWritable();
        this.timestamp = new LongWritable();
    }


    public TimestampBinPair(long bin, Date ts) {
        this.bin = new LongWritable(bin);
        this.timestamp = new LongWritable(ts.getTime());
    }

    public LongWritable getBin() {
        return bin;
    }

    public LongWritable getTimestamp() {
        return timestamp;
    }

    public int compareTo(TimestampBinPair other) {
        int value = this.bin.compareTo(other.bin);
        if (value == 0) {
            value = this.timestamp.compareTo(other.timestamp);
        }
        return value;
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.bin.write(dataOutput);
        this.timestamp.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.bin.readFields(dataInput);
        this.timestamp.readFields(dataInput);
    }
}
