import java.util.Date;

public class OperationRecord {
    public final String symbol;
    public final Date ts;
    public final Float dealPrice;

    public OperationRecord(String symbol, Date ts, Float dealPrice) {
        this.symbol = symbol;
        this.ts = ts;
        this.dealPrice = dealPrice;
    }
}
