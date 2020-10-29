import java.util.Date;

public class InputOperationRecord {
    public final String symbol;
    public final Date ts;
    public final Float dealPrice;
    public final Long dealID;

    public InputOperationRecord(String symbol, Date ts, Float dealPrice, Long dealID) {
        this.symbol = symbol;
        this.ts = ts;
        this.dealPrice = dealPrice;
        this.dealID = dealID;
    }
}
