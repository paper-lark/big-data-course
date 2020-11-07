package models;

import java.util.ArrayList;
import java.util.List;

public class InputFileHeader {
    public final int symbolIdx;
    public final int priceIdx;
    public final int momentIdx;
    public final int dealIDIdx;

    public InputFileHeader(String filename, List<String> header) throws IllegalArgumentException {
        symbolIdx = header.indexOf("#SYMBOL");
        priceIdx = header.indexOf("PRICE_DEAL");
        momentIdx = header.indexOf("MOMENT");
        dealIDIdx = header.indexOf("ID_DEAL");

        ArrayList<String> missingHeaders = new ArrayList<String>();
        if (symbolIdx == -1) {
            missingHeaders.add("#SYMBOL");
        }
        if (priceIdx == -1) {
            missingHeaders.add("PRICE_DEAL");
        }
        if (momentIdx == -1) {
            missingHeaders.add("MOMENT");
        }
        if (dealIDIdx == -1) {
            missingHeaders.add("ID_DEAL");
        }
        if (!missingHeaders.isEmpty()) {
            throw new IllegalArgumentException(String.format("File=%s does not contain required columns: %s", filename, missingHeaders));
        }
    }
}
