package models;

import lombok.Data;

@Data
public class MatrixSize {
    public final int m;
    public final int n;

    @Override
    public String toString() {
        return "" + m + "x" + n;
    }
}
