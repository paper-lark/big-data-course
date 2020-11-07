package models;

public class Size {
    public final int m;
    public final int n;

    @Override
    public String toString() {
        return "" + m + "x" + n;
    }

    public Size(int m, int n) {
        this.m = m;
        this.n = n;
    }
}
