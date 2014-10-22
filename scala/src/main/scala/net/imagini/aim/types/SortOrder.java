package net.imagini.aim.types;

public enum SortOrder {
    ASC(1),
    DESC(-1);
    private int cmp;
    private SortOrder(int cmp) { this.cmp = cmp; }
    public int getComparator() { return cmp; }
}
