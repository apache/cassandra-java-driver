package com.datastax.driver.core.utils.querybuilder;

public class Ordering extends Utils.Appendeable {

    private final String name;
    private final boolean isDesc;

    private Ordering(String name, boolean isDesc) {
        this.name = name;
        this.isDesc = isDesc;
    }

    public static Ordering asc(String columnName) {
        return new Ordering(columnName, false);
    }

    public static Ordering desc(String columnName) {
        return new Ordering(columnName, true);
    }

    void appendTo(StringBuilder sb) {
        Utils.appendName(name, sb);
        sb.append(isDesc ? " DESC" : " ASC");
    }

    String name() { return null; }
    String firstValue() { return null; }
}
