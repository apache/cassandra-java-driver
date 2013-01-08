package com.datastax.driver.core.utils.querybuilder;

public class Ordering extends Utils.Appendeable {

    private final String name;
    private final boolean isDesc;

    Ordering(String name, boolean isDesc) {
        this.name = name;
        this.isDesc = isDesc;
    }

    void appendTo(StringBuilder sb) {
        Utils.appendName(name, sb);
        sb.append(isDesc ? " DESC" : " ASC");
    }
}
