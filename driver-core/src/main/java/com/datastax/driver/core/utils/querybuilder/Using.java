package com.datastax.driver.core.utils.querybuilder;

public class Using extends Utils.Appendeable {

    private final String optionName;
    private final long value;

    private Using(String optionName, long value) {
        this.optionName = optionName;
        this.value = value;
    }

    public static Using timestamp(long timestamp) {
        if (timestamp < 0)
            throw new IllegalArgumentException("Invalid timestamp, must be positive");

        return new Using("TIMESTAMP", timestamp);
    }

    public static Using ttl(int ttl) {
        if (ttl < 0)
            throw new IllegalArgumentException("Invalid ttl, must be positive");

        return new Using("TTL", ttl);
    }

    void appendTo(StringBuilder sb) {
        sb.append(optionName).append(" ").append(value);
    }

    String name() { return null; }
    String firstValue() { return null; }
}
