package com.datastax.driver.core.querybuilder;

public class Using extends Utils.Appendeable {

    private final String optionName;
    private final long value;

    Using(String optionName, long value) {
        this.optionName = optionName;
        this.value = value;
    }

    void appendTo(StringBuilder sb) {
        sb.append(optionName).append(" ").append(value);
    }
}
