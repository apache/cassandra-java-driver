package com.datastax.driver.core.querybuilder;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public abstract class Clause extends Utils.Appendeable {

    protected final String name;

    private Clause(String name) {
        this.name = name;
    };

    String name() {
        return name;
    }

    abstract Object firstValue();

    static class SimpleClause extends Clause {

        private final String op;
        private final Object value;

        SimpleClause(String name, String op, Object value) {
            super(name);
            this.op = op;
            this.value = value;
        }

        void appendTo(StringBuilder sb) {
            Utils.appendName(name, sb).append(op);
            Utils.appendValue(value, sb);
        }

        Object firstValue() {
            return value;
        }
    }

    static class InClause extends Clause {

        private final List<Object> values;

        InClause(String name, List<Object> values) {
            super(name);
            this.values = values;

            if (values == null || values.size() == 0)
                throw new IllegalArgumentException("Missing values for IN clause");
        }

        void appendTo(StringBuilder sb) {
            Utils.appendName(name, sb).append(" IN (");
            Utils.joinAndAppendValues(sb, ",", values).append(")");
        }

        Object firstValue() {
            return values.get(0);
        }
    }
}
