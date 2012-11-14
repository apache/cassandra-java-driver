package com.datastax.driver.core.utils.querybuilder;

import java.net.InetAddress;
import java.util.Date;
import java.util.UUID;

public abstract class Clause extends Utils.Appendeable {

    protected final String name;

    private Clause(String name) {
        this.name = name;
    };

    String name() {
        return name;
    }

    public static Clause eq(String name, Object value) {
        return new SimpleClause(name, "=", value);
    }

    public static Clause in(String name, Object... values) {
        return new InClause(name, values);
    }

    public static Clause lt(String name, Object value) {
        return new SimpleClause(name, "<", value);
    }

    public static Clause lte(String name, Object value) {
        return new SimpleClause(name, "<=", value);
    }

    public static Clause gt(String name, Object value) {
        return new SimpleClause(name, ">", value);
    }

    public static Clause gte(String name, Object value) {
        return new SimpleClause(name, ">=", value);
    }

    private static class SimpleClause extends Clause {

        private final String op;
        private final Object value;

        private SimpleClause(String name, String op, Object value) {
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

    private static class InClause extends Clause {

        private final Object[] values;

        private InClause(String name, Object[] values) {
            super(name);
            this.values = values;

            if (values == null || values.length == 0)
                throw new IllegalArgumentException("Missing values for IN clause");
        }

        void appendTo(StringBuilder sb) {
            Utils.appendName(name, sb).append(" IN (");
            Utils.joinAndAppendValues(sb, ",", values).append(")");
        }

        Object firstValue() {
            return values[0];
        }
    }
}
