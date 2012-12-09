package com.datastax.driver.core.utils.querybuilder;

import java.nio.ByteBuffer;

import com.datastax.driver.core.Statement;

/**
 * A built BATCH statement.
 */
public class Batch extends Statement {

    private final ByteBuffer routingKey;
    private final Statement[] statements;
    private Using[] usings;

    private StringBuilder builder;

    Batch(Statement[] statements) {
        if (statements.length == 0)
            throw new IllegalArgumentException("Cannot build a BATCH without any statement");

        this.statements = statements;
        ByteBuffer rk = null;
        for (Statement statement : statements) {
            rk = statement.getRoutingKey();
            if (rk != null)
                break;
        }
        this.routingKey = rk;
    }

    public String getQueryString() {
        if (builder != null)
            return builder.toString();

        builder = new StringBuilder();
        builder.append("BEGIN BATCH");

        if (usings != null && usings.length > 0) {
            builder.append(" USING ");
            Utils.joinAndAppend(null, builder, " AND ", usings);
        }
        builder.append(" ");

        for (Statement statement : statements) {
            String str = statement.getQueryString();
            builder.append(str);
            if (!str.trim().endsWith(";"))
                builder.append(";");
        }
        builder.append("APPLY BATCH;");
        return builder.toString();
    }

    /**
     * Adds a USING clause to this statement.
     *
     * @param usings the options to use.
     * @return this statement.
     *
     * @throws IllegalStateException if a USING clause has already been
     * provided.
     */
    public Batch using(Using... usings) {
        if (this.usings != null)
            throw new IllegalStateException("A USING clause has already been provided");

        this.usings = usings;
        return this;
    }

    public ByteBuffer getRoutingKey() {
        return routingKey;
    }
}
