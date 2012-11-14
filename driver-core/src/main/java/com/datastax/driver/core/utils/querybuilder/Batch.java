package com.datastax.driver.core.utils.querybuilder;

import java.nio.ByteBuffer;

import com.datastax.driver.core.CQLStatement;

public class Batch extends CQLStatement {

    private final ByteBuffer routingKey;
    private final CQLStatement[] statements;
    private Using[] usings;

    private StringBuilder builder;

    Batch(CQLStatement[] statements) {
        if (statements.length == 0)
            throw new IllegalArgumentException("Cannot build a BATCH without any statement");

        this.statements = statements;
        ByteBuffer rk = null;
        for (int i = 0; i < statements.length; i++) {
            rk = statements[i].getRoutingKey();
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

        for (int i = 0; i < statements.length; i++) {
            String str = statements[i].getQueryString();
            builder.append(str);
            if (!str.trim().endsWith(";"))
                builder.append(";");
        }
        builder.append("APPLY BATCH;");
        return builder.toString();
    }

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
