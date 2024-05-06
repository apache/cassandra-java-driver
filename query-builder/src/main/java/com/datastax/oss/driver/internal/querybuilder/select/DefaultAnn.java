package com.datastax.oss.driver.internal.querybuilder.select;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.querybuilder.select.Ann;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.Map;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class DefaultAnn implements Ann {
    private final CqlIdentifier cqlIdentifier;
    private final CqlVector<Number> vector;

    public DefaultAnn(@NonNull CqlIdentifier cqlIdentifier, @NonNull CqlVector<Number> vector) {
        this.cqlIdentifier = cqlIdentifier;
        this.vector = vector;
    }

    @NonNull
    @Override
    public String asCql() {
        StringBuilder builder = new StringBuilder();
        builder.append("ORDER BY ");
        builder.append(this.cqlIdentifier.asCql(true));
        builder.append(" ANN OF ");
        literal(vector).appendTo(builder);
        return builder.toString();
    }

    @NonNull
    @Override
    public SimpleStatement build() {
        return Ann.super.build();
    }

    @NonNull
    @Override
    public SimpleStatement build(@NonNull Object... values) {
        return Ann.super.build(values);
    }

    @NonNull
    @Override
    public SimpleStatement build(@NonNull Map<String, Object> namedValues) {
        return Ann.super.build(namedValues);
    }

    @NonNull
    @Override
    public SimpleStatementBuilder builder() {
        return Ann.super.builder();
    }

}
