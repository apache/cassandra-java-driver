/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping;

import com.datastax.driver.core.DataType;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.querybuilder.QueryBuilder.quote;

abstract class ColumnMapper<T> {

    public enum Kind {PARTITION_KEY, CLUSTERING_COLUMN, REGULAR, COMPUTED}

    ;

    private final String columnName;
    private final String alias;
    protected final String fieldName;
    protected final Class<?> javaType;
    // Note: dataType is not guaranteed to be exact. Typically, it will be uuid even if the underlying
    // type is timeuuid. Currently, this is not a problem, but we might allow some @Timeuuid annotation
    // for the sake of validation (similarly, we'll always have text, never ascii).
    protected final DataType dataType;
    protected final Kind kind;
    protected final int position;

    protected ColumnMapper(Field field, DataType dataType, int position, AtomicInteger columnCounter) {
        this.columnName = AnnotationParser.columnName(field);
        this.alias = (columnCounter != null)
                ? AnnotationParser.newAlias(field, columnCounter.incrementAndGet())
                : null;
        this.fieldName = field.getName();
        this.javaType = field.getType();
        this.dataType = dataType;
        this.kind = AnnotationParser.kind(field);
        this.position = position;
    }

    public abstract Object getValue(T entity);

    public abstract void setValue(T entity, Object value);

    public String getColumnName() {
        return kind == Kind.COMPUTED
                ? columnName
                : quote(columnName);
    }

    public String getAlias() {
        return alias;
    }

    public DataType getDataType() {
        return dataType;
    }

}
