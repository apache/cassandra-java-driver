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

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TypeCodec;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

abstract class ColumnMapper<T> {

    public enum Kind {PARTITION_KEY, CLUSTERING_COLUMN, REGULAR, COMPUTED}

    ;

    private final String columnName;
    private final String alias;
    protected final String fieldName;
    /**
     * The type of the Java field in the mapped class
     */
    protected final TypeToken<Object> fieldType;
    protected final Kind kind;
    protected final int position;
    protected final TypeCodec<Object> customCodec;

    @SuppressWarnings("unchecked")
    protected ColumnMapper(Field field, int position, AtomicInteger columnCounter) {
        this.columnName = AnnotationParser.columnName(field);
        this.alias = (columnCounter != null)
                ? AnnotationParser.newAlias(field, columnCounter.incrementAndGet())
                : null;
        this.fieldName = field.getName();
        this.fieldType = (TypeToken<Object>) TypeToken.of(field.getGenericType());
        this.kind = AnnotationParser.kind(field);
        this.position = position;
        this.customCodec = AnnotationParser.customCodec(field);
    }

    public abstract Object getValue(T entity);

    public abstract void setValue(T entity, Object value);

    public String getColumnName(boolean quoted) {
        return kind == Kind.COMPUTED || !quoted
                ? columnName
                : Metadata.quote(columnName);
    }
    
    public String getColumnName() {
        return getColumnName(true);
    }

    public String getAlias() {
        return alias;
    }

    public TypeCodec<Object> getCustomCodec() {
        return customCodec;
    }

    /**
     * The Java type of this column.
     */
    public TypeToken<Object> getJavaType() {
        return fieldType;
    }
}
