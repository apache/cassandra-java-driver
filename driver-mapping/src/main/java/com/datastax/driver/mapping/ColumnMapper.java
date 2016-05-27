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

import com.datastax.driver.core.TypeCodec;
import com.google.common.reflect.TypeToken;

import java.util.concurrent.atomic.AtomicInteger;

abstract class ColumnMapper<T> {

    final MappedProperty<T> property;
    final String alias;
    final int position;

    @SuppressWarnings("unchecked")
    protected ColumnMapper(MappedProperty<T> property, int position, AtomicInteger columnCounter) {
        this.property = property;
        this.position = position;
        this.alias = (columnCounter != null)
                ? AnnotationParser.newAlias(columnCounter.incrementAndGet())
                : null;
    }

    abstract Object getValue(T entity);

    abstract void setValue(T entity, Object value);

    String getColumnName() {
        return property.columnName();
    }

    String getAlias() {
        return alias;
    }

    public TypeCodec<Object> getCustomCodec() {
        return property.customCodec();
    }

    /**
     * The Java type of this column.
     */
    @SuppressWarnings("unchecked")
    public TypeToken<Object> getJavaType() {
        return (TypeToken<Object>) property.type();
    }

}
