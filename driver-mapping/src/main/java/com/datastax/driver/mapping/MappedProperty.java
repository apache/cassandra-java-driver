/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.mapping;

import com.datastax.driver.core.TypeCodec;
import com.google.common.reflect.TypeToken;

/**
 * A Java property that is mapped to either a table column,
 * a user-defined type (UDT) field, or
 * a CQL expression such as {@code "ttl(col1)"}.
 */
public interface MappedProperty<T> {

    /**
     * Returns this property's name.
     *
     * @return this property's name; may not be {@code null}.
     */
    String getPropertyName();

    /**
     * Returns the name of the table column or
     * UDT field that this property maps to.
     * <p/>
     * Note that case-sensitive identifiers should
     * be quoted with {@link com.datastax.driver.core.Metadata#quote}
     * <p/>
     * In case of a {@link #isComputed() computed} property,
     * this method should return the CQL expression to compute
     * the property value, e.g. {@code "ttl(col1)"}.
     *
     * @return the name of the table column or
     * UDT field that this property maps to, or the CQL expression
     * in case of computed properties; may not be {@code null}.
     */
    String getMappedName();

    /**
     * Returns this property's type.
     *
     * @return this property's type; may not be {@code null}.
     */
    TypeToken<T> getPropertyType();

    /**
     * Returns the {@link TypeCodec codec} to use
     * to serialize and deserialize this property.
     * <p/>
     * If this method returns {@code null}, then a default codec
     * for the property's {@link #getPropertyType() type}
     * will be used.
     *
     * @return {@link TypeCodec codec} to use
     * to serialize and deserialize this property.
     */
    TypeCodec<T> getCustomCodec();

    /**
     * Returns {@code true} if this property is
     * part of the table's partition key,
     * {@code false} otherwise.
     * <p/>
     * This method has no effect if this property
     * is mapped to a UDT field or a CQL expression.
     *
     * @return {@code true} if this property is
     * part of the table's partition key,
     * {@code false} otherwise.
     * @see com.datastax.driver.mapping.annotations.PartitionKey
     */
    boolean isPartitionKey();

    /**
     * Returns {@code true} if this property is
     * a clustering column,
     * {@code false} otherwise.
     * <p/>
     * This method has no effect if this property
     * is mapped to a UDT field or a CQL expression.
     *
     * @return {@code true} if this property is
     * a clustering column,
     * {@code false} otherwise.
     * @see com.datastax.driver.mapping.annotations.ClusteringColumn
     */
    boolean isClusteringColumn();

    /**
     * Returns this property's zero-based position
     * among partition key columns or clustering columns.
     * <p/>
     * For example, assuming the following primary key definition:
     * {@code PRIMARY KEY ((col1, col2), col3, col4)},
     * {@code col1} has position 0 (i.e. first partition key column),
     * {@code col2} has position 1 (i.e. second partition key column),
     * {@code col3} has position 0 (i.e. first clustering key column),
     * {@code col4} has position 1 (i.e. second clustering key column),
     * <p/>
     * This method has no effect if this property
     * is not part of the primary key, or if it is
     * mapped to a UDT field or a CQL expression.
     * Implementors are encouraged to return {@code -1} in these
     * situations.
     *
     * @return this property's zero-based position
     * among partition key columns or clustering columns.
     */
    int getPosition();

    /**
     * Returns {@code true} if this property is computed,
     * i.e. if it represents the result of a CQL expression
     * such as {@code "ttl(col1)"},
     * {@code false} otherwise.
     * <p/>
     * Computed properties are not allowed with protocol v1.
     * <p/>
     * Also note that computed properties are read-only.
     *
     * @return {@code true} if this property is computed,
     * {@code false} otherwise.
     * @see com.datastax.driver.mapping.annotations.Computed
     */
    boolean isComputed();

    /**
     * Reads the current value of this property in the given {@code entity}.
     *
     * @param entity The instance to read the property from; may not be {@code null}.
     * @return The property value.
     * @throws IllegalArgumentException if the property cannot be read.
     */
    T getValue(Object entity);

    /**
     * Writes the given value to this property in the given {@code entity}.
     *
     * @param entity The instance to write the property to; may not be {@code null}.
     * @param value  The property value.
     * @throws IllegalArgumentException if the property cannot be written.
     */
    void setValue(Object entity, T value);

}
