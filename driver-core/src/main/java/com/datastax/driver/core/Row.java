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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * A CQL Row returned in a {@link ResultSet}.
 * <p/>
 * The values of a CQL Row can be retrieved by either index (index starts at zero)
 * or name. When getting them by name, names follow the case insensitivity
 * rules explained in {@link ColumnDefinitions}.
 */
public interface Row extends GettableData {

    /**
     * Returns the columns contained in this Row.
     *
     * @return the columns contained in this Row.
     */
    public ColumnDefinitions getColumnDefinitions();

    /**
     * Returns the {@code i}th value of this row as a {@link Token}.
     * <p/>
     * {@link #getPartitionKeyToken()} should generally be preferred to this method (unless the
     * token column is aliased).
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as an Token.
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException      if column {@code i} is not of the type of token values
     *                                   for this cluster (this depends on the configured partitioner).
     */
    public Token getToken(int i);

    /**
     * Returns the value of column {@code name} as a {@link Token}.
     * <p/>
     * {@link #getPartitionKeyToken()} should generally be preferred to this method (unless the
     * token column is aliased).
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a Token.
     * @throws IllegalArgumentException if {@code name} is not part of the
     *                                  ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException     if column {@code name} is not of the type of token values
     *                                  for this cluster (this depends on the configured partitioner).
     */
    public Token getToken(String name);

    /**
     * Returns the value of the first column containing a {@link Token}.
     * <p/>
     * This method is a shorthand for queries returning a single token in an unaliased
     * column. It will look for the first name matching {@code token(...)}:
     * <pre>
     * {@code
     * ResultSet rs = session.execute("SELECT token(k) FROM my_table WHERE k = 1");
     * Token token = rs.one().getPartitionKeyToken(); // retrieves token(k)
     * }
     * </pre>
     * If that doesn't work for you (for example, if you're using an alias), use
     * {@link #getToken(int)} or {@link #getToken(String)}.
     *
     * @return the value of column {@code name} as a Token.
     * @throws IllegalStateException if no column named {@code token(...)} exists in this
     *                               ResultSet.
     * @throws InvalidTypeException  if the first column named {@code token(...)} is not of
     *                               the type of token values for this cluster (this depends on the configured partitioner).
     */
    public Token getPartitionKeyToken();

}
