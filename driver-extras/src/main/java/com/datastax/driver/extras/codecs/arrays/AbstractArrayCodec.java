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
package com.datastax.driver.extras.codecs.arrays;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.lang.reflect.Array;

import static com.datastax.driver.core.ParseUtils.skipSpaces;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Base class for all codecs dealing with Java arrays.
 * This class aims to reduce the amount of code required to create such codecs.
 *
 * @param <T> The Java array type this codec handles
 */
public abstract class AbstractArrayCodec<T> extends TypeCodec<T> {

    /**
     * @param cqlType   The CQL type. Must be a list type.
     * @param javaClass The Java type. Must be an array class.
     */
    public AbstractArrayCodec(DataType.CollectionType cqlType, Class<T> javaClass) {
        super(cqlType, javaClass);
        checkArgument(cqlType.getName() == DataType.Name.LIST, "Expecting CQL list type, got %s", cqlType);
        checkArgument(javaClass.isArray(), "Expecting Java array class, got %s", javaClass);
    }

    @Override
    public String format(T array) throws InvalidTypeException {
        if (array == null)
            return "NULL";
        int length = Array.getLength(array);
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < length; i++) {
            if (i != 0)
                sb.append(",");
            formatElement(sb, array, i);
        }
        sb.append(']');
        return sb.toString();
    }

    @Override
    public T parse(String value) throws InvalidTypeException {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        int idx = skipSpaces(value, 0);
        if (value.charAt(idx++) != '[')
            throw new InvalidTypeException(String.format("cannot parse list value from \"%s\", at character %d expecting '[' but got '%c'", value, idx, value.charAt(idx)));

        idx = skipSpaces(value, idx);

        if (value.charAt(idx) == ']')
            return newInstance(0);

        // first pass: determine array length
        int length = getArrayLength(value, idx);

        // second pass: parse elements
        T array = newInstance(length);
        int i = 0;
        for (; idx < value.length(); i++) {
            int n = skipLiteral(value, idx);
            parseElement(value.substring(idx, n), array, i);
            idx = skipSpaces(value, n);
            if (value.charAt(idx) == ']')
                return array;
            idx = skipComma(value, idx);
            idx = skipSpaces(value, idx);
        }

        throw new InvalidTypeException(String.format("Malformed list value \"%s\", missing closing ']'", value));
    }

    /**
     * Create a new array instance with the given size.
     *
     * @param size The size of the array to instantiate.
     * @return a new array instance with the given size.
     */
    protected abstract T newInstance(int size);

    /**
     * Format the {@code index}th element of {@code array} to {@code output}.
     *
     * @param output The StringBuilder to write to.
     * @param array  The array to read from.
     * @param index  The element index.
     */
    protected abstract void formatElement(StringBuilder output, T array, int index);

    /**
     * Parse the {@code index}th element of {@code array} from {@code input}.
     *
     * @param input The String to read from.
     * @param array The array to write to.
     * @param index The element index.
     */
    protected abstract void parseElement(String input, T array, int index);

    private int getArrayLength(String value, int idx) {
        int length = 1;
        for (; idx < value.length(); length++) {
            idx = skipLiteral(value, idx);
            idx = skipSpaces(value, idx);
            if (value.charAt(idx) == ']')
                break;
            idx = skipComma(value, idx);
            idx = skipSpaces(value, idx);
        }
        return length;
    }

    private int skipComma(String value, int idx) {
        if (value.charAt(idx) != ',')
            throw new InvalidTypeException(String.format("Cannot parse list value from \"%s\", at character %d expecting ',' but got '%c'", value, idx, value.charAt(idx)));
        return idx + 1;
    }

    private int skipLiteral(String value, int idx) {
        try {
            return ParseUtils.skipCQLValue(value, idx);
        } catch (IllegalArgumentException e) {
            throw new InvalidTypeException(String.format("Cannot parse list value from \"%s\", invalid CQL value at character %d", value, idx), e);
        }
    }

}
