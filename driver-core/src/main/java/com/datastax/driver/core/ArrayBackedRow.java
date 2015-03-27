/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.List;

import com.datastax.driver.core.exceptions.DriverInternalError;

/**
 * Implementation of a Row backed by an ArrayList.
 */
class ArrayBackedRow extends AbstractGettableData implements Row {

    private final Token.Factory tokenFactory;
    private final List<ByteBuffer> data;

    private ArrayBackedRow(ColumnDefinitions metadata, Token.Factory tokenFactory, List<ByteBuffer> data) {
        super(metadata);
        this.tokenFactory = tokenFactory;
        this.data = data;
    }

    static Row fromData(ColumnDefinitions metadata, Token.Factory tokenFactory, List<ByteBuffer> data) {
        if (data == null)
            return null;

        return new ArrayBackedRow(metadata, tokenFactory, data);
    }

    public ColumnDefinitions getColumnDefinitions() {
        return metadata;
    }

    @Override
    protected ByteBuffer getValue(int i) {
        return data.get(i);
    }

    public Token getToken(int i) {
        if (tokenFactory == null)
            throw new DriverInternalError("Token factory not set. This should only happen at initialization time");

        metadata.checkType(i, tokenFactory.getTokenType().getName());

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return tokenFactory.deserialize(value);
    }

    public Token getToken(String name) {
        return getToken(metadata.getFirstIdx(name));
    }

    public Token getPartitionKeyToken() {
        int i = 0;
        for (ColumnDefinitions.Definition column : metadata) {
            if (column.getName().matches("token(.*)"))
                return getToken(i);
            i++;
        }
        throw new IllegalStateException("Found no column named 'token(...)'. If the column is aliased, use getToken(String).");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Row[");
        for (int i = 0; i < metadata.size(); i++) {
            if (i != 0)
                sb.append(", ");
            ByteBuffer bb = getValue(i);
            if (bb == null)
                sb.append("NULL");
            else
                sb.append(metadata.getType(i).codec().deserialize(bb).toString());
        }
        sb.append(']');
        return sb.toString();
    }
}
