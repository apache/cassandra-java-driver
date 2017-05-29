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

import com.datastax.driver.core.*;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.*;

/**
 * A result set whose rows are mapped to an entity class.
 */
public class Result<T> implements PagingIterable<Result<T>, T> {

    private final ResultSet rs;
    private final EntityMapper<T> mapper;
    private final boolean useAlias;

    Result(ResultSet rs, EntityMapper<T> mapper, boolean useAlias) {
        this.rs = rs;
        this.mapper = mapper;
        this.useAlias = useAlias;
    }

    private T map(Row row) {
        T entity = mapper.newEntity();
        for (AliasedMappedProperty c : mapper.allColumns) {
            @SuppressWarnings("unchecked")
            AliasedMappedProperty col = c;
            String name = col.alias != null && this.useAlias ? col.alias : col.mappedProperty.getMappedName();
            if (!row.getColumnDefinitions().contains(name))
                continue;

            Object value;
            TypeCodec<Object> customCodec = col.mappedProperty.getCustomCodec();
            if (customCodec != null)
                value = row.get(name, customCodec);
            else
                value = row.get(name, col.mappedProperty.getPropertyType());

            if (shouldSetValue(value)) {
                col.mappedProperty.setValue(entity, value);
            }
        }
        return entity;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    private static boolean shouldSetValue(Object value) {
        if (value == null)
            return false;
        if (value instanceof Collection)
            return !((Collection<?>) value).isEmpty();
        if (value instanceof Map)
            return !((Map<?, ?>) value).isEmpty();
        return true;
    }

    @Override
    public boolean isExhausted() {
        return rs.isExhausted();
    }

    @Override
    public T one() {
        Row row = rs.one();
        return row == null ? null : map(row);
    }

    @Override
    public List<T> all() {
        List<Row> rows = rs.all();
        List<T> entities = new ArrayList<T>(rows.size());
        for (Row row : rows) {
            entities.add(map(row));
        }
        return entities;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            private final Iterator<Row> rowIterator = rs.iterator();

            @Override
            public boolean hasNext() {
                return rowIterator.hasNext();
            }

            @Override
            public T next() {
                return map(rowIterator.next());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public ExecutionInfo getExecutionInfo() {
        return rs.getExecutionInfo();
    }

    @Override
    public List<ExecutionInfo> getAllExecutionInfo() {
        return rs.getAllExecutionInfo();
    }

    @Override
    public ListenableFuture<Result<T>> fetchMoreResults() {
        return Futures.transform(rs.fetchMoreResults(), new Function<ResultSet, Result<T>>() {
            @Override
            public Result<T> apply(ResultSet rs) {
                return Result.this;
            }
        });
    }

    @Override
    public boolean isFullyFetched() {
        return rs.isFullyFetched();
    }

    @Override
    public int getAvailableWithoutFetching() {
        return rs.getAvailableWithoutFetching();
    }

}
