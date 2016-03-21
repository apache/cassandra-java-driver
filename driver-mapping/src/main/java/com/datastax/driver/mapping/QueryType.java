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

import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.Mapper.Option.IfNotExists;
import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

class QueryType {

    private enum Kind {SAVE, GET, DEL, SLICE, REVERSED_SLICE}

    private final Kind kind;

    // For slices
    private final int startBoundSize;
    private final boolean startInclusive;
    private final int endBoundSize;
    private final boolean endInclusive;

    public static final QueryType SAVE = new QueryType(Kind.SAVE);
    public static final QueryType DEL = new QueryType(Kind.DEL);
    public static final QueryType GET = new QueryType(Kind.GET);

    private QueryType(Kind kind) {
        this(kind, 0, false, 0, false);
    }

    private QueryType(Kind kind, int startBoundSize, boolean startInclusive, int endBoundSize, boolean endInclusive) {
        this.kind = kind;
        this.startBoundSize = startBoundSize;
        this.startInclusive = startInclusive;
        this.endBoundSize = endBoundSize;
        this.endInclusive = endInclusive;
    }

    String makePreparedQueryString(TableMetadata table, EntityMapper<?> mapper, MappingManager manager, Set<ColumnMapper<?>> columns, Collection<Mapper.Option> options) {
        switch (kind) {
            case SAVE: {
                Insert insert = table == null
                        ? insertInto(mapper.getKeyspace(), mapper.getTable())
                        : insertInto(table);
                for (ColumnMapper<?> cm : columns)
                    if (cm.kind != ColumnMapper.Kind.COMPUTED)
                        insert.value(cm.getColumnName(), bindMarker());

                Insert.Options usings = insert.using();
                for (Mapper.Option opt : options) {
                    opt.checkValidFor(QueryType.SAVE, manager);

                    if (shouldIfNotExists(opt)) {
                        insert.ifNotExists();
                    }

                    if (opt.isIncludedInQuery())
                        opt.appendTo(usings);
                }
                return insert.toString();
            }
            case GET: {
                Select.Selection selection = select();
                for (ColumnMapper cm : mapper.allColumns()) {
                    Select.SelectionOrAlias column = (cm.kind == ColumnMapper.Kind.COMPUTED)
                            ? ((Select.SelectionOrAlias) selection).raw(cm.getColumnName())
                            : selection.column(cm.getColumnName());

                    if (cm.getAlias() == null) {
                        selection = column;
                    } else {
                        selection = column.as(cm.getAlias());
                    }
                }
                Select select;
                if (table == null) {
                    select = selection.from(mapper.getKeyspace(), mapper.getTable());
                } else {
                    select = selection.from(table);
                }
                Select.Where where = select.where();
                for (int i = 0; i < mapper.primaryKeySize(); i++)
                    where.and(eq(mapper.getPrimaryKeyColumn(i).getColumnName(), bindMarker()));

                for (Mapper.Option opt : options)
                    opt.checkValidFor(QueryType.GET, manager);
                return select.toString();
            }
            case DEL: {
                Delete delete = table == null
                        ? delete().all().from(mapper.getKeyspace(), mapper.getTable())
                        : delete().all().from(table);
                Delete.Where where = delete.where();
                for (int i = 0; i < mapper.primaryKeySize(); i++)
                    where.and(eq(mapper.getPrimaryKeyColumn(i).getColumnName(), bindMarker()));
                Delete.Options usings = delete.using();
                for (Mapper.Option opt : options) {
                    opt.checkValidFor(QueryType.DEL, manager);
                    if (opt.isIncludedInQuery())
                        opt.appendTo(usings);
                }
                return delete.toString();
            }
            case SLICE:
            case REVERSED_SLICE: {
                Select select = table == null
                        ? select().all().from(mapper.getKeyspace(), mapper.getTable())
                        : select().all().from(table);
                Select.Where where = select.where();
                for (int i = 0; i < mapper.partitionKeys.size(); i++)
                    where.and(eq(mapper.partitionKeys.get(i).getColumnName(), bindMarker()));

                if (startBoundSize > 0) {
                    if (startBoundSize == 1) {
                        String name = mapper.clusteringColumns.get(0).getColumnName();
                        where.and(startInclusive ? gte(name, bindMarker()) : gt(name, bindMarker()));
                    } else {
                        List<String> names = new ArrayList<String>(startBoundSize);
                        List<Object> values = new ArrayList<Object>(startBoundSize);
                        for (int i = 0; i < startBoundSize; i++) {
                            names.add(mapper.clusteringColumns.get(i).getColumnName());
                            values.add(bindMarker());
                        }
                        where.and(startInclusive ? gte(names, values) : gt(names, values));
                    }
                }

                if (endBoundSize > 0) {
                    if (endBoundSize == 1) {
                        String name = mapper.clusteringColumns.get(0).getColumnName();
                        where.and(endInclusive ? gte(name, bindMarker()) : gt(name, bindMarker()));
                    } else {
                        List<String> names = new ArrayList<String>(endBoundSize);
                        List<Object> values = new ArrayList<Object>(endBoundSize);
                        for (int i = 0; i < endBoundSize; i++) {
                            names.add(mapper.clusteringColumns.get(i).getColumnName());
                            values.add(bindMarker());
                        }
                        where.and(endInclusive ? lte(names, values) : lt(names, values));
                    }
                }

                select = select.limit(bindMarker());

                if (kind == Kind.REVERSED_SLICE)
                    select = select.orderBy(desc(mapper.clusteringColumns.get(0).getColumnName()));

                return select.toString();
            }
        }
        throw new AssertionError();
    }
    
    private static boolean shouldIfNotExists(Mapper.Option option) {
        if (option instanceof IfNotExists) {
            return ((IfNotExists) option).ifNotExists;
        }

        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || this.getClass() != obj.getClass())
            return false;

        QueryType that = (QueryType) obj;
        return kind == that.kind
                && startBoundSize == that.startBoundSize
                && startInclusive == that.startInclusive
                && endBoundSize == that.endBoundSize
                && endInclusive == that.endInclusive;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(kind, startBoundSize, startInclusive, endBoundSize, endInclusive);
    }

}
