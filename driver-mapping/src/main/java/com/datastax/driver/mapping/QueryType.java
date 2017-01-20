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

import java.util.Collection;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

enum QueryType {

    SAVE {
        @Override
        String makePreparedQueryString(TableMetadata table, EntityMapper<?> mapper, MappingManager manager, Set<PropertyMapper> columns, Collection<Mapper.Option> options) {
            Insert insert = table == null
                    ? insertInto(mapper.keyspace, mapper.table)
                    : insertInto(table);
            for (PropertyMapper col : columns)
                if (!col.isComputed())
                    insert.value(col.columnName, bindMarker());

            Insert.Options usings = insert.using();
            for (Mapper.Option opt : options) {
                opt.checkValidFor(QueryType.SAVE, manager);
                if (opt.isIncludedInQuery())
                    opt.appendTo(usings);
            }
            return insert.toString();
        }

    },

    GET {
        @Override
        String makePreparedQueryString(TableMetadata table, EntityMapper<?> mapper, MappingManager manager, Set<PropertyMapper> columns, Collection<Mapper.Option> options) {
            Select.Selection selection = select();
            for (PropertyMapper col : mapper.allColumns) {
                Select.SelectionOrAlias column = col.isComputed()
                        ? selection.raw(col.columnName)
                        : selection.column(col.columnName);

                if (col.alias == null) {
                    selection = column;
                } else {
                    selection = column.as(col.alias);
                }
            }
            Select select;
            if (table == null) {
                select = selection.from(mapper.keyspace, mapper.table);
            } else {
                select = selection.from(table);
            }
            Select.Where where = select.where();
            for (int i = 0; i < mapper.primaryKeySize(); i++)
                where.and(eq(mapper.getPrimaryKeyColumn(i).columnName, bindMarker()));

            for (Mapper.Option opt : options)
                opt.checkValidFor(QueryType.GET, manager);
            return select.toString();
        }
    },
	
	GET_BY_PARTITION_KEY {
        @Override
        String makePreparedQueryString(TableMetadata table, EntityMapper<?> mapper, MappingManager manager, Set<PropertyMapper> columns, Collection<Mapper.Option> options) {
            Select.Selection selection = select();
            for (PropertyMapper col : mapper.allColumns) {
                Select.SelectionOrAlias column = col.isComputed()
                        ? selection.raw(col.columnName)
                        : selection.column(col.columnName);

                if (col.alias == null) {
                    selection = column;
                } else {
                    selection = column.as(col.alias);
                }
            }
            Select select;
            if (table == null) {
                select = selection.from(mapper.keyspace, mapper.table);
            } else {
                select = selection.from(table);
            }
            Select.Where where = select.where();
			// in where clause, we need to restrict just the partition keys
            for (int i = 0; i < mapper.partitionKeys.size(); i++)
                where.and(eq(mapper.getPrimaryKeyColumn(i).columnName, bindMarker()));

            for (Mapper.Option opt : options)
                opt.checkValidFor(QueryType.GET, manager); // same with get one
            return select.toString();
        }
    },

    DEL {
        @Override
        String makePreparedQueryString(TableMetadata table, EntityMapper<?> mapper, MappingManager manager, Set<PropertyMapper> columns, Collection<Mapper.Option> options) {
            Delete delete = table == null
                    ? delete().all().from(mapper.keyspace, mapper.table)
                    : delete().all().from(table);
            Delete.Where where = delete.where();
            for (int i = 0; i < mapper.primaryKeySize(); i++)
                where.and(eq(mapper.getPrimaryKeyColumn(i).columnName, bindMarker()));
            Delete.Options usings = delete.using();
            for (Mapper.Option opt : options) {
                opt.checkValidFor(QueryType.DEL, manager);
                if (opt.isIncludedInQuery())
                    opt.appendTo(usings);
                    }
            return delete.toString();
        }
    };

    abstract String makePreparedQueryString(TableMetadata table, EntityMapper<?> mapper, MappingManager manager, Set<PropertyMapper> columns, Collection<Mapper.Option> options);

}
