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
        String makePreparedQueryString(TableMetadata table, EntityMapper<?> mapper, MappingManager manager, Set<AliasedMappedProperty> columns, Collection<Mapper.Option> options) {
            Insert insert = table == null
                    ? insertInto(mapper.keyspace, mapper.table)
                    : insertInto(table);
            for (AliasedMappedProperty col : columns)
                if (!col.mappedProperty.isComputed())
                    insert.value(col.mappedProperty.getMappedName(), bindMarker());

            for (Mapper.Option opt : options) {
                opt.validate(QueryType.SAVE, manager);
                opt.modifyQueryString(insert);
            }
            return insert.toString();
        }

    },

    GET {
        @Override
        String makePreparedQueryString(TableMetadata table, EntityMapper<?> mapper, MappingManager manager, Set<AliasedMappedProperty> columns, Collection<Mapper.Option> options) {
            Select.Selection selection = select();
            for (AliasedMappedProperty col : mapper.allColumns) {
                Select.SelectionOrAlias column = col.mappedProperty.isComputed()
                        ? selection.raw(col.mappedProperty.getMappedName())
                        : selection.column(col.mappedProperty.getMappedName());

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
                where.and(eq(mapper.getPrimaryKeyColumn(i).mappedProperty.getMappedName(), bindMarker()));

            for (Mapper.Option option : options) {
                option.validate(QueryType.GET, manager);
                option.modifyQueryString(select);
            }
            return select.toString();
        }
    },

    DEL {
        @Override
        String makePreparedQueryString(TableMetadata table, EntityMapper<?> mapper, MappingManager manager, Set<AliasedMappedProperty> columns, Collection<Mapper.Option> options) {
            Delete delete = table == null
                    ? delete().all().from(mapper.keyspace, mapper.table)
                    : delete().all().from(table);
            Delete.Where where = delete.where();
            for (int i = 0; i < mapper.primaryKeySize(); i++)
                where.and(eq(mapper.getPrimaryKeyColumn(i).mappedProperty.getMappedName(), bindMarker()));
            for (Mapper.Option option : options) {
                option.validate(QueryType.DEL, manager);
                option.modifyQueryString(delete);
            }
            return delete.toString();
        }
    };

    abstract String makePreparedQueryString(TableMetadata table, EntityMapper<?> mapper, MappingManager manager, Set<AliasedMappedProperty> columns, Collection<Mapper.Option> options);

}
