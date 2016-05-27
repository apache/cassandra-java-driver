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

import com.datastax.driver.core.ConsistencyLevel;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link EntityMapper} implementation that use reflection to read and write fields
 * of an entity.
 */
class ReflectionMapper<T> extends EntityMapper<T> {

    private static final ReflectionFactory factory = new ReflectionFactory();

    private ReflectionMapper(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
        super(entityClass, keyspace, table, writeConsistency, readConsistency);
    }

    public static Factory factory() {
        return factory;
    }

    @Override
    public T newEntity() {
        try {
            return entityClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Can't create an instance of " + entityClass.getName());
        }
    }

    private static class LiteralMapper<T> extends ColumnMapper<T> {

        private LiteralMapper(MappedProperty<T> property, int position, AtomicInteger columnCounter) {
            super(property, position, columnCounter);
        }

        @Override
        Object getValue(T entity) {
            try {
                return property.getter().invoke(entity);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Could not get field '" + property.name() + "'");
            } catch (Exception e) {
                throw new IllegalStateException("Unable to access getter for '" + property.name() + "' in " + entity.getClass().getName(), e);
            }
        }

        @Override
        void setValue(Object entity, Object value) {
            try {
                property.setter().invoke(entity, value);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Could not set field '" + property.name() + "' to value '" + value + "'");
            } catch (Exception e) {
                throw new IllegalStateException("Unable to access setter for '" + property.name() + "' in " + entity.getClass().getName(), e);
            }
        }
    }

    private static class ReflectionFactory implements Factory {

        @Override
        public <T> EntityMapper<T> create(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
            return new ReflectionMapper<T>(entityClass, keyspace, table, writeConsistency, readConsistency);
        }

        @Override
        public <T> ColumnMapper<T> createColumnMapper(MappedProperty<T> property, int position, MappingManager mappingManager, AtomicInteger columnCounter) {
            for (Class<?> udt : TypeMappings.findUDTs(property.type().getType()))
                mappingManager.getUDTCodec(udt);
            return new LiteralMapper<T>(property, position, columnCounter);
        }
    }
}
