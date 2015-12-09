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
package com.datastax.driver.mapping.annotations;

import com.datastax.driver.core.TypeCodec;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that allows to specify the name of the CQL column to which the
 * field should be mapped.
 * <p/>
 * Note that this annotation is generally optional in the sense that any field
 * of a class annotated by {@link Table} will be mapped by default to a column
 * having the same name than this field unless that field has the
 * {@link Transient} annotation. As such, this annotation is mainly useful when
 * the name to map the field to is not the same one that the field itself (but
 * can be added without it's name parameter for documentation sake).
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    /**
     * Name of the column being mapped in Cassandra. By default, the name of the
     * field will be used.
     *
     * @return the name of the mapped column in Cassandra, or {@code ""} to use
     * the field name.
     */
    String name() default "";

    /**
     * Whether the column name is a case sensitive one.
     *
     * @return whether the column name is a case sensitive one.
     */
    boolean caseSensitive() default false;

    /**
     * A custom codec that will be used to serialize and deserialize the column.
     *
     * @return the codec's class. It must have a no-argument constructor (the mapper
     * will create an instance and cache it).
     */
    Class<? extends TypeCodec<?>> codec() default Defaults.NoCodec.class;
}
