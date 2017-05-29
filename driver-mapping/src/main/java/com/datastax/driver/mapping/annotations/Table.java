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
package com.datastax.driver.mapping.annotations;

import com.datastax.driver.mapping.Mapper;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines to which table a class must be mapped to.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {
    /**
     * The name of the keyspace the table is part of.
     *
     * @return the name of the keyspace.
     */
    String keyspace() default "";

    /**
     * The name of the table.
     *
     * @return the name of the table.
     */
    String name();

    /**
     * Whether the keyspace name is a case sensitive one.
     *
     * @return whether the keyspace name is a case sensitive one.
     */
    boolean caseSensitiveKeyspace() default false;

    /**
     * Whether the table name is a case sensitive one.
     *
     * @return whether the table name is a case sensitive one.
     */
    boolean caseSensitiveTable() default false;

    /**
     * The consistency level to use for the write operations provded by the {@link Mapper} class.
     *
     * @return the consistency level to use for the write operations provded by the {@link Mapper} class.
     */
    String writeConsistency() default "";

    /**
     * The consistency level to use for the read operations provded by the {@link Mapper} class.
     *
     * @return the consistency level to use for the read operations provded by the {@link Mapper} class.
     */
    String readConsistency() default "";
}
