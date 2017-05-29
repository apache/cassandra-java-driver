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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines to which User Defined Type a class must be mapped to.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface UDT {
    /**
     * The name of the keyspace the type is part of.
     *
     * @return the name of the keyspace.
     */
    String keyspace() default "";

    /**
     * The name of the type.
     *
     * @return the name of the type.
     */
    String name();

    /**
     * Whether the keyspace name is a case sensitive one.
     *
     * @return whether the keyspace name is a case sensitive one.
     */
    boolean caseSensitiveKeyspace() default false;

    /**
     * Whether the type name is a case sensitive one.
     *
     * @return whether the type name is a case sensitive one.
     */
    boolean caseSensitiveType() default false;
}
