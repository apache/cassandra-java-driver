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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.datastax.driver.core.Configuration;

/**
 * Query parameters to use in the (generated) implementation of a method of an {@link Accessor}
 * interface.
 * <p>
 * All the parameters of this annotation are optional, and when not provided default to whatever
 * default the {@code Cluster} instance used underneath are (those set in
 * {@link Configuration#getQueryOptions}).
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface QueryParameters {
    /**
     * The consistency level to use for the operation.
     *
     * @return the consistency level to use for the operation.
     */
    String consistency() default "";

    /**
     * The fetch size to use for paging the result of this operation.
     *
     * @return the fetch size to use for the operation.
     */
    int fetchSize() default -1;

    /**
     * Whether tracing should be enabled for this operation.
     *
     * @return whether tracing should be enabled for this operation.
     */
    boolean tracing() default false;
}
