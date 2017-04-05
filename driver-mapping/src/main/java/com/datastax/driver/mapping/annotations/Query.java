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
 * Defines the CQL query that an {@link Accessor} method must implement.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Query {
    /**
     * The CQL query to use.
     * <p/>
     * In that query string, the parameter of the annotated method can be referenced using
     * name bind markers. For instance, the first parameter can be refered by {@code :arg0},
     * the second one by {@code :arg1}, ... Alternatively, if a parameter of the annonated
     * method has a {@link Param} annotation, the value of that latter annoation should be
     * used instead.
     *
     * @return the CQL query to use.
     */
    String value();
}
