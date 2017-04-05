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

import com.datastax.driver.core.DataType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies that the property decorated with this annotation maps to a CQL type that is {@link DataType#isFrozen() frozen},
 * or contains frozen subtypes.
 * <p/>
 * This annotation is purely informational at this stage, the validity of the declaration is not checked.
 * But will become useful when a schema generation feature is added to the mapper. Therefore it is a good idea to keep
 * frozen declarations up-to-date.
 *
 * @see FrozenKey
 * @see FrozenValue
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Frozen {

    // Implementation note: frozen annotations were previously checked at runtime, this code can be found in version 2.1.7

    /**
     * Contains the full CQL type of the target column. As a convenience, this can be left out when only the top-level
     * type is frozen.
     * <p/>
     * Examples:
     * <pre>
     * // Will map to frozen&lt;user&gt;
     * &#64;Frozen
     * private User user;
     *
     * &#64;Frozen("map&lt;text, map&lt;text, frozen&lt;user&gt;&gt;&gt;")
     * private Map&lt;String, Map&lt;String, User&gt;&gt; m;
     * </pre>
     * <p/>
     * Also consider the {@link FrozenKey @FrozenKey} and {@link FrozenValue @FrozenValue} shortcuts for simple collections.
     *
     * @return the full CQL type of the target column.
     */
    String value() default "";
}
