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

import com.datastax.driver.mapping.EnumType;

/**
 * Defines that the annotated field (that must be a Java Enum) must be
 * persisted as an enumerated type.
 * <p>
 * The optional {@link EnumType} value defined how the enumeration must be
 * persisted.
 */
@Target(value = {ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface Enumerated {
    /**
     * How the enumeration must be persisted.
     *
     * @return the {@link EnumType} to use for mapping this enumeration.
     */
    EnumType value() default EnumType.STRING;
}
