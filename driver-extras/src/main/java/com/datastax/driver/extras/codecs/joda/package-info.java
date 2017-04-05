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
/**
 * This package contains a collection of convenience
 * {@link com.datastax.driver.core.TypeCodec TypeCodec} instances useful for
 * serializing between CQL types and Joda Time types such as {@link org.joda.time.DateTime}.
 * <p/>
 * <p/>
 * Note that classes in this package require the presence of
 * <a href="http://www.joda.org/joda-time/">Joda Time library</a> at runtime.
 * If you use Maven, this can be done by declaring the following dependency in your project:
 * <p/>
 * <pre>{@code
 * <dependency>
 *   <groupId>joda-time</groupId>
 *   <artifactId>joda-time</artifactId>
 *   <version>2.9.1</version>
 * </dependency>
 * }</pre>
 */
package com.datastax.driver.extras.codecs.joda;
