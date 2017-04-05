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
 * This package and its subpackages contain several convenience {@link com.datastax.driver.core.TypeCodec TypeCodec}s.
 * <p/>
 * <table summary="Supported Mappings">
 * <tr>
 * <th>Package</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>{@link com.datastax.driver.extras.codecs.arrays}</td>
 * <td>codecs mapping CQL lists to Java arrays.</td>
 * </tr>
 * <tr>
 * <td>{@link com.datastax.driver.extras.codecs.date}</td>
 * <td>codecs mapping CQL temporal types to Java primitive types.</td>
 * </tr>
 * <tr>
 * <td>{@link com.datastax.driver.extras.codecs.enums}</td>
 * <td>codecs mapping CQL types to Java enums.</td>
 * </tr>
 * <tr>
 * <td>{@link com.datastax.driver.extras.codecs.guava}</td>
 * <td>codecs mapping CQL types to Guava-specific Java types.</td>
 * </tr>
 * <tr>
 * <td>{@link com.datastax.driver.extras.codecs.jdk8}</td>
 * <td>codecs mapping CQL types to Java 8 types, including {@code java.time} API and {@code java.util.Optional}.</td>
 * </tr>
 * <tr>
 * <td>{@link com.datastax.driver.extras.codecs.joda}</td>
 * <td>codecs mapping CQL types to Joda Time types.</td>
 * </tr>
 * <tr>
 * <td>{@link com.datastax.driver.extras.codecs.json}</td>
 * <td>codecs mapping CQL varchars to JSON structures.</td>
 * </tr>
 * </table>
 */
package com.datastax.driver.extras.codecs;
