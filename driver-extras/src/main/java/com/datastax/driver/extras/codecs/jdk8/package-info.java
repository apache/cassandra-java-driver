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

/**
 * This package contains a collection of convenience
 * {@link com.datastax.driver.core.TypeCodec TypeCodec} instances useful for
 * serializing between CQL types and JDK 8 types, such as the ones from the
 * {@code java.time} API and {@link java.util.Optional}.
 * <p>
 * The codecs in this class provide the following mappings:
 *
 * <table summary="Supported Mappings">
 *     <tr>
 *         <th>Codec</th>
 *         <th>CQL type</th>
 *         <th>JDK 8 type</th>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.jdk8.OptionalCodec}</td>
 *         <td>Any</td>
 *         <td>{@link java.util.Optional Optional}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec}</td>
 *         <td>{@link com.datastax.driver.core.DataType#time() time}</td>
 *         <td>{@link java.time.LocalTime LocalTime}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.jdk8.LocalDateCodec}</td>
 *         <td>{@link com.datastax.driver.core.DataType#date() date}</td>
 *         <td>{@link java.time.LocalDate LocalDate}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.jdk8.InstantCodec}</td>
 *         <td>{@link com.datastax.driver.core.DataType#timestamp() timestamp}</td>
 *         <td>{@link java.time.Instant Instant}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.jdk8.ZonedDateTimeCodec}</td>
 *         <td>{@link com.datastax.driver.core.TupleType tuple} of
 *         ({@link com.datastax.driver.core.DataType#timestamp() timestamp},
 *         {@link com.datastax.driver.core.DataType#varchar() varchar})</td>
 *         <td>{@link java.time.ZonedDateTime ZonedDateTime}</td>
 *     </tr>
 * </table>
 *
 */
package com.datastax.driver.extras.codecs.jdk8;
