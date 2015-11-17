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
 * {@link com.datastax.driver.core.TypeCodec} instances useful for
 * serializing between CQL temporal types and Java primitive types.
 * <p>
 * The codecs in this class provide the following mappings:
 *
 * <table summary="Supported Mappings">
 *     <tr>
 *         <th>Codec</th>
 *         <th>CQL type</th>
 *         <th>Java type</th>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.date.SimpleTimestampCodec}</td>
 *         <td>{@link com.datastax.driver.core.DataType#timestamp() timestamp}</td>
 *         <td>{@code long} (representing milliseconds since the Epoch)</td>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.date.SimpleDateCodec}</td>
 *         <td>{@link com.datastax.driver.core.DataType#date() date}</td>
 *         <td>{@code int} (representing days since the Epoch)</td>
 *     </tr>
 * </table>
 */
package com.datastax.driver.extras.codecs.date;
