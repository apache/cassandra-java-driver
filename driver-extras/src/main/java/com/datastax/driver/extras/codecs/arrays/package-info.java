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
 * serializing between CQL lists and Java arrays.
 * <p>
 * The codecs in this package provide the following mappings:
 *
 * <table summary="Supported Mappings">
 *     <tr>
 *         <th>Codec</th>
 *         <th>CQL type</th>
 *         <th>Java type</th>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.arrays.ObjectArrayCodec}</td>
 *         <td>{@code list<?>}</td>
 *         <td>{@code T[]}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.arrays.IntArrayCodec}</td>
 *         <td>{@code list<int>}</td>
 *         <td>{@code int[]}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.arrays.LongArrayCodec}</td>
 *         <td>{@code list<long>}</td>
 *         <td>{@code long[]}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.arrays.FloatArrayCodec}</td>
 *         <td>{@code list<float>}</td>
 *         <td>{@code float[]}</td>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.arrays.DoubleArrayCodec}</td>
 *         <td>{@code list<double>}</td>
 *         <td>{@code double[]}</td>
 *     </tr>
 * </table>
 */
package com.datastax.driver.extras.codecs.arrays;
