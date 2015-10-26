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
 * serializing JSON structures.
 * <p>
 * The codecs in this class provide support for the following mappings
 * and frameworks:
 *
 * <table summary="Supported Mappings">
 *     <tr>
 *         <th>Codec</th>
 *         <th>CQL type</th>
 *         <th>Java type</th>
 *         <th>JSON Framework</th>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.json.JacksonJsonCodec}</td>
 *         <td>{@link com.datastax.driver.core.DataType#varchar() varchar}</td>
 *         <td>Any</td>
 *         <td><a href="http://wiki.fasterxml.com/JacksonHome">Jackson</a></td>
 *     </tr>
 *     <tr>
 *         <td>{@link com.datastax.driver.extras.codecs.json.Jsr353JsonCodec}</td>
 *         <td>{@link com.datastax.driver.core.DataType#varchar() varchar}</td>
 *         <td>{@link javax.json.JsonStructure JsonStructure}</td>
 *         <td><a href="https://jcp.org/en/jsr/detail?id=353">Java API for JSON processing</a> (JSR 353)</td>
 *     </tr>
 * </table>
 */
package com.datastax.driver.extras.codecs.json;
