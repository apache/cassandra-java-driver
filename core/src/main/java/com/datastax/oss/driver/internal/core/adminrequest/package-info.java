/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Infrastructure to execute internal requests in the driver, for example control connection
 * queries, or automatic statement preparation.
 *
 * <p>This is a stripped-down version of the public API, with the bare minimum for our needs:
 *
 * <ul>
 *   <li>async mode only.
 *   <li>execution on a given channel, without retries.
 *   <li>{@code QUERY} and {@code PREPARE} messages only.
 *   <li>paging is possible, but only on the same channel. If the channel gets closed between pages,
 *       the query fails.
 *   <li>values can only be bound by name, and it is assumed that the target type can always be
 *       inferred unambiguously (i.e. the only integer type is {@code int}, etc).
 *   <li>limited result API: getters by internal name only, no custom codecs.
 *   <li>codecs are only implemented for the types we actually need for admin queries.
 * </ul>
 */
package com.datastax.oss.driver.internal.core.adminrequest;
