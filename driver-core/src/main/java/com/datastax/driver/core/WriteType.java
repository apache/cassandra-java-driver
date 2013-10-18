/*
 *      Copyright (C) 2012 DataStax Inc.
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
package com.datastax.driver.core;

/**
 * The type of a Cassandra write query.
 * <p>
 * This information is returned by Cassandra when a write timeout is raised to
 * indicate what type of write timeouted. This information is useful to decide
 * which retry policy to adopt.
 */
public enum WriteType
{
    /** A write to a single partition key. Such writes are guaranteed to be atomic and isolated. */
    SIMPLE,
    /** A write to a multiple partition key that used the distributed batch log to ensure atomicity. */
    BATCH,
    /** A write to a multiple partition key that doesn't use the distributed batch log. Atomicity for such writes is not guaranteed */
    UNLOGGED_BATCH,
    /** A counter write (that can be for one or multiple partition key). Such write should not be replayed to avoid overcount. */
    COUNTER,
    /** The initial write to the distributed batch log that Cassandra performs internally before a BATCH write. */
    BATCH_LOG,
    /**
     * A conditional write. If a timeout has this {@code WriteType}, the timeout has happened while doing the compare-and-swap for
     * an conditional update. In this case, the update may or may not have been applied.
     */
    CAS;
}
