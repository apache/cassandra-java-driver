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
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.WriteType;

/**
 * A Cassandra timeout during a write query.
 */
public class WriteTimeoutException extends QueryTimeoutException {

    private final WriteType writeType;

    public WriteTimeoutException(ConsistencyLevel consistency, WriteType writeType, int received, int required) {
        super(String.format("Cassandra timeout during write query at consistency %s (%d replica were required but only %d acknowledged the write)", consistency, required, received),
              consistency,
              received,
              required);
        this.writeType = writeType;
    }

    private WriteTimeoutException(String msg, Throwable cause, ConsistencyLevel consistency, WriteType writeType, int received, int required) {
        super(msg, cause, consistency, received, required);
        this.writeType = writeType;
    }

    /**
     * The type of the write for which a timeout was raised.
     *
     * @return the type of the write for which a timeout was raised.
     */
    public WriteType getWriteType() {
        return writeType;
    }

    public DriverException copy() {
        return new WriteTimeoutException(getMessage(),
                                         this,
                                         getConsistencyLevel(),
                                         getWriteType(),
                                         getReceivedAcknowledgements(),
                                         getRequiredAcknowledgements());
    }
}
