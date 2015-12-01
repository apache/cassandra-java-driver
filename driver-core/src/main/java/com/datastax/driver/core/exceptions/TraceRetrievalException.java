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
package com.datastax.driver.core.exceptions;

/**
 * Exception thrown if a query trace cannot be retrieved.
 *
 * @see com.datastax.driver.core.QueryTrace
 */
public class TraceRetrievalException extends DriverException {

    private static final long serialVersionUID = 0;

    public TraceRetrievalException(String message) {
        super(message);
    }

    public TraceRetrievalException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public TraceRetrievalException copy() {
        return new TraceRetrievalException(getMessage(), this);
    }
}
