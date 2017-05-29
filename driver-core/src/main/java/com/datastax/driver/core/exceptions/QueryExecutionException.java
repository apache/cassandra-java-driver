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
package com.datastax.driver.core.exceptions;

/**
 * Exception related to the execution of a query.
 * <p/>
 * This corresponds to the exception that Cassandra throws when a (valid) query
 * cannot be executed (TimeoutException, UnavailableException, ...).
 */
@SuppressWarnings("serial")
public abstract class QueryExecutionException extends DriverException {

    protected QueryExecutionException(String msg) {
        super(msg);
    }

    protected QueryExecutionException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
