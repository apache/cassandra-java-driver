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

/**
 * Error during a truncation operation.
 */
public class TruncateException extends QueryExecutionException {

    private static final long serialVersionUID = 0;

    public TruncateException(String msg) {
        super(msg);
    }

    private TruncateException(String msg, Throwable cause) {
        super(msg, cause);
    }

    @Override
    public DriverException copy() {
        return new TruncateException(getMessage(), this);
    }
}
