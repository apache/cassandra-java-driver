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
 * An unexpected error happened internally.
 *
 * This should never be raise and indicates a bug (either in the driver or in
 * Cassandra).
 */
public class DriverInternalError extends RuntimeException {

    public DriverInternalError(String message) {
        super(message);
    }

    public DriverInternalError(Throwable cause) {
        super(cause);
    }

    public DriverInternalError(String message, Throwable cause) {
        super(message, cause);
    }

}
