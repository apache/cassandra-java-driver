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
 * Top level class for exceptions thrown by the driver.
 */
public class DriverException extends RuntimeException {

    private static final long serialVersionUID = 0;

    public DriverException(String message) {
        super(message);
    }

    public DriverException(Throwable cause) {
        super(cause);
    }

    public DriverException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Copy the exception.
     * <p/>
     * This returns a new exception, equivalent to the original one, except that
     * because a new object is created in the current thread, the top-most
     * element in the stacktrace of the exception will refer to the current
     * thread (this is mainly intended for internal use by the driver). The cause of
     * the copied exception will be the original exception.
     *
     * @return a copy/clone of this exception.
     */
    public DriverException copy() {
        return new DriverException(getMessage(), this);
    }
}
