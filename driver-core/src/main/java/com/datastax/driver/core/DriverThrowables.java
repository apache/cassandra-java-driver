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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;

import java.util.concurrent.ExecutionException;

class DriverThrowables {

    static RuntimeException propagateCause(ExecutionException e) {
        Throwable cause = e.getCause();

        if (cause instanceof Error)
            throw ((Error) cause);

        // We could just rethrow e.getCause(). However, the cause of the ExecutionException has likely been
        // created on the I/O thread receiving the response. Which means that the stacktrace associated
        // with said cause will make no mention of the current thread. This is painful for say, finding
        // out which execute() statement actually raised the exception. So instead, we re-create the
        // exception.
        if (cause instanceof DriverException)
            throw ((DriverException) cause).copy();
        else
            throw new DriverInternalError("Unexpected exception thrown", cause);
    }
}
