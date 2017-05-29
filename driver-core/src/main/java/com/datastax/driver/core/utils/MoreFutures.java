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
package com.datastax.driver.core.utils;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Helpers to work with Guava's {@link ListenableFuture}.
 */
public class MoreFutures {
    /**
     * An immediate successful {@code ListenableFuture<Void>}.
     */
    public static final ListenableFuture<Void> VOID_SUCCESS = Futures.immediateFuture(null);

    /**
     * A {@link FutureCallback} that does nothing on failure.
     */
    public static abstract class SuccessCallback<V> implements FutureCallback<V> {
        @Override
        public void onFailure(Throwable t) { /* nothing */ }
    }

    /**
     * A {@link FutureCallback} that does nothing on success.
     */
    public static abstract class FailureCallback<V> implements FutureCallback<V> {
        @Override
        public void onSuccess(V result) { /* nothing */ }
    }
}
