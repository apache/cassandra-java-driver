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
