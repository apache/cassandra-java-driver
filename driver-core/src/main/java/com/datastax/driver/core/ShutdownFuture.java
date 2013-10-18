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

import java.util.List;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;

/**
 * A future on the shutdown of a Cluster or Session instance.
 * <p>
 * This is a standard future except for the fact that this class has an
 * additional {@link #force} method that can be used to expedite the shutdown
 * process (see below).
 * <p>
 * Note that this class implements <a href="http://code.google.com/p/guava-libraries/">Guava</a>'s {@code
 * ListenableFuture} and can so be used with Guava's future utilities.
 */
public abstract class ShutdownFuture extends AbstractFuture<Void> {

    ShutdownFuture() {}

    static ShutdownFuture immediateFuture() {
        ShutdownFuture future = new ShutdownFuture() {
            @Override
            public ShutdownFuture force() {
                return this;
            }
        };
        future.set(null);
        return future;
    }

    /**
     * Try to force the completion of the shutdown this a future of.
     * <p>
     * This method will do its best to expedite the shutdown process. In particular, all connection
     * will be closed right away, even if there is ongoing queries at the time of the call
     * to this method.
     * <p>
     * Note that this method does not block. The completion of this method does not imply that the
     * shutdown process and you still need to wait on this future to ensure that, though calling
     * this method does ensure said future will return in a timely way.
     *
     * @return this {@code ShutdownFuture}.
     */
    public abstract ShutdownFuture force();

    // Internal utility for cases where we want to build a future that wait on other ones
    static class Forwarding extends ShutdownFuture {

        private final List<ShutdownFuture> futures;

        Forwarding(List<ShutdownFuture> futures) {
            this.futures = futures;

            Futures.addCallback(Futures.allAsList(futures), new FutureCallback<List<Void>>() {
                public void onFailure(Throwable t) {
                    Forwarding.this.setException(t);
                }

                public void onSuccess(List<Void> v) {
                    Forwarding.this.onFuturesDone();
                }
            });
        }

        @Override
        public ShutdownFuture force() {
            for (ShutdownFuture future : futures)
                future.force();
            return this;
        }

        protected void onFuturesDone() {
            set(null);
        }
    }
}
