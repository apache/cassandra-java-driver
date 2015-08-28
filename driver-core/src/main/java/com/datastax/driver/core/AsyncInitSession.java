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
package com.datastax.driver.core;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * A {@link Session} that can be initialized asynchronously.
 *
 * This interface exists only for backward compatibility reasons: {@link #initAsync()} should really be
 * defined by {@link Session}, but adding it after the fact would break binary compatibility.
 *
 * By default, all sessions returned by the driver implement this interface. The only way you would get
 * sessions that don't is if you use a custom {@link Cluster} subclass.
 */
public interface AsyncInitSession extends Session {
    /**
     * Initialize this session asynchronously.
     *
     * @return a future that will complete when the session is fully initialized.
     */
    ListenableFuture<Session> initAsync();
}
