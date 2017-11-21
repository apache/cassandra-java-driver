/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.util.concurrent.CompletionStage;

/**
 * Utility class to wrap a session.
 *
 * <p>This will typically be used to mix in a convenience interface from a 3rd-party extension:
 *
 * <pre>{@code
 * class ReactiveSessionWrapper extends SessionWrapper implements ReactiveSession {
 *   public ReactiveSessionWrapper(Session delegate) {
 *     super(delegate);
 *   }
 * }
 * }</pre>
 */
public class SessionWrapper implements Session {

  private final Session delegate;

  public SessionWrapper(Session delegate) {
    this.delegate = delegate;
  }

  @Override
  public CqlIdentifier getKeyspace() {
    return delegate.getKeyspace();
  }

  @Override
  public <RequestT extends Request, ResultT> ResultT execute(
      RequestT request, GenericType<ResultT> resultType) {
    return delegate.execute(request, resultType);
  }

  @Override
  public CompletionStage<Void> closeFuture() {
    return delegate.closeFuture();
  }

  @Override
  public CompletionStage<Void> closeAsync() {
    return delegate.closeAsync();
  }

  @Override
  public CompletionStage<Void> forceCloseAsync() {
    return delegate.forceCloseAsync();
  }
}
