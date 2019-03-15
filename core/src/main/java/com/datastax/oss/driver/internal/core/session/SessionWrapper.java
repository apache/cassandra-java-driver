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
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;

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
@ThreadSafe
public class SessionWrapper implements Session {

  private final Session delegate;

  public SessionWrapper(@NonNull Session delegate) {
    this.delegate = delegate;
  }

  @NonNull
  public Session getDelegate() {
    return delegate;
  }

  @NonNull
  @Override
  public String getName() {
    return delegate.getName();
  }

  @NonNull
  @Override
  public Metadata getMetadata() {
    return delegate.getMetadata();
  }

  @Override
  public boolean isSchemaMetadataEnabled() {
    return delegate.isSchemaMetadataEnabled();
  }

  @NonNull
  @Override
  public CompletionStage<Metadata> setSchemaMetadataEnabled(@Nullable Boolean newValue) {
    return delegate.setSchemaMetadataEnabled(newValue);
  }

  @NonNull
  @Override
  public CompletionStage<Metadata> refreshSchemaAsync() {
    return delegate.refreshSchemaAsync();
  }

  @NonNull
  @Override
  public CompletionStage<Boolean> checkSchemaAgreementAsync() {
    return delegate.checkSchemaAgreementAsync();
  }

  @NonNull
  @Override
  public DriverContext getContext() {
    return delegate.getContext();
  }

  @NonNull
  @Override
  public Optional<CqlIdentifier> getKeyspace() {
    return delegate.getKeyspace();
  }

  @NonNull
  @Override
  public Optional<Metrics> getMetrics() {
    return delegate.getMetrics();
  }

  @Nullable
  @Override
  public <RequestT extends Request, ResultT> ResultT execute(
      @NonNull RequestT request, @NonNull GenericType<ResultT> resultType) {
    return delegate.execute(request, resultType);
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    return delegate.closeFuture();
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    return delegate.closeAsync();
  }

  @NonNull
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    return delegate.forceCloseAsync();
  }
}
