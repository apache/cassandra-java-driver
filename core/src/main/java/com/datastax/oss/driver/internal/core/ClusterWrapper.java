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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.Session;
import java.util.concurrent.CompletionStage;

/** Utility class to wrap a cluster and make it return a different session type. */
public abstract class ClusterWrapper<SourceSessionT extends Session, TargetSessionT extends Session>
    implements Cluster<TargetSessionT> {

  private final Cluster<SourceSessionT> delegate;

  public ClusterWrapper(Cluster<SourceSessionT> delegate) {
    this.delegate = delegate;
  }

  protected abstract TargetSessionT wrap(SourceSessionT session);

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public Metadata getMetadata() {
    return delegate.getMetadata();
  }

  @Override
  public boolean isSchemaMetadataEnabled() {
    return delegate.isSchemaMetadataEnabled();
  }

  @Override
  public CompletionStage<Metadata> setSchemaMetadataEnabled(Boolean newValue) {
    return delegate.setSchemaMetadataEnabled(newValue);
  }

  @Override
  public CompletionStage<Metadata> refreshSchemaAsync() {
    return delegate.refreshSchemaAsync();
  }

  @Override
  public CompletionStage<Boolean> checkSchemaAgreementAsync() {
    return delegate.checkSchemaAgreementAsync();
  }

  @Override
  public DriverContext getContext() {
    return delegate.getContext();
  }

  @Override
  public CompletionStage<TargetSessionT> connectAsync(CqlIdentifier keyspace) {
    return delegate.connectAsync(keyspace).thenApply(this::wrap);
  }

  @Override
  public Cluster<TargetSessionT> register(SchemaChangeListener listener) {
    delegate.register(listener);
    return this;
  }

  @Override
  public Cluster<TargetSessionT> unregister(SchemaChangeListener listener) {
    delegate.unregister(listener);
    return this;
  }

  @Override
  public Cluster<TargetSessionT> register(NodeStateListener listener) {
    delegate.register(listener);
    return this;
  }

  @Override
  public Cluster<TargetSessionT> unregister(NodeStateListener listener) {
    delegate.unregister(listener);
    return this;
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
