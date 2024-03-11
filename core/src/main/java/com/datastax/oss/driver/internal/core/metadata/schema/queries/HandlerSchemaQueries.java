package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class HandlerSchemaQueries implements SchemaQueries {

  private final CompletableFuture<SchemaRows> schemaRowsFuture = new CompletableFuture<>();

  private final String message;

  public HandlerSchemaQueries(String message) {
    this.message = message;
  }

  @Override
  public CompletionStage<SchemaRows> execute() {
    schemaRowsFuture.completeExceptionally(new IllegalStateException(message));
    return schemaRowsFuture;
  }
}
