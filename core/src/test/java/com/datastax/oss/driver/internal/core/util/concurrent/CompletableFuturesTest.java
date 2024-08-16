package com.datastax.oss.driver.internal.core.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class CompletableFuturesTest {
  @Test
  public void should_not_suppress_identical_exceptions() throws Exception {
    RuntimeException error = new RuntimeException();
    CompletableFuture<Void> future1 = new CompletableFuture<>();
    future1.completeExceptionally(error);
    CompletableFuture<Void> future2 = new CompletableFuture<>();
    future2.completeExceptionally(error);
    try {
      // if timeout exception is thrown, it indicates that CompletableFutures.allSuccessful()
      // did not complete the returned future and potentially caller will wait infinitely
      CompletableFutures.allSuccessful(Arrays.asList(future1, future2))
          .toCompletableFuture()
          .get(1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      assertThat(e.getCause()).isEqualTo(error);
    }
  }
}
