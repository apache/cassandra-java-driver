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
package com.datastax.oss.driver.internal.core.config.composite;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(DataProviderRunner.class)
public class CompositeDriverConfigReloadTest {

  @Mock private DriverConfigLoader primaryLoader;
  @Mock private DriverConfigLoader fallbackLoader;
  private DriverConfigLoader compositeLoader;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    compositeLoader = DriverConfigLoader.compose(primaryLoader, fallbackLoader);
  }

  @Test
  @UseDataProvider("reloadabilities")
  public void should_be_reloadable_if_either_child_is(
      boolean primaryIsReloadable,
      boolean fallbackIsReloadable,
      boolean compositeShouldBeReloadable) {
    when(primaryLoader.supportsReloading()).thenReturn(primaryIsReloadable);
    when(fallbackLoader.supportsReloading()).thenReturn(fallbackIsReloadable);
    assertThat(compositeLoader.supportsReloading()).isEqualTo(compositeShouldBeReloadable);
  }

  @Test
  @UseDataProvider("reloadabilities")
  public void should_delegate_reloading_to_reloadable_children(
      boolean primaryIsReloadable,
      boolean fallbackIsReloadable,
      boolean compositeShouldBeReloadable) {
    when(primaryLoader.supportsReloading()).thenReturn(primaryIsReloadable);
    when(primaryLoader.reload())
        .thenReturn(
            primaryIsReloadable
                ? CompletableFuture.completedFuture(true)
                : CompletableFutures.failedFuture(new UnsupportedOperationException()));

    when(fallbackLoader.supportsReloading()).thenReturn(fallbackIsReloadable);
    when(fallbackLoader.reload())
        .thenReturn(
            fallbackIsReloadable
                ? CompletableFuture.completedFuture(true)
                : CompletableFutures.failedFuture(new UnsupportedOperationException()));

    CompletionStage<Boolean> reloadFuture = compositeLoader.reload();

    if (compositeShouldBeReloadable) {
      assertThat(reloadFuture).isCompletedWithValue(true);
    } else {
      assertThat(reloadFuture).isCompletedExceptionally();
      Throwable t = catchThrowable(() -> reloadFuture.toCompletableFuture().get());
      assertThat(t).hasRootCauseInstanceOf(UnsupportedOperationException.class);
    }
    verify(primaryLoader, primaryIsReloadable ? times(1) : never()).reload();
    verify(fallbackLoader, fallbackIsReloadable ? times(1) : never()).reload();
  }

  @DataProvider
  public static Object[][] reloadabilities() {
    return new Object[][] {
      // primaryIsReloadable, fallbackIsReloadable, compositeShouldBeReloadable
      new Object[] {true, true, true},
      new Object[] {true, false, true},
      new Object[] {false, true, true},
      new Object[] {false, false, false},
    };
  }
}
