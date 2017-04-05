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
package com.datastax.driver.core.policies;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.Assertions.assertThat;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.when;

public class RollingCountTest {
    @Mock
    Clock clock;

    RollingCount rollingCount;

    @BeforeMethod(groups = "unit")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        assertThat(clock.nanoTime()).isEqualTo(0);
        rollingCount = new RollingCount(clock);
    }

    @Test(groups = "unit")
    public void should_record_adds_in_first_interval() {
        // t = 0
        rollingCount.increment();
        setTime(1, SECONDS);
        rollingCount.increment();
        setTime(2, SECONDS);
        rollingCount.increment();

        // the count does not update in real time...
        assertThat(rollingCount.get()).isEqualTo(0);

        // but only at the end of each 5-second interval
        setTime(5, SECONDS);
        assertThat(rollingCount.get()).isEqualTo(3);
    }

    @Test(groups = "unit")
    public void should_record_adds_over_two_intervals() {
        rollingCount.add(2);

        setTime(5, SECONDS); // 2nd interval
        rollingCount.add(3);

        setTime(10, SECONDS);
        assertThat(rollingCount.get()).isEqualTo(5);
    }

    @Test(groups = "unit")
    public void should_record_adds_separated_by_idle_intervals() {
        rollingCount.add(2);

        setTime(10, SECONDS); // 3rd interval
        rollingCount.add(3);

        setTime(15, SECONDS);
        assertThat(rollingCount.get()).isEqualTo(5);
    }

    @Test(groups = "unit")
    public void should_rotate() {
        // 2 in [0,5[, then 1 every 5 seconds in [5,60[
        rollingCount.add(2);
        for (int i = 1; i < 12; i++) {
            setTime(i * 5, SECONDS);
            rollingCount.add(1);
        }
        setTime(60, SECONDS);
        // the previous minute is now [0,60[
        assertThat(rollingCount.get()).isEqualTo(13);

        rollingCount.add(1);
        setTime(65, SECONDS);
        // the previous minute is now [5,65[, so the 2 events from [0,5[ should be forgotten
        assertThat(rollingCount.get()).isEqualTo(12);
    }

    @Test(groups = "unit")
    public void should_rotate_with_idle_intervals() {
        // 1 every 5 seconds in [0,60[
        for (int i = 0; i < 12; i++) {
            setTime(i * 5, SECONDS);
            rollingCount.add(1);
        }
        // idle in [60, 75[, then 1 in [75,80[
        setTime(75, SECONDS);
        rollingCount.add(1);

        setTime(80, SECONDS);
        // the last minute is [20,80[, with 1 every 5 seconds except during 15 seconds
        assertThat(rollingCount.get()).isEqualTo(9);
    }

    @Test(groups = "unit")
    public void should_rotate_when_idle_for_full_period() {
        // 1 every 5 seconds in [0,60[
        for (int i = 0; i < 12; i++) {
            setTime(i * 5, SECONDS);
            rollingCount.add(1);
        }
        // idle for the next minute [60,120[, then 1 in [120,125[
        setTime(120, SECONDS);
        rollingCount.add(1);

        setTime(125, SECONDS);
        assertThat(rollingCount.get()).isEqualTo(1);
    }

    @Test(groups = "unit")
    public void should_rotate_when_idle_for_more_than_full_period() {
        // 1 every 5 seconds in [0,60[
        for (int i = 0; i < 12; i++) {
            setTime(i * 5, SECONDS);
            rollingCount.add(1);
        }
        // idle for the next minute and 5 seconds [60,125[, then 1 in [125,130[
        setTime(125, SECONDS);
        rollingCount.add(1);

        setTime(130, SECONDS);
        assertThat(rollingCount.get()).isEqualTo(1);
    }

    private void setTime(long time, TimeUnit unit) {
        when(clock.nanoTime()).thenReturn(NANOSECONDS.convert(time, unit));
    }
}
