/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.api.core.data.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZonedDateTime;
import org.junit.Test;

public class DateRangePrecisionTest {

  @Test
  public void should_round_up() {
    ZonedDateTime timestamp = ZonedDateTime.parse("2011-02-03T04:05:16.789Z");
    assertThat(DateRangePrecision.MILLISECOND.roundUp(timestamp))
        .isEqualTo("2011-02-03T04:05:16.789Z");
    assertThat(DateRangePrecision.SECOND.roundUp(timestamp)).isEqualTo("2011-02-03T04:05:16.999Z");
    assertThat(DateRangePrecision.MINUTE.roundUp(timestamp)).isEqualTo("2011-02-03T04:05:59.999Z");
    assertThat(DateRangePrecision.HOUR.roundUp(timestamp)).isEqualTo("2011-02-03T04:59:59.999Z");
    assertThat(DateRangePrecision.DAY.roundUp(timestamp)).isEqualTo("2011-02-03T23:59:59.999Z");
    assertThat(DateRangePrecision.MONTH.roundUp(timestamp)).isEqualTo("2011-02-28T23:59:59.999Z");
    assertThat(DateRangePrecision.YEAR.roundUp(timestamp)).isEqualTo("2011-12-31T23:59:59.999Z");
  }

  @Test
  public void should_round_down() {
    ZonedDateTime timestamp = ZonedDateTime.parse("2011-02-03T04:05:16.789Z");
    assertThat(DateRangePrecision.MILLISECOND.roundDown(timestamp))
        .isEqualTo("2011-02-03T04:05:16.789Z");
    assertThat(DateRangePrecision.SECOND.roundDown(timestamp))
        .isEqualTo("2011-02-03T04:05:16.000Z");
    assertThat(DateRangePrecision.MINUTE.roundDown(timestamp))
        .isEqualTo("2011-02-03T04:05:00.000Z");
    assertThat(DateRangePrecision.HOUR.roundDown(timestamp)).isEqualTo("2011-02-03T04:00:00.000Z");
    assertThat(DateRangePrecision.DAY.roundDown(timestamp)).isEqualTo("2011-02-03T00:00:00.000Z");
    assertThat(DateRangePrecision.MONTH.roundDown(timestamp)).isEqualTo("2011-02-01T00:00:00.000Z");
    assertThat(DateRangePrecision.YEAR.roundDown(timestamp)).isEqualTo("2011-01-01T00:00:00.000Z");
  }
}
