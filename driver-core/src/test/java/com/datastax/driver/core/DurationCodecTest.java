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
package com.datastax.driver.core;

import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static com.datastax.driver.core.Duration.*;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static org.assertj.core.api.Assertions.assertThat;

public class DurationCodecTest {

    @Test(groups = "unit")
    public void testFromStringWithStandardPattern() {
        assertCodec("1y2mo", Duration.newInstance(14, 0, 0));
        assertCodec("-1y2mo", Duration.newInstance(-14, 0, 0));
        assertCodec("1Y2MO", Duration.newInstance(14, 0, 0));
        assertCodec("2w", Duration.newInstance(0, 14, 0));
        assertCodec("2d10h", Duration.newInstance(0, 2, 10 * NANOS_PER_HOUR));
        assertCodec("2d", Duration.newInstance(0, 2, 0));
        assertCodec("30h", Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR));
        assertCodec("30h20m", Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE));
        assertCodec("20m", Duration.newInstance(0, 0, 20 * NANOS_PER_MINUTE));
        assertCodec("56s", Duration.newInstance(0, 0, 56 * NANOS_PER_SECOND));
        assertCodec("567ms", Duration.newInstance(0, 0, 567 * NANOS_PER_MILLI));
        assertCodec("1950us", Duration.newInstance(0, 0, 1950 * NANOS_PER_MICRO));
        assertCodec("1950µs", Duration.newInstance(0, 0, 1950 * NANOS_PER_MICRO));
        assertCodec("1950000ns", Duration.newInstance(0, 0, 1950000));
        assertCodec("1950000NS", Duration.newInstance(0, 0, 1950000));
        assertCodec("-1950000ns", Duration.newInstance(0, 0, -1950000));
        assertCodec("1y3mo2h10m", Duration.newInstance(15, 0, 130 * NANOS_PER_MINUTE));
    }

    @Test(groups = "unit")
    public void testFromStringWithIso8601Pattern() {
        assertCodec("P1Y2D", Duration.newInstance(12, 2, 0));
        assertCodec("P1Y2M", Duration.newInstance(14, 0, 0));
        assertCodec("P2W", Duration.newInstance(0, 14, 0));
        assertCodec("P1YT2H", Duration.newInstance(12, 0, 2 * NANOS_PER_HOUR));
        assertCodec("-P1Y2M", Duration.newInstance(-14, 0, 0));
        assertCodec("P2D", Duration.newInstance(0, 2, 0));
        assertCodec("PT30H", Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR));
        assertCodec("PT30H20M", Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE));
        assertCodec("PT20M", Duration.newInstance(0, 0, 20 * NANOS_PER_MINUTE));
        assertCodec("PT56S", Duration.newInstance(0, 0, 56 * NANOS_PER_SECOND));
        assertCodec("P1Y3MT2H10M", Duration.newInstance(15, 0, 130 * NANOS_PER_MINUTE));
    }

    @Test(groups = "unit")
    public void testFromStringWithIso8601AlternativePattern() {
        assertCodec("P0001-00-02T00:00:00", Duration.newInstance(12, 2, 0));
        assertCodec("P0001-02-00T00:00:00", Duration.newInstance(14, 0, 0));
        assertCodec("P0001-00-00T02:00:00", Duration.newInstance(12, 0, 2 * NANOS_PER_HOUR));
        assertCodec("-P0001-02-00T00:00:00", Duration.newInstance(-14, 0, 0));
        assertCodec("P0000-00-02T00:00:00", Duration.newInstance(0, 2, 0));
        assertCodec("P0000-00-00T30:00:00", Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR));
        assertCodec("P0000-00-00T30:20:00", Duration.newInstance(0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE));
        assertCodec("P0000-00-00T00:20:00", Duration.newInstance(0, 0, 20 * NANOS_PER_MINUTE));
        assertCodec("P0000-00-00T00:00:56", Duration.newInstance(0, 0, 56 * NANOS_PER_SECOND));
        assertCodec("P0001-03-00T02:10:00", Duration.newInstance(15, 0, 130 * NANOS_PER_MINUTE));
    }

    private void assertCodec(String input, Duration expected) {
        // serialize + deserialize
        ByteBuffer bytes = TypeCodec.duration().serialize(Duration.from(input), V4);
        Duration actual = TypeCodec.duration().deserialize(bytes, V4);
        assertThat(actual).isEqualTo(expected);
        // format + parse
        String format = TypeCodec.duration().format(Duration.from(input));
        actual = TypeCodec.duration().parse(format);
        assertThat(actual).isEqualTo(expected);
        // parse alone
        actual = TypeCodec.duration().parse(input);
        assertThat(actual).isEqualTo(expected);
    }

}
