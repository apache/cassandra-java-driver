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
package com.datastax.driver.extras.codecs.jdk8;

import com.datastax.driver.core.Assertions;
import com.datastax.driver.core.TupleType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.google.common.collect.Lists.newArrayList;
import static java.time.Instant.ofEpochMilli;
import static java.time.Instant.parse;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.ofInstant;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ZonedDateTimeCodecTest {

    private final TupleType tupleType = mock(TupleType.class);

    @DataProvider(name = "ZonedDateTimeCodecTest.parse")
    public Object[][] parseParameters() {
        return new Object[][]{
                {null, null},
                {"", null},
                {"NULL", null},
                {"(0                         ,'+01:00')", ofInstant(ofEpochMilli(0), ZoneOffset.of("+01:00"))},
                {"(1277860847999             ,'+01:00')", ofInstant(parse("2010-06-30T01:20:47.999Z"), ZoneOffset.of("+01:00"))},
                {"('2010-06-30T01:20Z'       ,'+01:00')", ofInstant(parse("2010-06-30T01:20:00.000Z"), ZoneOffset.of("+01:00"))},
                {"('2010-06-30T01:20:47Z'    ,'+01:00')", ofInstant(parse("2010-06-30T01:20:47.000Z"), ZoneOffset.of("+01:00"))},
                {"('2010-06-30T01:20:47.999Z','+01:00')", ofInstant(parse("2010-06-30T01:20:47.999Z"), ZoneOffset.of("+01:00"))}
        };
    }

    @DataProvider(name = "ZonedDateTimeCodecTest.format")
    public Object[][] formatParameters() {
        return new Object[][]{
                {null, "NULL"},
                {ofInstant(ofEpochMilli(0), UTC), "('1970-01-01T00:00:00Z','+00:00')"},
                {ZonedDateTime.parse("2010-06-30T01:20:47.999+01:00"), "('2010-06-30T00:20:47.999Z','+01:00')"}
        };
    }

    @Test(groups = "unit", dataProvider = "ZonedDateTimeCodecTest.parse")
    public void should_parse_valid_formats(String input, ZonedDateTime expected) {
        // given
        TupleType tupleType = mock(TupleType.class);
        when(tupleType.getComponentTypes()).thenReturn(newArrayList(timestamp(), varchar()));
        ZonedDateTimeCodec codec = new ZonedDateTimeCodec(tupleType);
        // when
        ZonedDateTime actual = codec.parse(input);
        // then
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "unit", dataProvider = "ZonedDateTimeCodecTest.format")
    public void should_serialize_and_format_valid_object(ZonedDateTime input, String expected) {
        // given
        when(tupleType.getComponentTypes()).thenReturn(newArrayList(timestamp(), varchar()));
        ZonedDateTimeCodec codec = new ZonedDateTimeCodec(tupleType);
        // when
        String actual = codec.format(input);
        // then
        Assertions.assertThat(codec).withProtocolVersion(V4).canSerialize(input);
        assertThat(actual).isEqualTo(expected);
    }

}
