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
package com.datastax.driver.extras.codecs.joda;

import com.datastax.driver.core.Assertions;
import com.datastax.driver.core.TupleType;
import org.joda.time.DateTime;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;
import static org.joda.time.DateTimeZone.forID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DateTimeCodecTest {

    private final TupleType tupleType = mock(TupleType.class);

    @DataProvider(name = "DateTimeCodecTest.parse")
    public Object[][] parseParameters() {
        return new Object[][]{
                {null, null},
                {"", null},
                {"NULL", null},
                {"(0                         ,'UTC')", new DateTime(0, forID("UTC"))},
                {"(1277860847999             ,'+01:00')", new DateTime("2010-06-30T02:20:47.999+01:00", forID("+01:00"))},
                {"('2010-06-30T01:20Z'       ,'+01:00')", new DateTime("2010-06-30T02:20:00.000+01:00", forID("+01:00"))},
                {"('2010-06-30T01:20:47Z'    ,'+01:00')", new DateTime("2010-06-30T02:20:47.000+01:00", forID("+01:00"))},
                {"('2010-06-30T01:20:47.999Z','+01:00')", new DateTime("2010-06-30T02:20:47.999+01:00", forID("+01:00"))}
        };
    }

    @DataProvider(name = "DateTimeCodecTest.format")
    public Object[][] formatParameters() {
        return new Object[][]{
                {null, "NULL"},
                {new DateTime(0).withZone(UTC), "('1970-01-01T00:00:00.000Z','+00:00')"},
                {new DateTime("2010-06-30T01:20:47.999+01:00", forID("+01:00")), "('2010-06-30T00:20:47.999Z','+01:00')"}
        };
    }

    @Test(groups = "unit", dataProvider = "DateTimeCodecTest.parse")
    public void should_parse_valid_formats(String input, DateTime expected) {
        // given
        when(tupleType.getComponentTypes()).thenReturn(newArrayList(timestamp(), varchar()));
        DateTimeCodec codec = new DateTimeCodec(tupleType);
        // when
        DateTime actual = codec.parse(input);
        // then
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "unit", dataProvider = "DateTimeCodecTest.format")
    public void should_serialize_and_format_valid_object(DateTime input, String expected) {
        // given
        when(tupleType.getComponentTypes()).thenReturn(newArrayList(timestamp(), varchar()));
        DateTimeCodec codec = new DateTimeCodec(tupleType);
        // when
        String actual = codec.format(input);
        // then
        Assertions.assertThat(codec).withProtocolVersion(V4).canSerialize(input);
        assertThat(actual).isEqualTo(expected);
    }

}
