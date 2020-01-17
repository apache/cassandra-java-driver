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
package com.datastax.dse.driver.api.core.data.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.internal.SerializationHelper;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.text.ParseException;
import java.time.temporal.ChronoField;
import java.util.function.Predicate;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DateRangeTest {

  @Test
  @UseDataProvider("rangeStrings")
  public void should_parse_and_format(String source) throws Exception {
    DateRange parsed = DateRange.parse(source);
    assertThat(parsed.toString()).isEqualTo(source);
  }

  @DataProvider
  public static Object[][] rangeStrings() {
    return new Object[][] {
      {"[2011-01 TO 2015]"},
      {"[2010-01-02 TO 2015-05-05T13]"},
      {"[1973-06-30T13:57:28.123Z TO 1999-05-05T14:14:59]"},
      // leap year
      {"[2010-01-01T15 TO 2016-02]"},
      // pre-epoch
      {"[1500 TO 1501]"},
      {"[0001 TO 0001-01-02]"},
      {"[0000 TO 0000-01-02]"},
      {"[-0001 TO -0001-01-02]"},
      // unbounded
      {"[* TO 2014-12-01]"},
      {"[1999 TO *]"},
      {"[* TO *]"},
      // single bound ranges
      // AD/BC era boundary
      {"0001-01-01"},
      {"-0001-01-01"},
      {"-0009"},
      {"2000-11"},
      {"*"}
    };
  }

  @Test
  public void should_use_proleptic_parser() throws Exception {
    DateRange parsed = DateRange.parse("[0000 TO 0000-01-02]");
    assertThat(parsed.getLowerBound().getTimestamp().get(ChronoField.YEAR)).isEqualTo(0);
  }

  @Test
  public void should_fail_to_parse_invalid_strings() {
    assertThatThrownBy(() -> DateRange.parse("foo")).matches(hasOffset(0));
    assertThatThrownBy(() -> DateRange.parse("[foo TO *]")).matches(hasOffset(1));
    assertThatThrownBy(() -> DateRange.parse("[* TO foo]")).matches(hasOffset(6));
  }

  private static Predicate<Throwable> hasOffset(int offset) {
    return e -> ((ParseException) e).getErrorOffset() == offset;
  }

  @Test
  public void should_fail_to_parse_inverted_range() {
    assertThatThrownBy(() -> DateRange.parse("[2001-01 TO 2000]"))
        .hasMessage(
            "Lower bound of a date range should be before upper bound, got: [2001-01 TO 2000]");
  }

  @Test
  public void should_not_equate_single_date_open_to_both_open_range() throws Exception {
    assertThat(DateRange.parse("*")).isNotEqualTo(DateRange.parse("[* TO *]"));
  }

  @Test
  public void should_not_equate_same_ranges_with_different_precisions() throws ParseException {
    assertThat(DateRange.parse("[2001 TO 2002]"))
        .isNotEqualTo(DateRange.parse("[2001-01 TO 2002-12]"));
  }

  @Test
  public void should_give_same_hashcode_to_equal_objects() throws ParseException {
    assertThat(DateRange.parse("[2001 TO 2002]").hashCode())
        .isEqualTo(DateRange.parse("[2001 TO 2002]").hashCode());
  }

  @Test
  public void should_serialize_and_deserialize() throws Exception {
    DateRange initial = DateRange.parse("[1973-06-30T13:57:28.123Z TO 1999-05-05T14:14:59]");
    DateRange deserialized = SerializationHelper.serializeAndDeserialize(initial);
    assertThat(deserialized).isEqualTo(initial);
  }
}
