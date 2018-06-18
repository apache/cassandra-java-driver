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
package com.datastax.oss.driver.internal.querybuilder.schema;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

public class OptionsUtils {
  @NonNull
  public static String buildOptions(@NonNull Map<String, Object> options, boolean first) {
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, Object> option : options.entrySet()) {
      if (first) {
        builder.append(" WITH ");
        first = false;
      } else {
        builder.append(" AND ");
      }
      String value = OptionsUtils.extractOptionValue(option.getValue());
      builder.append(option.getKey()).append("=").append(value);
    }
    return builder.toString();
  }

  @NonNull
  private static String extractOptionValue(@NonNull Object option) {
    StringBuilder optionValue = new StringBuilder();
    if (option instanceof String) {
      optionValue.append("'").append((String) option).append("'");
    } else if (option instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> optionMap = (Map<String, Object>) option;
      boolean first = true;
      optionValue.append("{");
      for (Map.Entry<String, Object> subOption : optionMap.entrySet()) {
        if (first) {
          first = false;
        } else {
          optionValue.append(",");
        }
        optionValue
            .append("'")
            .append(subOption.getKey())
            .append("':")
            .append(extractOptionValue(subOption.getValue()));
      }
      optionValue.append("}");
    } else {
      optionValue.append(option);
    }
    return optionValue.toString();
  }
}
