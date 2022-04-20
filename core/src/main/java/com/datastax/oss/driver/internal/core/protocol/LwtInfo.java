/*
 * Copyright (C) 2022 ScyllaDB
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

package com.datastax.oss.driver.internal.core.protocol;

import java.util.List;
import java.util.Map;

public class LwtInfo {
  private static final String SCYLLA_LWT_ADD_METADATA_MARK_KEY = "SCYLLA_LWT_ADD_METADATA_MARK";
  private static final String LWT_OPTIMIZATION_META_BIT_MASK_KEY = "LWT_OPTIMIZATION_META_BIT_MASK";

  private final int mask;

  private LwtInfo(int m) {
    mask = m;
  }

  public int getMask() {
    return mask;
  }

  public boolean isLwt(int flags) {
    return (flags & mask) == mask;
  }

  public static LwtInfo parseLwtInfo(Map<String, List<String>> supported) {
    if (!supported.containsKey(SCYLLA_LWT_ADD_METADATA_MARK_KEY)) {
      return null;
    }
    List<String> list = supported.get(SCYLLA_LWT_ADD_METADATA_MARK_KEY);
    if (list == null || list.size() != 1) {
      return null;
    }
    String val = list.get(0);
    if (val == null || !val.startsWith(LWT_OPTIMIZATION_META_BIT_MASK_KEY + "=")) {
      return null;
    }
    long mask;
    try {
      mask = Long.parseLong(val.substring((LWT_OPTIMIZATION_META_BIT_MASK_KEY + "=").length()));
    } catch (Exception e) {
      System.err.println(
          "Error while parsing " + LWT_OPTIMIZATION_META_BIT_MASK_KEY + ": " + e.getMessage());
      return null;
    }
    if (mask > Integer.MAX_VALUE) {
      // Unfortunately server returns mask as unsigned int32 so we have to parse it as int64 and
      // convert to proper signed int32
      mask += Integer.MIN_VALUE;
      mask += Integer.MIN_VALUE;
    }

    return new LwtInfo((int) mask);
  }

  public void addOption(Map<String, String> options) {
    options.put(SCYLLA_LWT_ADD_METADATA_MARK_KEY, Integer.toString(mask));
  }
}
