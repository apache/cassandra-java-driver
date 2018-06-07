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
package com.datastax.oss.driver.osgi;

import static com.datastax.oss.driver.osgi.BundleOptions.snappyBundle;
import static org.ops4j.pax.exam.CoreOptions.options;

import org.ops4j.pax.exam.Option;

public class OsgiSnappyIT extends OsgiBaseIT {

  @Override
  public Option[] additionalOptions() {
    return options(snappyBundle());
  }

  @Override
  public String[] sessionOptions() {
    return new String[] {"advanced.protocol.compression = snappy"};
  }
}
