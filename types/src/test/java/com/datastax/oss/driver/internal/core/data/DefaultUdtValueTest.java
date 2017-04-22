/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.data;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.type.DataType;
import com.datastax.oss.driver.api.type.UserDefinedType;
import com.datastax.oss.driver.internal.type.UserDefinedTypeBuilder;
import java.util.List;

public class DefaultUdtValueTest extends AccessibleByIdTestBase<UdtValue> {

  @Override
  protected UdtValue newInstance(List<DataType> dataTypes, AttachmentPoint attachmentPoint) {
    UserDefinedTypeBuilder builder =
        new UserDefinedTypeBuilder(
            CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("type"));
    for (int i = 0; i < dataTypes.size(); i++) {
      builder.withField(CqlIdentifier.fromInternal("field" + i), dataTypes.get(i));
    }
    UserDefinedType userDefinedType = builder.build();
    userDefinedType.attach(attachmentPoint);
    return userDefinedType.newValue();
  }
}
