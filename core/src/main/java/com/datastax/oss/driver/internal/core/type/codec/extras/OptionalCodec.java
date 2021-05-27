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
package com.datastax.oss.driver.internal.core.type.codec.extras;

import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import net.jcip.annotations.Immutable;

/**
 * A codec that wraps other codecs around {@link Optional} instances.
 *
 * @param <T> The wrapped Java type.
 */
@Immutable
public class OptionalCodec<T> extends MappingCodec<T, Optional<T>> {

  public OptionalCodec(@NonNull TypeCodec<T> innerCodec) {
    super(
        Objects.requireNonNull(innerCodec, "innerCodec must not be null"),
        GenericType.optionalOf(innerCodec.getJavaType()));
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    Objects.requireNonNull(value);
    if (value instanceof Optional) {
      Optional<?> optional = (Optional<?>) value;
      return optional.map(innerCodec::accepts).orElse(true);
    }
    return false;
  }

  @Nullable
  @Override
  protected Optional<T> innerToOuter(@Nullable T value) {
    return Optional.ofNullable(isAbsent(value) ? null : value);
  }

  @Nullable
  @Override
  protected T outerToInner(@Nullable Optional<T> value) {
    return value != null && value.isPresent() ? value.get() : null;
  }

  protected boolean isAbsent(@Nullable T value) {
    return value == null
        || (value instanceof Collection && ((Collection<?>) value).isEmpty())
        || (value instanceof Map && ((Map<?, ?>) value).isEmpty());
  }
}
