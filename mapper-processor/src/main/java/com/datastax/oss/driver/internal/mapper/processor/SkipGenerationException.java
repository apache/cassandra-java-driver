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
package com.datastax.oss.driver.internal.mapper.processor;

/**
 * Convenience exception to abort generation when an error is found.
 *
 * <p>The throwing code should generally use {@link DecoratedMessager} to issue a compilation error,
 * then some parent component higher up the stack should catch and ignore this error.
 */
public class SkipGenerationException extends RuntimeException {}
