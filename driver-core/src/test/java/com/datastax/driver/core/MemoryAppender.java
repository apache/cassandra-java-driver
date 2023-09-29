/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import java.io.StringWriter;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;

/**
 * Simple Log4J appender that captures logs to memory in order to inspect them in unit tests.
 * <p>
 * There is no purging mechanism, so make sure it doesn't stay enabled for too long (this is best
 * done with an {@code @After} method that removes it).
 */
public class MemoryAppender extends WriterAppender {
    public final StringWriter writer = new StringWriter();

    public MemoryAppender() {
        setWriter(writer);
        setLayout(new PatternLayout("%m%n"));
    }

    public String get() {
        return writer.toString();
    }
}
