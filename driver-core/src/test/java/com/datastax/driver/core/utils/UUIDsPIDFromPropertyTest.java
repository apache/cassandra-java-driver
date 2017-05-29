/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core.utils;

import com.datastax.driver.core.MemoryAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class UUIDsPIDFromPropertyTest {

    private static final Logger logger = Logger.getLogger(UUIDs.class);

    @Test(groups = "isolated")
    public void should_obtain_pid_from_system_property() {
        // If the com.datastax.driver.PID property is set, it should be used and this should be logged.
        MemoryAppender appender = new MemoryAppender();
        Level originalLevel = logger.getLevel();
        try {
            logger.setLevel(Level.INFO);
            logger.addAppender(appender);
            int pid = 8675;
            System.setProperty(UUIDs.PID_SYSTEM_PROPERTY, "" + pid);
            UUIDs.timeBased();
            assertThat(appender.get())
                    .containsOnlyOnce(String.format("PID obtained from System property %s: %d",
                            UUIDs.PID_SYSTEM_PROPERTY, pid));
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(originalLevel);
        }
    }
}
