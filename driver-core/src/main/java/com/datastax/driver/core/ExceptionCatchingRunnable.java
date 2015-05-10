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
package com.datastax.driver.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Simple utility class to make sure we don't let exception slip away and kill
// our executors.
abstract class ExceptionCatchingRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ExceptionCatchingRunnable.class);

    public abstract void runMayThrow() throws Exception;

    @Override
    public void run() {
        try {
            runMayThrow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Unexpected error while executing task", e);
        }
    }
}
