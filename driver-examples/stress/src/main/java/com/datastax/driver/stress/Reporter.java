/*
 *      Copyright (C) 2012 DataStax Inc.
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
package com.datastax.driver.stress;

import java.io.File;
import java.util.concurrent.*;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.*;

public class Reporter {

    public final Meter requests = Metrics.newMeter(Reporter.class, "requests", "requests", TimeUnit.SECONDS);
    public final Timer latencies = Metrics.newTimer(Reporter.class, "latencies", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    public Reporter(boolean useCsv) {
        if (useCsv) {
            CsvReporter.enable(new File("metrics"), 1, TimeUnit.SECONDS);
        } else {
            ConsoleReporter.enable(5, TimeUnit.SECONDS);
        }
    }
}
