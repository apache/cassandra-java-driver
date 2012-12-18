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
