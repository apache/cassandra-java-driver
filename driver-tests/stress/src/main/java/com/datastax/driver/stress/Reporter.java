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
package com.datastax.driver.stress;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.stats.Snapshot;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Reporter implements Runnable {

    private Meter requests;
    private Timer latencies;

    public Meter requestsMiddle;
    public Timer latenciesMiddle;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final File csvFile;
    private final String header;
    private final long period;
    private final int consoleReportPeriod;
    private final int iterations;
    private final AtomicInteger requestsDone = new AtomicInteger(0);

    private final int middleLowBound;
    private final int middleHighBound;

    private volatile double meanMiddleRate = -1.0;

    private long startTime;
    private long lastOpCount;

    private PrintStream csv;

    private long lastConsoleOpCount;
    private long lastConsoleTimestamp;
    private int tickSinceLastConsoleReport;

    public Reporter(int consoleReportPeriod, String csvFileName, String[] args, int iterations) {
        this(1, consoleReportPeriod, new File(csvFileName), formatHeader(args), iterations);
    }

    private Reporter(int csvPeriod, int consolePeriod, File csvFile, String header, int iterations) {
        this.period = csvPeriod;
        this.consoleReportPeriod = consolePeriod / csvPeriod;
        this.csvFile = csvFile;
        this.header = header;
        this.iterations = iterations;

        // Middle = exclude first and last 10% (note: if requests is -1, we'll end up with negative bounds, which will always exclude. It's fine if we never stop anyway)
        this.middleLowBound = (int) (0.1 * iterations);
        this.middleHighBound = (int) (0.9 * iterations);
    }

    private static String formatHeader(String[] args) {
        StringBuilder sb = new StringBuilder();
        sb.append("stress");
        for (String arg : args)
            sb.append(' ').append(arg);
        return sb.toString();
    }

    public void start() {

        this.requests = Metrics.newMeter(Reporter.class, "requests", "requests", TimeUnit.SECONDS);
        this.latencies = Metrics.newTimer(Reporter.class, "latencies", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        this.startTime = System.currentTimeMillis();
        initConsole();
        initCSV();

        this.executor.scheduleAtFixedRate(this, period, period, TimeUnit.SECONDS);
    }

    public Context newRequest() {
        int iteration = requestsDone.getAndIncrement();
        if (iteration >= middleLowBound) {
            if (latenciesMiddle == null) {
                synchronized (this) {
                    if (latenciesMiddle == null) {
                        latenciesMiddle = Metrics.newTimer(Reporter.class, "latencies_middle", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
                        requestsMiddle = Metrics.newMeter(Reporter.class, "requests_middle", "requests", TimeUnit.SECONDS);
                    }
                }
            }
            if (iteration > middleHighBound) {
                if (meanMiddleRate < 0)
                    meanMiddleRate = requestsMiddle.meanRate();

                return new Context(this, latencies.time(), null);
            } else {
                return new Context(this, latencies.time(), latenciesMiddle.time());
            }
        } else {
            return new Context(this, latencies.time(), null);
        }
    }

    public void stop() {
        executor.shutdown();

        long tstamp = System.currentTimeMillis();
        long elapsed = TimeUnit.MILLISECONDS.toSeconds(tstamp - startTime);
        Report lastReport = new Report(tstamp, elapsed, requests, latencies, lastOpCount);

        stopCSV(lastReport);
        stopConsole(lastReport);
    }

    @Override
    public void run() {
        report();
    }

    private void report() {
        long tstamp = System.currentTimeMillis();
        long elapsed = TimeUnit.MILLISECONDS.toSeconds(tstamp - startTime);

        Report report = new Report(tstamp, elapsed, requests, latencies, lastOpCount);
        lastOpCount = report.totalOps;

        reportToCSV(report);
        reportToConsole(report);
    }

    private void initCSV() {
        if (csvFile.exists())
            if (!csvFile.delete())
                throw new RuntimeException("File " + csvFile + " already exists and cannot delete it");

        try {
            if (!csvFile.createNewFile())
                throw new RuntimeException("Unable to create report file " + csvFile);

            csv = new PrintStream(new FileOutputStream(csvFile));

            csv.println("# " + header);
            csv.println("# elapsed,total_ops,interval_rate,mean_rate,mean_latency,95th_latency,99th_latency,std_dev");

        } catch (IOException e) {
            throw new RuntimeException("Error creating report file " + csvFile, e);
        }
    }

    private void reportToCSV(Report report) {
        csv.println(new StringBuilder()
                .append(report.elapsed).append(',')
                .append(report.totalOps).append(',')
                .append(report.intervalRate).append(',')
                .append(report.meanRate).append(',')
                .append(report.meanLatency).append(',')
                .append(report.latency95th).append(',')
                .append(report.latency99th).append(',')
                .append(report.stdDev));
    }

    private void stopCSV(Report lastReport) {
        reportToCSV(lastReport);
    }

    private void initConsole() {
        this.lastConsoleTimestamp = startTime;

        System.out.println(" Time (s) |    total ops | interval rate |    mean rate | mean latency (ms) | 95th latency (ms) | 99th latency (ms) |   std dev");
        System.out.println("--------------------------------------------------------------------------------------------------------------------------------");
    }

    private void reportToConsole(Report report) {

        if (++tickSinceLastConsoleReport < consoleReportPeriod)
            return;

        printReportToConsole(report);

        tickSinceLastConsoleReport = 0;
        lastConsoleTimestamp = report.timestamp;
        lastConsoleOpCount = report.totalOps;
    }

    private void printReportToConsole(Report report) {
        long delay = report.timestamp - lastConsoleTimestamp;
        double intervalRate = (((double) (report.totalOps - lastConsoleOpCount)) / delay) * 1000;

        String msg = String.format(" %8d | %12d | %13.2f | %12.2f | %17.3f | %17.3f | %17.3f | %9.3f",
                report.elapsed, report.totalOps, intervalRate, report.meanRate, report.meanLatency, report.latency95th, report.latency99th, report.stdDev);
        System.out.println(msg);
    }

    private void stopConsole(Report lastReport) {
        printReportToConsole(lastReport);

        if (latenciesMiddle == null)
            return;

        Snapshot snapshot = latenciesMiddle.getSnapshot();

        System.out.println();
        System.out.println("For the middle 80% of values:");
        System.out.println(String.format("  Mean rate (ops/sec):          %10.1f", meanMiddleRate));
        System.out.println(String.format("  Mean latency (ms):            %10.3f", latenciesMiddle.mean()));
        System.out.println(String.format("  Median latency (ms):          %10.3f", snapshot.getMedian()));
        System.out.println(String.format("  75th percentile latency (ms): %10.3f", snapshot.get75thPercentile()));
        System.out.println(String.format("  95th percentile latency (ms): %10.3f", snapshot.get95thPercentile()));
        System.out.println(String.format("  99th percentile latency (ms): %10.3f", snapshot.get99thPercentile()));
        System.out.println(String.format("  Standard latency deviation:   %10.3f", latenciesMiddle.stdDev()));
    }

    private static class Report {
        public final long timestamp;
        public final long elapsed;

        public final long totalOps;
        public final long intervalRate;
        public final double meanRate;

        public final double meanLatency;
        public final double latency95th;
        public final double latency99th;
        public final double stdDev;

        public Report(long timestamp, long elapsed, Meter meter, Timer timer, long lastOpCount) {
            this.timestamp = timestamp;
            this.elapsed = elapsed;

            this.totalOps = meter.count();
            this.intervalRate = totalOps - lastOpCount;
            this.meanRate = meter.meanRate();

            Snapshot snapshot = timer.getSnapshot();
            this.meanLatency = timer.mean();
            this.latency95th = snapshot.get95thPercentile();
            this.latency99th = snapshot.get99thPercentile();
            this.stdDev = timer.stdDev();
        }
    }

    public static class Context {
        private final Reporter reporter;
        private final TimerContext context;
        private final TimerContext contextMiddle;

        private Context(Reporter reporter, TimerContext context, TimerContext contextMiddle) {
            this.reporter = reporter;
            this.context = context;
            this.contextMiddle = contextMiddle;
        }

        public void done() {
            context.stop();
            reporter.requests.mark();
            if (contextMiddle != null) {
                contextMiddle.stop();
                reporter.requestsMiddle.mark();
            }
        }
    }
}
