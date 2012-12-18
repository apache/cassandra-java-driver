package com.datastax.driver.stress;

import java.util.concurrent.*;

public class Producer extends Thread {

    private final QueryGenerator generator;
    private final BlockingQueue<QueryGenerator.Request> workQueue;

    public Producer(QueryGenerator generator, BlockingQueue<QueryGenerator.Request> workQueue) {
        super("Producer Thread");
        this.generator = generator;
        this.workQueue = workQueue;
        this.setDaemon(true);
    }

    public void run() {

        try {

            while (generator.hasNext())
                workQueue.put(generator.next());

            workQueue.put(QueryGenerator.DONE_MARKER);

        } catch (InterruptedException e) {
            System.err.println("Producer interrupted" + (e.getMessage() != null ? ": " + e.getMessage() : ""));
            return;
        }
    }
}
