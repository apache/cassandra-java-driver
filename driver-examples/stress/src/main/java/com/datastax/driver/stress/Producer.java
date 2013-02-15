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
