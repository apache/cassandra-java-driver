package com.datastax.driver.core;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class NamedThreadFactory implements ThreadFactory {

    protected final String name;
    protected final AtomicInteger n = new AtomicInteger(1);

    public NamedThreadFactory(String name) {
        this.name = name;
    }

    public Thread newThread(Runnable runnable) {
        return new Thread(runnable, name + "-" + n.getAndIncrement());
    }
}

