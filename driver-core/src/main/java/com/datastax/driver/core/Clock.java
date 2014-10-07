package com.datastax.driver.core;

/**
 * This interface allows us not to have a direct call to {@code System.currentTimeMillis()} for testing purposes
 */
interface Clock {
    /**
     * Returns the current time in milliseconds
     *
     * @return the difference, measured in milliseconds, between the current time and midnight, January 1, 1970 UTC.
     * @see System#currentTimeMillis()
     */
    long currentTime();
}

/**
 * Default implementation of a clock that delegate its calls to the system clock.
 */
class SystemClock implements Clock {
    @Override
    public long currentTime() {
        return System.currentTimeMillis();
    }
}