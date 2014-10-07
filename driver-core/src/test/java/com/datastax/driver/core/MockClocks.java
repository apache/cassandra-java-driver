package com.datastax.driver.core;

class MockClocks {
    static class BackInTimeClock implements Clock {
        final long arbitraryTimeStamp = 1412610226270L;
        int calls;

        @Override
        public long currentTime() {
            return arbitraryTimeStamp - calls++;
        }
    }

    static class FixedTimeClock implements Clock {
        final long fixedTime;

        public FixedTimeClock(long fixedTime) {
            this.fixedTime = fixedTime;
        }

        @Override
        public long currentTime() {
            return fixedTime;
        }
    }
}
