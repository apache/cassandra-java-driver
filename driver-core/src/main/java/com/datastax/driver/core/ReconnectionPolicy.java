package com.datastax.driver.core;

public interface ReconnectionPolicy {

    public long nextDelayMs();

    public interface Factory {
        public ReconnectionPolicy create();
    }

    public static class Constant implements ReconnectionPolicy {

        private final long delayMs;

        // TODO: validate arguments
        private Constant(long delayMs) {
            this.delayMs = delayMs;
        }

        public long nextDelayMs() {
            return delayMs;
        }

        public static ReconnectionPolicy.Factory makeFactory(final long constantDelayMs) {
            return new ReconnectionPolicy.Factory() {
                public ReconnectionPolicy create() {
                    return new Constant(constantDelayMs);
                }
            };
        }
    }

    public static class Exponential implements ReconnectionPolicy {

        private final long baseDelayMs;
        private final long maxDelayMs;
        private int attempts;

        // TODO: validate arguments
        private Exponential(long baseDelayMs, long maxDelayMs) {
            this.baseDelayMs = baseDelayMs;
            this.maxDelayMs = maxDelayMs;
        }

        public long nextDelayMs() {
            ++attempts;
            return baseDelayMs * (1 << attempts);
        }

        public static ReconnectionPolicy.Factory makeFactory(final long baseDelayMs, final long maxDelayMs) {
            return new ReconnectionPolicy.Factory() {
                public ReconnectionPolicy create() {
                    return new Exponential(baseDelayMs, maxDelayMs);
                }
            };
        }
    }
}
