package com.alicloud.openservices.tablestore.tunnel.pipeline;

public class ProcessDataBackoff implements IBackoff {
    private static final long STOP = -1L;
    // The current Backoff interval.
    private int currentIntervalMillis;
    private int initialIntervalMillis = 10;
    // The valid range of randomizationFactor is [0, 1).
    private double randomizationFactor = 0.25;
    private double multiplier = 5;
    private int maxIntervalMillis = 5000;
    // The initial BackOff time, updated when newly constructed or Reset.
    private long startTimeMillis;
    private int maxElapsedTimeMillis = 0;

    public ProcessDataBackoff() {
        reset();
    }

    public ProcessDataBackoff(int maxIntervalMillis) {
        reset();
        this.maxIntervalMillis = maxIntervalMillis;
    }

    @Override
    public void reset() {
        currentIntervalMillis = initialIntervalMillis;
        startTimeMillis = System.currentTimeMillis();
    }

    @Override
    public long nextBackOffMillis() {
        if (maxElapsedTimeMillis != 0 && (System.currentTimeMillis() - startTimeMillis) > maxElapsedTimeMillis) {
            return STOP;
        }
        long randomizedInterval = getRandomValueFromInterval(randomizationFactor, Math.random(), currentIntervalMillis);

        if (currentIntervalMillis >= maxIntervalMillis / multiplier) {
            currentIntervalMillis = maxIntervalMillis;
        } else {
            currentIntervalMillis *= multiplier;
        }
        return randomizedInterval;
    }

    private long getRandomValueFromInterval(
        double randomizationFactor, double random, int currentIntervalMillis) {
        double delta = randomizationFactor * currentIntervalMillis;
        double minInterval = currentIntervalMillis - delta;
        double maxInterval = currentIntervalMillis + delta;

        long randomValue = (long)(minInterval + (random * (maxInterval - minInterval + 1)));
        return randomValue;
    }
}
