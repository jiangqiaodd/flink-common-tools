package org.bridge.metric.test;

import org.apache.flink.metrics.Gauge;

public class LongGauge implements Gauge<Long> {
    private long value;
    @Override
    public Long getValue() {
        return value;
    }

    public void inc(long l) {
        value += l;
    }

    public void inc() {
        value += 1;
    }
 }
