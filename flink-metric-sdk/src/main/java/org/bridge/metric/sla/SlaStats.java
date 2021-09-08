package org.bridge.metric.sla;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;

/**
 * SLA 指标，包括内置的 recordsIn/recordsOut/recordsDropped/recordsErrored，以及用户自定义的指标。
 **/
public class SlaStats implements Serializable {
    private long recordsIn;
    private long recordsOut;
    private long recordsDropped;
    private long recordsErrored;
    private HashMap<String, Long> customMetrics;

    static final long serialVersionUID = 1L;

    public SlaStats() {
    }

    public long getRecordsIn() {
        return recordsIn;
    }

    public void setRecordsIn(long recordsIn) {
        this.recordsIn = recordsIn;
    }

    public long getRecordsOut() {
        return recordsOut;
    }

    public void setRecordsOut(long recordsOut) {
        this.recordsOut = recordsOut;
    }

    public long getRecordsDropped() {
        return recordsDropped;
    }

    public void setRecordsDropped(long recordsDropped) {
        this.recordsDropped = recordsDropped;
    }

    public long getRecordsErrored() {
        return recordsErrored;
    }

    public void setRecordsErrored(long recordsErrored) {
        this.recordsErrored = recordsErrored;
    }

    public HashMap<String, Long> getCustomMetrics() {
        return customMetrics;
    }

    public void setCustomMetrics(HashMap<String, Long> customMetrics) {
        this.customMetrics = customMetrics;
    }

    public void merge(SlaStats other) {
        this.recordsIn += other.recordsIn;
        this.recordsOut += other.recordsOut;
        this.recordsDropped += other.recordsDropped;
        this.recordsErrored += other.recordsErrored;
        if (other.customMetrics != null && this.customMetrics != null) {
            other.customMetrics.forEach(
                    (key, value) -> {
                        if (value != null) {
                            this.customMetrics.merge(key, value, (v1, v2) -> v1 + v2);
                        }
                    }
            );
        } else if (this.customMetrics == null) {
            this.customMetrics = other.customMetrics;
        }
    }

    public void addOneToRecordsIn() {
        this.recordsIn++;
    }

    public void addOneToRecordsOut() {
        this.recordsOut++;
    }

    public void addOneToRecordsDropped() {
        this.recordsDropped++;
    }

    public void addOneToRecordsErrored() {
        this.recordsErrored++;
    }

    public void addOneToCustomMetric(String key) {
        if (customMetrics == null) {
            customMetrics = new HashMap<>(8);
        }
        long currentValue = customMetrics.getOrDefault(key, 0L);
        this.customMetrics.put(key, currentValue + 1);
    }

    public void addCustomMetric(String key, Long value) {
        if (customMetrics == null) {
            customMetrics = new HashMap<>(8);
        }
        this.customMetrics.put(key, value);
    }

    @Override
    public String toString() {
        return "SlaStats{" +
                "recordsIn=" + recordsIn +
                ", recordsOut=" + recordsOut +
                ", recordsDropped=" + recordsDropped +
                ", recordsErrored=" + recordsErrored +
                ", customMetrics=" + customMetrics +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SlaStats)) return false;
        SlaStats slaStats = (SlaStats) o;
        return recordsIn == slaStats.recordsIn &&
                recordsOut == slaStats.recordsOut &&
                recordsDropped == slaStats.recordsDropped &&
                recordsErrored == slaStats.recordsErrored &&
                Objects.equals(customMetrics, slaStats.customMetrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordsIn, recordsOut, recordsDropped, recordsErrored, customMetrics);
    }
}
