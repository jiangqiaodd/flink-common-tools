package org.bridge.metric.sdk;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * SLA 指标，包括内置的 recordsIn/recordsOut/recordsDropped/recordsErrored，以及用户自定义的指标。
 **/
public class SlaStats implements Serializable {
    // 标准四个 counter, 用于 sla 指标
    private long recordsIn;
    private long recordsOut;
    private long recordsDropped;
    private long recordsErrored;

    // 标准的 gauge 的 state 状态, 用于带日期的指标
    private HashMap<String, HashMap<String, Long>> dateGaugeState = new HashMap<>(8);
    private HashMap<String, Long> dateGaugeUpdateTime = new HashMap<>(8);

    // 用户自定义指标，存放在 customGauge 中
    private HashMap<String, HashMap<String, Long>> customGaugeState = new HashMap<>(8);
    private HashMap<String, Long> customGaugeUpdateTime = new HashMap<>(8);

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

    public HashMap<String, HashMap<String, Long>> getDateGaugeState() {
        return dateGaugeState;
    }

    public void setDateGaugeState(HashMap<String, HashMap<String, Long>> customMetrics) {
        this.dateGaugeState = customMetrics;
    }

    public HashMap<String, Long> getDateGaugeUpdateTime() {
        return dateGaugeUpdateTime;
    }

    public void setDateGaugeUpdateTime(HashMap<String, Long> dateGaugeUpdateTime) {
        this.dateGaugeUpdateTime = dateGaugeUpdateTime;
    }

    public void setCustomGaugeState(HashMap<String, HashMap<String, Long>> customGaugeState) {
        this.customGaugeState = customGaugeState;
    }

    public HashMap<String, HashMap<String, Long>> getCustomGaugeState() {
        return customGaugeState;
    }


    public HashMap<String, Long> getCustomGaugeUpdateTime() {
        return customGaugeUpdateTime;
    }

    public void setCustomGaugeUpdateTime(HashMap<String, Long> customGaugeUpdateTime) {
        this.customGaugeUpdateTime = customGaugeUpdateTime;
    }

    public void mergeState(SlaStats other) {
        System.out.println("merge state origin:" + this);
        System.out.println("merge state other:" + other);
        this.recordsIn += other.recordsIn;
        this.recordsOut += other.recordsOut;
        this.recordsDropped += other.recordsDropped;
        this.recordsErrored += other.recordsErrored;
        mergeState(this.dateGaugeState, other.dateGaugeState);
        mergeState(this.customGaugeState, other.customGaugeState);
        mergeUpdateTime(this.customGaugeUpdateTime, other.customGaugeUpdateTime);
        mergeUpdateTime(this.dateGaugeUpdateTime, other.dateGaugeUpdateTime);

        System.out.println("after merge dateGaugeState:" + dateGaugeState);
        System.out.println("after merge customGaugeState:" + customGaugeState);

    }

    public void mergeState(HashMap<String, HashMap<String, Long>> origin, HashMap<String, HashMap<String, Long>> other) {
        if (other != null) {
            for (Map.Entry<String, HashMap<String, Long>> entry: other.entrySet()) {
                if (entry.getValue() != null) {
                    HashMap<String, Long> groupContent =
                            origin.computeIfAbsent(entry.getKey(), k -> new HashMap<String, Long>(8));
                    entry.getValue().forEach(
                            (key, value) -> {
                                groupContent.merge(key, value, (v1, v2) -> v1 + v2);
                            }
                    );
                }
            }
        }
    }

    public void mergeUpdateTime(HashMap<String, Long> origin, HashMap<String, Long> other) {
        if (other != null) {
            for (Map.Entry<String, Long> entry: other.entrySet()) {
                if (entry.getValue() > origin.getOrDefault(entry.getKey(), 0L)) {
                    origin.put(entry.getKey(), entry.getValue());
                }
            }
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

    public void addCustomMetric(String group, String key, Long value) {
        addMetric(group, key, value, false);
    }


    public void addDateMetric(String group, String key, Long value) {
        addMetric(group, key, value, true);
    }

    private void addMetric(String group, String key, Long value, boolean isDateGroup) {
        if (isDateGroup) {
            if (dateGaugeState == null) {
                dateGaugeState = new HashMap<>(8);
            }
            if (dateGaugeUpdateTime == null) {
                dateGaugeUpdateTime = new HashMap<>(8);
            }
            dateGaugeState.computeIfAbsent(group, k -> new HashMap<>(8));
            long currentValue = dateGaugeState.get(group).getOrDefault(key, 0L);
            dateGaugeState.get(group).put(key, currentValue + value);
            dateGaugeUpdateTime.put(group, System.currentTimeMillis());
            System.out.println("dateGaugeState:" + dateGaugeState);
            System.out.println("dateGaugeUpdateTime:" + dateGaugeUpdateTime);
        } else {
            if (customGaugeState == null) {
                customGaugeState = new HashMap<>(8);
            }
            if (customGaugeUpdateTime == null) {
                customGaugeUpdateTime = new HashMap<>(8);
            }
            customGaugeState.computeIfAbsent(group, k -> new HashMap<>(8));
            long currentValue = customGaugeState.get(group).getOrDefault(key, 0L);
            customGaugeState.get(group).put(key, currentValue + value);
            customGaugeUpdateTime.put(group, System.currentTimeMillis());
            System.out.println("customState:" + customGaugeState);
            System.out.println("customGaugeUpdateTime:" + customGaugeUpdateTime);
        }
    }

    public void removeCustomerGroup(String group) {
        removeGroup(group, false);
    }

    public void removeDateGroup(String date) {
        removeGroup(date, true);
    }

    public void removeGroup(String group, boolean isDateGroup) {
        if (isDateGroup) {
            if (dateGaugeState != null) {
                dateGaugeState.remove(group);
            }

            if (dateGaugeUpdateTime != null) {
                dateGaugeUpdateTime.remove(group);
            }
        } else {
            if (customGaugeState != null) {
                customGaugeState.remove(group);
            }

            if (customGaugeUpdateTime != null) {
                customGaugeUpdateTime.remove(group);
            }
        }

        System.out.println("slaStat-customDateGauge: " + dateGaugeState);
        System.out.println("slaStat-dateGaugeUpdateTime " + dateGaugeUpdateTime);
    }

    @Override
    public String toString() {
        return "SlaStats{" +
                "recordsIn=" + recordsIn +
                ", recordsOut=" + recordsOut +
                ", recordsDropped=" + recordsDropped +
                ", recordsErrored=" + recordsErrored +
                ", dateGaugeState=" + dateGaugeState +
                ", customGaugeState=" + customGaugeState +
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
                Objects.equals(dateGaugeState, slaStats.dateGaugeState) &&
                Objects.equals(customGaugeState, slaStats.customGaugeState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordsIn, recordsOut, recordsDropped, recordsErrored, dateGaugeState, customGaugeState);
    }
}
