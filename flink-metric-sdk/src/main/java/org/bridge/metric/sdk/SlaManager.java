package org.bridge.metric.sdk;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;

/**
 * SLA 计算函数，利用 OperatorState 统计 SLA 指标并通过 REST API 暴露到 Web UI。
 * <p>
 * 统计的 SLA 指标有:
 * - recordsIn: 摄入的数据记录数
 * - recordsOut: 输出的数据记录数
 * - recordsDropped: 被过滤的数据记录数
 * - recordsErr: 出现错误的数据记录数
 **/
public class SlaManager implements Serializable {
    static final long serialVersionUID = 1L;
    private static final String BASIC_METRIC_GROUP = "streamfly_custom_metrics";
    private static final String BASIC_GAUGE_NAME = "streamfly_standard_gauge";
    private static final String CUSTOM_GAUGE_NAME = "streamfly_custom_gauge";
    private static final String DEFAULT_DATE_TIME_PATTERN = "yyyyMMdd";
    private final long dateGroupExpiredTimeMillis;
    private final long customGroupExpiredTimeMillis;
    private final String dateTimePattern;
    private final SlaStats slaStats = new SlaStats();
    private MetricGroup basicMetricGroup;
    private Counter recordsIn;
    private Counter recordsOut;
    private Counter recordsDropped;
    private Counter recordsErrored;
    private GroupedGauge dateGauge;
    private GroupedGauge customGauge;
    private Set<String> aliveDateSet;
    private Set<String> aliveGroupSet;
    private transient DateTimeFormatter dateGroupFormatter;

    public SlaManager() {
        this.dateGroupExpiredTimeMillis = 1000 * 3600 * 24 * 3;
        this.customGroupExpiredTimeMillis = 1000 * 3600 * 24;
        this.dateTimePattern = DEFAULT_DATE_TIME_PATTERN;
    }

    public SlaManager(long dateGroupExpiredTimeMillis, long customGroupExpiredTimeMillis) {
        this.dateGroupExpiredTimeMillis = dateGroupExpiredTimeMillis;
        this.customGroupExpiredTimeMillis = customGroupExpiredTimeMillis;
        this.dateTimePattern = DEFAULT_DATE_TIME_PATTERN;
    }

    public SlaManager(long dateGroupExpiredTimeMillis, long customGroupExpiredTimeMillis, String dateTimePattern) {
        this.dateGroupExpiredTimeMillis = dateGroupExpiredTimeMillis;
        this.customGroupExpiredTimeMillis = customGroupExpiredTimeMillis;
        this.dateTimePattern = dateTimePattern;
        checkDateTimePattern(dateTimePattern);
    }

    private void checkDateTimePattern(String dateTimePattern) {
        DateTimeFormatter.ofPattern(dateTimePattern);
    }


    /**
     * 在 RichFunction 的 open 函数使用，注册 Accumulator。
     *
     * @param runtimeContext RichFunction 的 RuntimeContext
     */
    public void registerMetrics(RuntimeContext runtimeContext) {
        basicMetricGroup = runtimeContext.getMetricGroup();
        MetricGroup counterGroup = basicMetricGroup.addGroup(BASIC_METRIC_GROUP);
        recordsIn = counterGroup.counter("recordsIn");
        recordsOut = counterGroup.counter("recordsOut");
        recordsDropped = counterGroup.counter("recordsDropped");
        recordsErrored = counterGroup.counter("recordsErrored");
        customGauge = basicMetricGroup.gauge(CUSTOM_GAUGE_NAME, new GroupedGauge());
        dateGauge = basicMetricGroup.gauge(BASIC_GAUGE_NAME, new GroupedGauge());

        customGauge.add("test_group", "test_key", 1000L);

        aliveDateSet = new HashSet<>();
        aliveGroupSet = new HashSet<>();

        dateGroupFormatter = DateTimeFormatter.ofPattern(dateTimePattern);
    }

    /**
     * 在 RichFunction 的 open 函数调用，将算子本地的恢复的 SLA 指标加到全局 Accumulator 指标里。
     */
    public void addLocalToMetrics() {
        System.out.println("starting to add stored value to metrics...." + slaStats);
        // 此时本地的 Counter 是初始状态，直接加上恢复的值即可
        recordsIn.inc(slaStats.getRecordsIn());
        recordsOut.inc(slaStats.getRecordsOut());
        recordsDropped.inc(slaStats.getRecordsDropped());
        recordsErrored.inc(slaStats.getRecordsErrored());

        // 如果存在着自定义 metric, 保留到 customGauge 中
        if (slaStats.getCustomGaugeState() != null) {
            slaStats.getCustomGaugeState().forEach(
                    (group, keyValues) -> {
                        if (keyValues != null) {
                            this.customGauge.add(group, keyValues);
                            this.aliveGroupSet.add(group);
                        }
                    }
            );
        }

        if (slaStats.getDateGaugeState() != null) {
            slaStats.getDateGaugeState().forEach(
                    (date, keyValues) -> {
                        if (keyValues != null) {
                            this.dateGauge.add(date, keyValues);
                            this.aliveDateSet.add(date);
                        }
                    }
            );
        }
    }

    /**
     *  在用户函数内调用，记录一条新增的 recordsIn。
     */
    public void addOneToRecordsIn() {
        recordsIn.inc(1);
        slaStats.addOneToRecordsIn();
    }

    /**
     *  在用户函数内调用，记录一条新增的 recordsOut。
     */
    public void addOneToRecordsOut() {
        recordsOut.inc(1);
        slaStats.addOneToRecordsOut();
    }

    /**
     *  在用户函数内调用，记录一条新增的 recordsDropped。
     */
    public void addOneToRecordsDropped() {
        recordsDropped.inc(1);
        slaStats.addOneToRecordsDropped();
    }

    /**
     *  在用户函数内调用，记录一条新增的 recordsErrored。
     */
    public void addOneToRecordsErrored() {
        recordsErrored.inc(1);
        slaStats.addOneToRecordsErrored();
    }

    /**
     * 在用户函数内调用，新增一条记录到用户自定义 counter。
     */
    public void addOneToCustomMetric(String key) {
        addToCustomMetric(key, key, 1L, null);
    }

    public void addOneToCustomMetric(String group, String key) {
        addToCustomMetric(group, key, 1L, null);
    }

    private void addToCustomMetric(String group, String key, long value, Map<String, String> tags) {
        this.customGauge.add(group, key, value, tags);
        this.aliveGroupSet.add(group);
        this.slaStats.addCustomMetric(group, GaugeElement.concatAsIdentifiedName(key, tags), value);
    }

    public void addOneToDateMetric(TemporalAccessor temporalAccessor, String key) {
        addToDateMetric(temporalAccessor, key, 1L, null);
    }

    public void addOneToDateMetric(TemporalAccessor temporalAccessor, String key, Map tags) {
        addToDateMetric(temporalAccessor, key, 1L, tags);
    }

    private void addToDateMetric(TemporalAccessor temporalAccessor, String key, long value, Map tags) {
        String date = this.dateGroupFormatter.format(temporalAccessor);
        this.dateGauge.add(date, key, value, tags);
        this.aliveDateSet.add(date);
        this.slaStats.addDateMetric(date, GaugeElement.concatAsIdentifiedName(key, tags), value);
    }

    /**
     * 在 ListCheckpointed 的 snapshotState 函数内使用，记录 SlaStats 到 OperatorState。
     * 并对过期 metric keys 1. state 清理   2. gauge 清理
     */
    public List<SlaStats> snapshotState(long checkpointId, long timestamp) throws Exception {
        HashMap<String, Long> dateGroupUpdateTime = slaStats.getDateGaugeUpdateTime();
        HashMap<String, Long> customGroupUpdateTime = slaStats.getCustomGaugeUpdateTime();
        Iterator<String> aliveGroupIterator = aliveGroupSet.iterator();
        Iterator<String> aliveDateIterator = aliveDateSet.iterator();

        while (aliveGroupIterator.hasNext()) {
            String aliveGroup = aliveGroupIterator.next();
            if (customGroupUpdateTime != null && customGroupUpdateTime.containsKey(aliveGroup)) {
                long updateTime = customGroupUpdateTime.get(aliveGroup);
                if (timestamp - updateTime > customGroupExpiredTimeMillis) {
                    customGauge.remove(aliveGroup);
                    slaStats.removeGroup(aliveGroup, false);
                    aliveGroupIterator.remove();
                }
            }

        }

        // remove expired date
        while (aliveDateIterator.hasNext()) {
            String aliveDate = aliveDateIterator.next();
            if (dateGroupUpdateTime != null && dateGroupUpdateTime.containsKey(aliveDate)) {
                long updateTime = dateGroupUpdateTime.get(aliveDate);
                if (timestamp - updateTime > dateGroupExpiredTimeMillis) {
                    dateGauge.remove(aliveDate);
                    slaStats.removeGroup(aliveDate, true);
                    aliveDateIterator.remove();
                }
            }
        }
        return Lists.newArrayList(slaStats);
    }

    /**
     * 在 ListCheckpointed 的 restoreState 函数内使用，从 OperatorState 恢复 SlaStats。
     */
    public void restoreState(List<SlaStats> list) throws Exception {
        for (SlaStats element : list) {
            slaStats.mergeState(element);
        }
    }
}
