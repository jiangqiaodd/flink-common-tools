package org.bridge.metric.sla;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * SLA 计算函数，利用 OperatorState 统计 SLA 指标并通过 REST API 暴露到 Web UI。
 *
 * 统计的 SLA 指标有:
 * - recordsIn: 摄入的数据记录数
 * - recordsOut: 输出的数据记录数
 * - recordsDropped: 被过滤的数据记录数
 * - recordsErr: 出现错误的数据记录数
 **/
public class SlaManager implements Serializable {

    private SlaStats slaStats = new SlaStats();

    private LongCounter recordsIn = new LongCounter();
    private LongCounter recordsOut = new LongCounter();
    private LongCounter recordsDropped = new LongCounter();
    private LongCounter recordsErrored = new LongCounter();
    private HashMap<String, LongCounter> customCounters = new HashMap<>(8);

    private static final DateTimeFormatter DATE_PREFIX_FOMRATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    static final long serialVersionUID = 1L;

    /**
     * 在 RichFunction 的 open 函数使用，注册 Accumulator。
     * @param runtimeContext RichFunction 的 RuntimeContext
     */
    public void registerAccumulator(RuntimeContext runtimeContext) {
        recordsIn = runtimeContext.getLongCounter("recordsIn");
        recordsOut = runtimeContext.getLongCounter("recordsOut");
        recordsDropped = runtimeContext.getLongCounter("recordsDropped");
        recordsErrored = runtimeContext.getLongCounter("recordsErrored");
    }

    public void registerAccumulator(RuntimeContext runtimeContext, Collection<String> customMetricKeys) {
        registerAccumulator(runtimeContext);
        Set<String> allKeys = new HashSet<>(10);
        allKeys.addAll(customMetricKeys);
        if (slaStats.getCustomMetrics() != null) {
            allKeys.addAll(slaStats.getCustomMetrics().keySet());
        }
        allKeys.forEach(
                key -> {
                    LongCounter customCounter =  runtimeContext.getLongCounter(key);
                    customCounters.put(key, customCounter);
                }
        );
    }

    /**
     * 在 RichFunction 的 open 函数调用，将算子本地的恢复的 SLA 指标加到全局 Accumulator 指标里。
     */
    public void addLocalToAccumulator() {
        // 此时本地的 Counter 是初始状态，直接加上恢复的值即可
        recordsIn.add(slaStats.getRecordsIn());
        recordsOut.add(slaStats.getRecordsOut());
        recordsDropped.add(slaStats.getRecordsDropped());
        recordsErrored.add(slaStats.getRecordsErrored());

        if (slaStats.getCustomMetrics() != null) {
            slaStats.getCustomMetrics().forEach(
                    (key, value) -> {
                        if (customCounters != null && customCounters.containsKey(key)) {
                            customCounters.get(key).add(value);
                        }
                    }
            );
        }
    }

    /**
     *  在用户函数内调用，记录一条新增的 recordsIn。
     */
    public void addOneToRecordsIn() {
        recordsIn.add(1);
        slaStats.addOneToRecordsIn();
    }

    /**
     *  在用户函数内调用，记录一条新增的 recordsOut。
     */
    public void addOneToRecordsOut() {
        recordsOut.add(1);
        slaStats.addOneToRecordsOut();
    }

    /**
     *  在用户函数内调用，记录一条新增的 recordsDropped。
     */
    public void addOneToRecordsDropped() {
        recordsDropped.add(1);
        slaStats.addOneToRecordsDropped();
    }

    /**
     *  在用户函数内调用，记录一条新增的 recordsErrored。
     */
    public void addOneToRecordsErrored() {
        recordsErrored.add(1);
        slaStats.addOneToRecordsErrored();
    }

    /**
     * 在用户函数内调用，新增一条记录到用户自定义 counter。如果该 key 不存在则自动注册，但是要到作业重启之后才能暴露到 Accumulator 上。
     * @deprecated 使用 #addOneToCustomCounter(String, RuntimeContext)
     */
    @Deprecated
    public void addOneToCustomCounter(String key) {
        if (customCounters.containsKey(key)) {
            customCounters.get(key).add(1);
            slaStats.addOneToCustomMetric(key);
        } else {
            LongCounter newCounter = new LongCounter();
            newCounter.add(1);
            customCounters.put(key, newCounter);
            slaStats.addCustomMetric(key, 1L);
        }
    }

    /**
     * 在用户函数内调用，新增一条记录到用户自定义 counter。如果该 key 不存在则自动注册。
     */
    public void addOneToCustomCounter(String key, RuntimeContext runtimeContext) {
        addOneToCustomCounter(key, runtimeContext, false);
    }

    /**
     * 在用户函数内调用，新增一条记录到用户自定义 counter。如果该 key 不存在则自动注册。
     */
    public void addOneToCustomCounter(String key, RuntimeContext runtimeContext, boolean datePrefix) {
        if (datePrefix) {
            key = LocalDate.now().format(DATE_PREFIX_FOMRATTER) + "-" + key;
        }
        if (customCounters.containsKey(key)) {
            customCounters.get(key).add(1);
            slaStats.addOneToCustomMetric(key);
        } else {
            LongCounter newCounter = runtimeContext.getLongCounter(key);
            newCounter.add(1);
            customCounters.put(key, newCounter);
            slaStats.addCustomMetric(key, 1L);
        }
    }

    /**
     * 在 ListCheckpointed 的 snapshotState 函数内使用，记录 SlaStats 到 OperatorState。
     */
    public List<SlaStats> snapshotState(long checkpointId, long timestamp) throws Exception {
        return Lists.newArrayList(slaStats);
    }

    /**
     * 在 ListCheckpointed 的 restoreState 函数内使用，从 OperatorState 恢复 SlaStats。
     */
    public void restoreState(List<SlaStats> list) throws Exception {
        for (SlaStats element: list) {
            slaStats.merge(element);
        }
    }
}
