package org.bridge.metric.sdk;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;

public class MyRichMapFunction extends RichMapFunction<String, String> implements ListCheckpointed<SlaStats> {
    private SlaManager slaManager = new SlaManager(
            5 * 1000 * 60, 1000 * 60 * 3, "yyyyMM*dd HH:mm");
    int index = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        slaManager.registerMetrics(getRuntimeContext());
        slaManager.addLocalToMetrics();

    }

    @Override
    public String map(String value) throws Exception {
        slaManager.addOneToRecordsIn();

        HashMap<String, String> tags = new HashMap<>(8);
        tags.put("keyword", "User");
        slaManager.addOneToDateMetric(LocalDateTime.now(), "records_in", tags);

        slaManager.addOneToDateMetric(LocalDateTime.now(), "records_dropped", tags);
        tags.put("keyword", "Item");
        slaManager.addOneToDateMetric(LocalDateTime.now(), "records_dropped", tags);
        slaManager.addOneToDateMetric(LocalDateTime.now(), "records_errored", tags);


        if (index ++ < 15) {
            slaManager.addOneToCustomMetric("self_metric");
        }

        if (index > 15 && index < 30) {
            slaManager.addOneToCustomMetric("20210831", "data_self_metric");
        }

        if (index > 20 && index < 36) {
            slaManager.addOneToCustomMetric("20210832", "date_self_metric");
        }

        if (index > 80) {
            index = 0;
        }
        return value;
    }

    @Override
    public List<SlaStats> snapshotState(long l, long l1) throws Exception {
        return slaManager.snapshotState(l, l1);
    }

    @Override
    public void restoreState(List<SlaStats> list) throws Exception {
        System.out.println("restore from checkpoint ");
        slaManager.restoreState(list);
    }
}
