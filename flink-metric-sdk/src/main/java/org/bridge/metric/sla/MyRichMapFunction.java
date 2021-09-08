package org.bridge.metric.sla;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.List;

public class MyRichMapFunction extends RichMapFunction implements ListCheckpointed<SlaStats> {
    private final SlaManager slaManager = new SlaManager();

    @Override
    public void open(Configuration parameters) throws Exception {
        slaManager.registerAccumulator(getRuntimeContext());
        slaManager.addLocalToAccumulator();

    }

    @Override
    public Object map(Object value) throws Exception {
        slaManager.addOneToRecordsIn();
        return value;
    }

    @Override
    public List<SlaStats> snapshotState(long l, long l1) throws Exception {
        return slaManager.snapshotState(l, l1);
    }

    @Override
    public void restoreState(List<SlaStats> list) throws Exception {
        getRuntimeContext();
        slaManager.restoreState(list);
    }
}
