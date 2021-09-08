package org.bridge.metric.sla;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


public class SlaJobEntry {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(30 * 1000, CheckpointingMode.AT_LEAST_ONCE);
        environment.setParallelism(2);

        DataStream<String> src = environment.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true) {
                    int x = (int) (Math.random() * 10);
                    sourceContext.collect("key" + x);
                    Thread.sleep(2 * 1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

//        src.print();
        src.map(new RichMapFunction<String, String>() {
            private final SlaManager slaManager = new SlaManager();

            @Override
            public void open(Configuration parameters) throws Exception {
                slaManager.registerAccumulator(getRuntimeContext());
                slaManager.addLocalToAccumulator();
            }

            @Override
            public String map(String value) throws Exception {
                slaManager.addOneToRecordsIn();
                return value;
            }
        });


        environment.execute("sla utils demo");

    }
}
