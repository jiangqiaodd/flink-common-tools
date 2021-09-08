package org.bridge.metric.sdk;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


public class JobEntry {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(20 * 1000, CheckpointingMode.AT_LEAST_ONCE);
//        environment.setParallelism(1);

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
        src.map(new MyRichMapFunction()).name("richMap").uid("selfRichMap").print();
        environment.execute("sla utils demo");

    }
}
