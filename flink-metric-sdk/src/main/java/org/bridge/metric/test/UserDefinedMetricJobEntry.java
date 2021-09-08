package org.bridge.metric.test;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;

public class UserDefinedMetricJobEntry {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> src = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect("hello world");
                    Thread.sleep(1000 * 5);
                }
            }

            @Override
            public void cancel() {

            }
        });

        src.map(new RichMapFunction<String, String>() {
            Counter counter1, counter2;
            MyGauge gauge;
            LongGauge gauge1;
            GenericMetricGroup group;
            boolean groupClosed = false;
            Tuple3Gauge tuple3Gauge;
            int index = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().getMetricGroup();
                group = (GenericMetricGroup)getRuntimeContext().getMetricGroup().addGroup("streamfly_custom_metrics").addGroup("20210812");

                System.out.println("group class:" + group.getClass());
                counter1 = group.counter("self_counter1");
                counter2 = group.counter("self_counter2");
                gauge1 = new LongGauge();
                group.gauge("self_gauge1", gauge1);
                System.out.println(getRuntimeContext().getMetricGroup());
                for (Map.Entry entry : getRuntimeContext().getMetricGroup().getAllVariables().entrySet()) {
                    System.out.println("metric group variable" + entry.getKey() +  ":" + entry.getValue());
                }



                HashMap<String, Long> map = new HashMap<String, Long>();
                map.put("test_key", 1000L);
                gauge = new MyGauge();
                gauge.add("20210901", "keywordA", 10L);
                getRuntimeContext().getMetricGroup().gauge("hello", gauge);


                getRuntimeContext().getMetricGroup().gauge("numGauge", new Gauge<Number>() {
                    @Override
                    public Number getValue() {
                        return 2222;
                    }
                });


                getRuntimeContext().getMetricGroup().gauge("stringGauge", new Gauge<String>() {
                    @Override
                    public String getValue() {
                        return "xjq";
                    }
                });

                tuple3Gauge = new Tuple3Gauge();
                getRuntimeContext().getMetricGroup().gauge("tuple3Gauge", tuple3Gauge);
            }

            @Override
            public String map(String value) throws Exception {
                tuple3Gauge.inc("KeywordOne", 20210810 + index++, 10L);
                if (index % 5 == 0) {
                    tuple3Gauge.clear();
                }

                if (index % 10 == 0) {
                    if (! group.isClosed()) {
                        System.out.println("close metric group: " + group);
                        group.close();
                    }
                    groupClosed = true;
                }

                System.out.println("metric group is closed: " + group.isClosed());
                if (!groupClosed) {
                    counter1.inc(3);
                    counter2.inc(5);
                    gauge1.inc();
                }

                System.out.println(gauge.getValue());
                return value + "after deal";
            }
        }).name("My-Map").print();




        env.execute("test");
    }
}
