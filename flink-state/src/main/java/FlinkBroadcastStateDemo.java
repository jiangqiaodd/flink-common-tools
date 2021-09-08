import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class FlinkBroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<String> src = environment.addSource(new SourceFunction<String>() {

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect("hello xjq");
                    Thread.sleep(1000 * 2);
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(1);


        MapStateDescriptor<String, Boolean> blackPoolDescriptor = new MapStateDescriptor<String, Boolean>(
                "blackPoolState",
                org.apache.flink.api.common.typeinfo.Types.STRING,
                org.apache.flink.api.common.typeinfo.Types.BOOLEAN
        );
        BroadcastStream<Tuple2<String, Boolean>> binlogStream =
                environment.fromElements(new Tuple2<>("haha", false)).broadcast(blackPoolDescriptor);

        DataStream<Object> out = src.keyBy(String::toString).connect(binlogStream).process(

                new KeyedBroadcastProcessFunction<String, String, Tuple2<String, Boolean>, Object>() {
                    //用于触发删除记录
                    MapStateDescriptor<String, Boolean> blackPoolBroadcastStateDesc;
                    MapStateDescriptor<String, Boolean> generalMapStateDesc;
                    private boolean first = true;
                    private transient BroadcastState<String, Boolean> blackPoolBroadcastState;
                    private transient BroadcastState<String, Long> strLongState;
                    private transient MapState<String, Boolean> generalMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        blackPoolBroadcastStateDesc = new MapStateDescriptor("blackPoolState", Types.STRING, Types.BOOLEAN);
                        generalMapStateDesc = new MapStateDescriptor("blackPoolState", Types.STRING, Types.BOOLEAN);

                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(Time.seconds(10))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        blackPoolBroadcastStateDesc.enableTimeToLive(ttlConfig);
                        generalMapStateDesc.enableTimeToLive(ttlConfig);

                        generalMapState = getRuntimeContext().getMapState(generalMapStateDesc);


                    }

                    @Override
                    public void processElement(String s, ReadOnlyContext readOnlyContext, Collector<Object> collector) throws Exception {
                        System.out.println("blackPoolBroadcastState:" + blackPoolBroadcastState);
                        if (blackPoolBroadcastState != null) {
                            System.out.println("blackPoolBroadcastState:" + blackPoolBroadcastState.get("haha"));
                        }
                        System.out.println("generalMapState:" + generalMapState.get("haha"));
                        collector.collect(s);

                        if (first) {
                            readOnlyContext.timerService().registerProcessingTimeTimer(readOnlyContext.currentProcessingTime() + 20 * 1000);
                            first = false;
                        }
                    }

                    @Override
                    public void processBroadcastElement(Tuple2<String, Boolean> value, Context context, Collector<Object> collector) throws Exception {
                        blackPoolBroadcastState = context.getBroadcastState(blackPoolBroadcastStateDesc);
                        blackPoolBroadcastState.put(value.f0, true);
                        generalMapState.put(value.f0, true);
                        System.out.println("process broadcast element:" + value);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                        blackPoolBroadcastState.remove("haha");
                    }
                }).setParallelism(1);
        out.print().setParallelism(1);


        environment.setParallelism(1);
        environment.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        environment.execute("demo");

    }
}
