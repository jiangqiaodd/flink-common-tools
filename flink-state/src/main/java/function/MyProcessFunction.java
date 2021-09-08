package function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class MyProcessFunction extends ProcessFunction<String, String> {

    private transient MapState<Long, Byte[]> mapState;
    private long index = 0;
    private long cleanIndex = 0;
    private boolean first;
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        // 第一种清理方式：通过mapState.remove 可行
        /*
        System.out.printf("clean mapstate.index: %s, cleanIndex: %s, firetime:%d \n",
                index, cleanIndex, timestamp);
        while (cleanIndex < index) {
            mapState.remove(cleanIndex++);
        }
        */

        // 第二种清理方式：通过 map 的 iterator 方法, 也是可以的
        Iterator<Long> iterable = mapState.keys().iterator();
        while (iterable.hasNext()) {
            long value = iterable.next();
            if (value < index) {
                iterable.remove();
                System.out.printf("remove index: %d \t", value);
            }
        }


        // 每 20 s 清理一下 mapState， 处理时间是ok的
//        ctx.timerService().registerProcessingTimeTimer(timestamp + 60 * 1000);

        // 每 20 s 清理一下 mapState， 事件事件则是需要数据源设置了 timeAndWatermark + 使用 EventTimeCharacter
        // (不使用 EventWindow 时， TimeAndWatermark 不会起作用 )
        ctx.timerService().registerEventTimeTimer(timestamp + 60 * 1000);

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Long, Byte[]> mapStateDescriptor = new MapStateDescriptor<Long, Byte[]>(
                "mapstate", Types.LONG, TypeInformation.of(new TypeHint<Byte[]>() {
        }));
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void processElement(String s, Context context, Collector<String> collector) throws Exception {
        Byte[] bytes = new Byte[1024];
        long now = System.currentTimeMillis();
        if (!first) {
            System.out.printf("first timerr on %d\n", now + 20 * 1000);
            context.timerService().registerProcessingTimeTimer(now + 20 * 1000);
            first = true;
        }
        mapState.put(index++, bytes);
        collector.collect(s);
    }
}

