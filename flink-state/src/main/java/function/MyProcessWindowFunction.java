package function;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class MyProcessWindowFunction extends ProcessWindowFunction<String, String ,String, TimeWindow> {

    private transient MapState<Long, Byte[]> mapState;
    private long index = 0;
    private long cleanIndex = 0;
    private boolean first;


    public MyProcessWindowFunction() {
        super();
    }

    @Override
    public void process(String s, Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
            collector.collect(iterable.iterator().next());
    }
}

