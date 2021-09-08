package function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;

public class MyRichMapFunction extends RichMapFunction<String, String > {

    private MapState<String, Byte[]> mapState;

    @Override
    public String map(String value) throws Exception {
        Byte[] bytes = new Byte[1024];
        mapState.put(value, bytes);
        return value;
    }
}
