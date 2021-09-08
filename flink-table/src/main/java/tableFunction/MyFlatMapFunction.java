package tableFunction;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class MyFlatMapFunction extends TableFunction<Row> {

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING, Types.LONG);
    }

    public void eval(String x, Long l) {
        collect(Row.of(x + "flatmap_one", l));
        collect(Row.of(x + "flatmap_two", l));
    }
}