package tableFunction;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class MyMapFunction extends ScalarFunction{

    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.STRING;
    }

    public String eval(String str) {
        System.out.println("调用");
        return str + "tag_xjq";
    }
}
