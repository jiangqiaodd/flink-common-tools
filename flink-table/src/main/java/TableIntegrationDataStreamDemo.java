import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import pojo.Student;

public class TableIntegrationDataStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(environment);

        DataStream dataStream = environment.fromElements(new Tuple2<>("xjq", 26), new Tuple2<>("xxx", 24));


        // 1. DataStream -> Table

        // 1.1 DataStream -> VIEW: 注册后，可以在 sql 语句中使用
        streamTableEnvironment.createTemporaryView("input_A", dataStream); // default field as f0, f1
        streamTableEnvironment.createTemporaryView("input_B", dataStream, Expressions.$("field_1"));
        // 使用例如： tableEnv.executeSql("select * from input_A")

        // 1.2 DataStream -> Table： 转换成 Table 后，可以使用 Table API
        // 如果设置的，字段名字少了，会只提取这部分的内容，作为 schema
        Table tableA = streamTableEnvironment.fromDataStream(dataStream);
        Table tableB = streamTableEnvironment.fromDataStream(dataStream, Expressions.$("field_1"));
        tableA.printSchema();
        tableB.printSchema();
        /*
        * root
         |-- f0: STRING
         |-- f1: INT

        root
         |-- field_1: STRING
        *  */

        // 2. Table -> DataStream
        DataStream res1 = streamTableEnvironment.toAppendStream(tableA, Row.class);
        System.out.println(res1.getType());

        DataStream res2 = streamTableEnvironment.toRetractStream(tableA, Row.class);
        System.out.println(res2.getType());

        DataStream res3 = streamTableEnvironment.toAppendStream(tableA, new TupleTypeInfo<>(Types.STRING(), Types.INT()));
        System.out.println(res3.getType());
        /*
        *
        *   Row(f0: String, f1: Integer)
            Java Tuple2<Boolean, Row(f0: String, f1: Integer)>
            Java Tuple2<String, Integer>
        *  */

/*
        res1.print();
        xjq,26
        xxx,24
        */


/*
        res2.print();
        (true,xjq,26)
        (true,xxx,24)
        */

/*
        res3.print();
        (xjq,26)
        (xxx,24)
       */

        // 3. DataType <> Table Schema
        DataStream<Student> studentStream = environment.fromElements(new Student("xx", 20),
                new Student("xu", 26));
        Table studentTable = streamTableEnvironment.fromDataStream(studentStream);
        studentTable.printSchema();
        /*
        * root
             |-- age: INT
             |-- name: STRING
        *  */


        environment.execute("ss");

    }


}
