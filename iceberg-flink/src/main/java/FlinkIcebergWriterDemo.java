import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple23;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.iceberg.*;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;

public class FlinkIcebergWriterDemo {

    public static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "keyword", Types.StringType.get())
    );

    public static final TableSchema FLINK_SCHEMA = TableSchema.builder()
            .field("id", DataTypes.INT())
            .field("data", DataTypes.STRING())
            .field("keyword", DataTypes.STRING())
            .build();


    public static final Schema TEST_ARRAY_SCHEMA = new Schema(
            Types.NestedField.optional(1, "fields", Types.ListType.ofOptional(2, Types.StringType.get()))
    );

    public static final Schema TEST_TIME_SCHEMA = new Schema(
            Types.NestedField.optional(1, "times", Types.TimestampType.withoutZone())
    );

    public static final Schema TEST_TIMESTAMP_WITH_ZONE_SCHEMA = new Schema(
            Types.NestedField.optional(1, "times", Types.TimestampType.withZone())
    );

    public static final Schema TEST_DATE_SCHEMA = new Schema(
            Types.NestedField.optional(1, "times", Types.DateType.get())
    );

    public static final TableSchema TEST_FLINK_SCHEMA = TableSchema.builder()
            .field("fields", DataTypes.ARRAY(DataTypes.STRING()))
            .build();

    public static final TableSchema All_Flink_TABLE_SCHEMA = TableSchema.builder()
            .field("id", DataTypes.STRING())
            .field("age", DataTypes.INT())
            .field("birth", DataTypes.BIGINT())
            .field("date", DataTypes.DATE())
            .field("timestamp", DataTypes.TIMESTAMP())
            .field("timestamptl", DataTypes.TIMESTAMP_WITH_TIME_ZONE(6))
            .field("double", DataTypes.DOUBLE())
            .field("arr", DataTypes.ARRAY(DataTypes.STRING()))
            .field("map", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
            .build();

    public static final TypeInformation All_typeInfo = TypeInformation.of(
            new TypeHint<Tuple8<String, Integer, Long, LocalDate, LocalDateTime, Double, ObjectArrayTypeInfo, HashMap>>() {
    });


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 构造 RowData 的 DataStream
        GenericRowData row1 = new GenericRowData(3);
        row1.setField(0, 27);
        row1.setField(1, StringData.fromString("i'm xjq"));
        row1.setField(2, StringData.fromString("A"));
        DataStream<RowData> rowDataStream =  environment.fromElements(row1, row1, row1);
        rowDataStream.print();

        DataStream<Row> rowStream = environment.fromElements(
                Row.of(27, "first", "student"),
                Row.of(37, "second", "teacher"));

        DataStream<Row> newRowStream = environment.fromElements(Row.of(299, "after upsert for student", "student"));


        // 1. test Array schema

        /* 1.1 error
        * * row 中不可以包含 list 需要用 Object[] 代替
        *   Caused by: java.lang.ClassCastException: java.util.ArrayList cannot be cast to [Ljava.lang.Object;
        */
        /*
        ArrayList<String> list = new ArrayList<>();
        list.add("AAA");
        list.add("BBBB");
        DataStream<Row> testRowStream = environment.fromElements(Row.of(list));
        */

        /* 1.2 error
        错误，因为 Row.of 会将 字符串对象解析成多个字段，而不是一个 arr 字段；
        DataStream<Row> testRowStream = environment.fromElements(Row.of(new String[]{"AAA", "BBB"}));
        */

        /*
         可以写入
        */

        Row row = new Row(1);
        row.setField(0, new String[]{"AAA", "BBB"});
        DataStream<Row> testRowStream = environment.fromElements(row);

        // 2 写入 TimeStamp 数据

        /* 2.1
        java.time.ZonedDateTime cannot be cast to java.time.LocalDateTime
        DataStream<Row> testRowStream = environment.fromElements(Row.of(ZonedDateTime.now()));
        */

        /* 2.2 ok, 可写可读
        */
        /*
        DataStream<Row> testRowStream = environment.fromElements(Row.of(LocalDateTime.now()));
        */



        // 2 写入 TimeStamp with zone 数据  -> table schema 的 TIMESTAMP_LTZ

        /* 2.1 error
        Caused by: java.lang.ClassCastException: java.time.LocalDateTime cannot be cast to java.time.Instant

        DataStream<Row> testRowStream = environment.fromElements(Row.of(LocalDateTime.now()));
        */

        /* 2.2 error
        Caused by: java.lang.ClassCastException: java.time.ZonedDateTime cannot be cast to java.time.Instant
        DataStream<Row> testRowStream = environment.fromElements(Row.of(ZonedDateTime.now()));
        */

        // 2.3 ok
        /*
        DataStream<Row> testRowStream = environment.fromElements(Row.of(ZonedDateTime.now().toInstant()));
        */


        // 3 写入
        /*
        DataStream<Row> testRowStream = environment.fromElements(Row.of(LocalDateTime.now()));
        */


        // 4 写入 Date
        /*
        DataStream<Row> testRowStream = environment.fromElements(Row.of(LocalDate.now()));
        */




        String localDir = "file:///Users/game-netease/IdeaProjects/flink-common-tools/iceberg-flink/warehouse";
        String rowDataLocalPath = localDir + "/table_a";
        String rowLocalPath = localDir + "/table_row2";
        String testLocalPath = localDir + "/table_test";
        TableLoader rowDataLoader = TableLoader.fromHadoopTable(rowDataLocalPath);
        TableLoader rowLoader = TableLoader.fromHadoopTable(rowLocalPath);
        TableLoader testLoader = TableLoader.fromHadoopTable(testLocalPath);


        // 在 localPath 下创建一张表，如果表已经创建可以省略这一步；
        HashMap<String, String> mapProp1 = new HashMap<>();
        mapProp1.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
        PartitionSpec spec1 = PartitionSpec.builderFor(SCHEMA).identity("keyword").build();
//        Table rowDataTable = new HadoopTables().create(SCHEMA, spec1, mapProp1, rowDataLocalPath);

        HashMap<String, String> mapProp2 = new HashMap<>();
        mapProp2.put("write.metadata.delete-after-commit.enabled", "true");
        mapProp2.put("write.metadata.previous-versions-max", "2");
        PartitionSpec spec2 = PartitionSpec.builderFor(SCHEMA).identity("keyword").build();
//        Table rowTable = new HadoopTables().create(SCHEMA, spec2, mapProp2, rowLocalPath);

        Schema testSchema = TEST_ARRAY_SCHEMA;
        PartitionSpec spec3 = PartitionSpec.builderFor(testSchema).build();
        Table testTable = new HadoopTables().create(testSchema, spec3, mapProp2, testLocalPath);
        TableSchema testTableSchema = FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(testSchema));
        System.out.println(FlinkSchemaUtil.convert(testSchema));
        System.out.println(testSchema);
        System.out.println(testTableSchema);

        // 向表中写入内容
        FlinkSink.forRowData(rowDataStream)
                .tableLoader(rowDataLoader)
                .writeParallelism(1)
                .build();

        FlinkSink.forRow(rowStream, FLINK_SCHEMA)
                .tableLoader(rowLoader)
                .writeParallelism(1)
                .build();

//        覆盖某个分区
//        FlinkSink.forRow(newRowStream, FLINK_SCHEMA)
//                .tableLoader(rowLoader)
//                .writeParallelism(1)
//                .overwrite(true)
//                .build();


        //for test iceberg table
        FlinkSink.forRow(testRowStream, testTableSchema)
                .tableLoader(testLoader)
                .writeParallelism(1)
                .build();

        environment.execute("basic demo");

    }

    public static boolean delFiles(File file){
        boolean result = false;
        //目录
        if(file.isDirectory()){
            File[] childrenFiles = file.listFiles();
            for (File childFile:childrenFiles){
                result = delFiles(childFile);
                if(!result){
                    return result;
                }
            }
        }
        //删除 文件、空目录
        result = file.delete();
        return result;
    }
}
