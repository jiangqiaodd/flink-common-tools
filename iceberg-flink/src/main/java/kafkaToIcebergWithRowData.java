import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class kafkaToIcebergWithRowData {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        String topic = "test_iceberg_input";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "test01.brokers.canal.netease.com:9092");
        properties.setProperty("group.id", "test_xjq");
//        properties.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer(topic, new SimpleStringSchema(), properties);
//        consumer.setStartFromTimestamp(1623811180);
        consumer.setStartFromEarliest();

        DataStream<String> dataStream = environment.addSource(consumer);
//        dataStream.print();


        DataStream<RowData> rowStream = dataStream.map(new MapFunction<String, RowData>() {
            @Override
            public RowData map(String s) throws Exception {
                Row row = new Row(3);
                row.setField(0, "l17");
                row.setField(1, "DecoderUse");
                row.setField(2, 1623320097);

                String[] arr = new String[]{"account_id","decoder","left_decoder","mac_addr","old_account_id","reason"};
                List<String> list = Arrays.asList(arr);

                StringData[] stringDataArr = new StringData[]{
                        StringData.fromString("account_id"),
                        StringData.fromString("decoder"),
                        StringData.fromString("left_decoder"),
                        StringData.fromString("mac_addr"),
                        StringData.fromString("old_account_id"),
                        StringData.fromString("reason"),
                };

//              RowData rowData = GenericRowData.of("l17", "DecoderUse", 1623320097000L, list);
                GenericRowData rowData = new GenericRowData(4);
                rowData.setField(0, StringData.fromString("l17"));
                rowData.setField(1, StringData.fromString("DecoderUse"));
                rowData.setField(2, 1623320097000L);
                rowData.setField(3, new GenericArrayData(stringDataArr));
                Thread.sleep(10000);
                return rowData;

            }
        });
        rowStream.print();


        String localPath = "file:///Users/game-netease/IdeaProjects/flink-common-tools/iceberg-flink/warehouse/table_schema";
        TableLoader tableLoader = TableLoader.fromHadoopTable(localPath);

        // iceberg table 的表结构, 适用于使用 RowData 的流
        Schema schema = new Schema(
                Types.NestedField.optional(1, "project", Types.StringType.get()),
                Types.NestedField.optional(2, "log_type", Types.StringType.get()),
                Types.NestedField.optional(3, "timestamp", Types.LongType.get()),
                Types.NestedField.optional(4, "fields", Types.ListType.ofOptional(5, Types.StringType.get()))
        );
        System.out.println(schema);


        HashMap<String, String> mapProp = new HashMap<>();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = new HadoopTables().create(schema, spec, mapProp, localPath);

        FlinkSink.forRowData(rowStream)
                .tableLoader(tableLoader)
                .distributionMode(DistributionMode.NONE)
                .writeParallelism(1)
                .build();


        environment.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
        environment.execute("consumer print");
    }
}
