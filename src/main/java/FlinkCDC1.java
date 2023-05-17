import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author sunshuxian
 * @description
 * @date 2023/5/16
 */
public class FlinkCDC1 {
    public static void main(String[] args) throws Exception {
        // 获取flink的运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        environment.setParallelism(1);

        MySqlSource.Builder<String> builder = MySqlSource.builder();
        DebeziumSourceFunction<String> mysqlSourceFunction = builder.hostname("localhost")
                .port(3306)
                .username("root")
                .password("ZKlHd9ecTl0Z")
                // 监听的库，可以是多个
                .databaseList("test")
                // 监听的表，可以是多个，必须是库名.表名
                .tableList("test.test1")
                // 反序列化器
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                // 所有链接这个数据库实例的链接的唯一值
                .serverId(1)
                .build();
        DataStreamSource<String> stream = environment.addSource(mysqlSourceFunction);
        stream.print();
        environment.execute("FLINK CDC");

    }
}
