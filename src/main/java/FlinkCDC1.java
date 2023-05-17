import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * -- 第一次全量同步日志  SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={ts_sec=1684331590, file=binlog.000234, pos=156}} ConnectRecord{topic='mysql_binlog_source.test.test1', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{mysql_binlog_source.test.test1.Key:STRUCT}, value=Struct{after=Struct{id=1,job_id=1,job_name=test_1,status=2},source=Struct{version=1.5.2.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1684331590609,snapshot=last,db=test,table=test1,server_id=0,file=binlog.000234,pos=156,row=0},op=r,ts_ms=1684331590611}, valueSchema=Schema{mysql_binlog_source.test.test1.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
 * -- 插入日志   SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1684331762, file=binlog.000234, pos=235, row=1, server_id=1, event=2}} ConnectRecord{topic='mysql_binlog_source.test.test1', kafkaPartition=null, key=Struct{id=2}, keySchema=Schema{mysql_binlog_source.test.test1.Key:STRUCT}, value=Struct{after=Struct{id=2,job_id=2,job_name=test_2,status=2},source=Struct{version=1.5.2.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1684331762000,db=test,table=test1,server_id=1,file=binlog.000234,pos=369,row=0},op=c,ts_ms=1684331762243}, valueSchema=Schema{mysql_binlog_source.test.test1.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
 * -- 更新日志   SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1684331786, file=binlog.000234, pos=538, row=1, server_id=1, event=2}} ConnectRecord{topic='mysql_binlog_source.test.test1', kafkaPartition=null, key=Struct{id=2}, keySchema=Schema{mysql_binlog_source.test.test1.Key:STRUCT}, value=Struct{before=Struct{id=2,job_id=2,job_name=test_2,status=2},after=Struct{id=2,job_id=2,job_name=test_2,status=1},source=Struct{version=1.5.2.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1684331786000,db=test,table=test1,server_id=1,file=binlog.000234,pos=681,row=0},op=u,ts_ms=1684331786563}, valueSchema=Schema{mysql_binlog_source.test.test1.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
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

        Properties properties = new Properties();
        properties.put("database.allowPublicKeyRetrieval", "true");
        properties.put("database.useSSL", "true");


        MySqlSource.Builder<String> builder = MySqlSource.builder();
        DebeziumSourceFunction<String> mysqlSourceFunction = builder
                .hostname("localhost")
                .port(3306)
                .username("root")
                //window密码
//                .password("ZKlHd9ecTl0Z")
                // mac密码
                .password("L2Bs9fD#")
                // 监听的库，可以是多个
                .databaseList("test")
                // 监听的表，可以是多个，必须是库名.表名
                .tableList("test.test1")
                // 反序列化器
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                // 所有链接这个数据库实例的链接的唯一值
                .serverId(1)
                .serverTimeZone("UTC")
                .debeziumProperties(properties)
                .build();
        DataStreamSource<String> stream = environment.addSource(mysqlSourceFunction);
        stream.print();
        environment.execute("FLINK CDC");


    }
}
