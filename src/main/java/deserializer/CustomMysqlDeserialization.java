package deserializer;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import constant.Operate;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import static constant.MysqlBinlogKey.*;


public class CustomMysqlDeserialization implements DebeziumDeserializationSchema {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        JSONObject result = new JSONObject();

        // 主键
        Struct key = (Struct) sourceRecord.key();
        String id = key.get(ID).toString();
        result.put(ID, id);

        Struct value = (Struct) sourceRecord.value();
        Struct source = (Struct) value.get(SOURCE);
        // 库名
        result.put(DB_NAME, source.get(DB));
        // 表名
        result.put(TABLE_NAME, source.get(TABLE));
        // process Time
        result.put(EVENT_TIME, value.get(TS_MS));
        // 事件类型
        result.put(EVENT_TYPE, Operate.getEventType(value.get(OP).toString()));
        // before
        JSONObject beforeJson = new JSONObject();
        if (null != value.get(BEFORE)) {
            Struct before = value.getStruct(BEFORE);
            before.schema().fields().forEach(field -> beforeJson.put(field.name(), before.get(field.name())));
        }
        result.put(BEFORE, beforeJson);

        // after
        JSONObject afterJson = new JSONObject();
        if (null != value.get(AFTER)) {
            Struct after = value.getStruct(AFTER);
            after.schema().fields().forEach(field -> afterJson.put(field.name(), after.get(field.name())));
        }
        result.put(AFTER, afterJson);

        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
