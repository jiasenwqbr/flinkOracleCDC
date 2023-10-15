package com.jason.oracle.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {
    //自定义数据解析器
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //获取主题信息,包含着数据库和表名  mysql_binlog_source.gmall-flink.z_user_info
        String topic = sourceRecord.topic();
        String[] arr = topic.split("\\.");
        String db = arr[1];
        String tableName = arr[2];
        //获取操作类型 READ DELETE UPDATE CREATE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);//before空的是因为插入 after是空的是因为删除 修改才是都不为空
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }
        //获取值信息并转换为Struct类型
        Struct value = (Struct) sourceRecord.value();
        //3.获取“befor”数据
        Struct before = value.getStruct("before");//
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema beforeSchema = before.schema();//字段
            List<Field> beforeFields = beforeSchema.fields();//存储在列表
            for (Field field : beforeFields) {  //field 就是 id  name    beforeValue就是id的值，name的值
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }
        }
        //4.获取“after”数据
        Struct after = value.getStruct("after");//
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            Schema afterSchema = after.schema();//字段
            List<Field> afterFields = afterSchema.fields();//存储在列表
            for (Field field : afterFields) {  //field 就是 id  name    afterValue就是id的值，name的值
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            }
        }
        //创建JSON对象用于封装最终返回值数据信息
        JSONObject result = new JSONObject();
        result.put("operation", operation.toString().toLowerCase());
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("database", db);
        result.put("table", tableName);
        //发送数据至下游
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}