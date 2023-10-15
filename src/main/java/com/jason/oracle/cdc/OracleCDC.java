package com.jason.oracle.cdc;

import com.alibaba.fastjson.JSONObject;
import com.jason.util.DataType2FlinkUtil;
import com.jason.util.FlinkSqlUtil;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Set;

public class OracleCDC {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String sourceHostname = parameterTool.get("sourceHostname");
        String sourceFlinkTableName = parameterTool.get("sourceFlinkTableName");
        String cols = parameterTool.get("cols");
        String colsType = parameterTool.get("colsType");
        String[] splitCols = cols.split("\\|");
        String[] splitColsType = colsType.split("\\|");
        String sourcePort = parameterTool.get("sourcePort");
        if (sourcePort == null || "".equals(sourcePort)) {
            sourcePort = "1521";
        }
        String cdcUsername = parameterTool.get("cdcUsername");
        String cdcPassword = parameterTool.get("cdcPassword");
        String sourceDatabaseName = parameterTool.get("sourceDatabaseName");
        String sourceSchemaName = parameterTool.get("sourceSchemaName");
        String cdcTableName = parameterTool.get("cdcTableName");
        String bootstrapServer = parameterTool.get("bootstrap.servers");
        String topicId = parameterTool.get("topicId");
        String scanStartupMode = parameterTool.get("scan.startup.mode");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);
        // 间隔10秒 重启3次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(10)));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sql = " CREATE TABLE " + sourceFlinkTableName + " (\n    ";
        for (int i = 0; i < splitCols.length; i++) {
            sql += " " + FlinkSqlUtil.replaceKeyWordInCol(splitCols[i])  + " " + DataType2FlinkUtil.oracleDataType2Flink(splitColsType[i]) + ",";
        }
        sql = sql.substring(0, sql.length() - 1);
        sql += "\n)\n";
        sql +=
                "WITH (\n      " +
                        "'connector' = 'oracle-cdc',\n    " +
                        "'hostname' = '" + sourceHostname + "',\n    " +
                        "'port' = '" + sourcePort + "',\n    " +
                        "'username' = '" + cdcUsername + "',\n    " +
                        "'password' = '" + cdcPassword + "',\n    " +
                        "'database-name' = '" + sourceDatabaseName + "',\n    " +
                        "'schema-name' = '" + sourceSchemaName + "',\n    " +
                        "'table-name' = '" + cdcTableName + "',\n    " +
                        " 'debezium.log.mining.continuous.mine'='true',\n" +
                        " 'debezium.log.mining.strategy'='online_catalog',\n" +
                        " 'debezium.database.tablename.case.insensitive'='false',";
        if (scanStartupMode != null) {
            sql += "'scan.startup.mode'=" + "'" + scanStartupMode + "',";
        }
        sql = sql.substring(0, sql.length() - 1);
        sql += ")";
        //System.out.println(sql);
        tableEnv.executeSql(sql);
        String outputSql = "select * from " + sourceFlinkTableName;
        // TableResult tableResult = tableEnv.executeSql(outputSql);
        // tableResult.print();
        Table table = tableEnv.sqlQuery(outputSql);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name",cdcTableName+"实时");
        //DataSet<Result> ds=  tableEnv.toDataSet(table, Result.class);
        //打印字段结构
        table.printSchema();
        //table 转成 dataStream 流
        SingleOutputStreamOperator<String> outStream = tableEnv.toRetractStream(table, Row.class).map(new MapFunction<Tuple2<Boolean, Row>, String>() {
            @Override
            public String map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
            	System.out.println("oracle log:"+booleanRowTuple2.f1.toString());
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("booleanFlag", booleanRowTuple2.f0);
                jsonObject.put("op", booleanRowTuple2.f1.getKind().name());
                JSONObject data = new JSONObject();
                Set<String> fieldSet = booleanRowTuple2.f1.getFieldNames(true);
                for (String ss : fieldSet) {
                    data.put(ss, booleanRowTuple2.f1.getField(ss));
                }
                jsonObject.put("data", data);
                jsonObject.put("timestamp", getNowTime());
                //System.out.println(jsonObject.toJSONString());
                return jsonObject.toJSONString();
            }
        });
        //sink的参数
        Properties sinkProperties = getPropertyFromArgs(parameterTool);

        outStream.addSink(new SinkOracle(sinkProperties));
        env.execute();
    }


    public static String getNowTime() {
        LocalDateTime localDateTime = LocalDateTime.now();
        return localDateTime.getYear() + "-" + localDateTime.getMonthValue() + "-" + localDateTime.getDayOfMonth() +
                " " + localDateTime.getHour() + ":" + localDateTime.getMinute() + ":" + localDateTime.getSecond() + "." +
                localDateTime.getNano() / 1000;
    }
    public static Properties getPropertyFromArgs(ParameterTool parameterTool){
        Properties sinkProperties = new Properties();
        String sinkDatabaseUrl = parameterTool.get("sinkDatabaseUrl");
        sinkProperties.setProperty("sinkDatabaseUrl", sinkDatabaseUrl);
        String sinkUsername = parameterTool.get("sinkUsername");
        sinkProperties.setProperty("sinkUsername", sinkUsername);
        String sinkPassword = parameterTool.get("sinkPassword");
        sinkProperties.setProperty("sinkPassword", sinkPassword);
        String sinkTableName = parameterTool.get("sinkTableName");
        sinkProperties.setProperty("sinkTableName", sinkTableName);
        String sinkPriKey = parameterTool.get("sinkPriKey");
        sinkProperties.setProperty("sinkPriKey", sinkPriKey);
        String cols = parameterTool.get("cols");
        String colsType = parameterTool.get("colsType");
        sinkProperties.setProperty("cols", cols);
        sinkProperties.setProperty("colsType", colsType);
        String enableLog = parameterTool.get("enableLog");
        sinkProperties.setProperty("enableLog", enableLog);
        if ("1".equals(enableLog)) {
            String logType = parameterTool.get("logType");
            sinkProperties.setProperty("logType", logType);
            String logDatabaseUrl = parameterTool.get("logDatabaseUrl");
            sinkProperties.setProperty("logDatabaseUrl", logDatabaseUrl);
            String logUsername = parameterTool.get("logUsername");
            sinkProperties.setProperty("logUsername", logUsername);
            String logPassword = parameterTool.get("logPassword");
            sinkProperties.setProperty("logPassword", logPassword);
            String logTablename = parameterTool.get("logTablename");
            sinkProperties.setProperty("logTablename", logTablename);
            String logExtCols = parameterTool.get("logExtCols");
            sinkProperties.setProperty("logExtCols", logExtCols);
            String logExtColTypes = parameterTool.get("logExtColTypes");
            sinkProperties.setProperty("logExtColTypes", logExtColTypes);
            String logExtColsPolicy = parameterTool.get("logExtColsPolicy");
            sinkProperties.setProperty("logExtColsPolicy", logExtColsPolicy);
        }
        String insertPolicy = parameterTool.get("insertPolicy");
        sinkProperties.setProperty("insertPolicy", insertPolicy);
        return sinkProperties;
    }

}
