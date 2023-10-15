package com.jason.oracle.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jason.util.SqlBuildUtil;
import com.jason.util.SqlExecuteUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.util.Set;

public class SinkOracle extends RichSinkFunction<String> {
    private Connection connection;
    private Connection logConnection;
    private PreparedStatement statement;
    private PreparedStatement logStatement;
    private String sql;
    private String tableName;
    private String priKey;
    //下沉数据库连接url
    private String sinkDatabaseUrl;
    //下沉数据库的用户名
    private String sinkUsername;
    //下沉数据库的密码
    private String sinkPassword;
    //下沉数据库的表名
    private String sinkTableName;
    //下沉数据库的主键名称
    private String sinkPriKey;
    //数据的字段
    private String cols;
    //数据字段类型
    private String colsType;
    //是否启用log
    private String enableLog;
    //日志的数据库连接url
    private String logDatabaseUrl;
    //日志的用户名
    private String logUsername;
    //日志用户的密码
    private String logPassword;
    //日志的表名
    private String logTablename;
    //插入策略
    private String insertPolicy;
    //日志附加字段
    private String logExtCols;
    //日志附加字段类型
    private String logExtColTypes;
    //日志的存储类型
    private String logType;
    private String logExtColsPolicy;

    public SinkOracle() {
    }

    public SinkOracle(Properties sinkProperties) {
        this.sinkDatabaseUrl = sinkProperties.getProperty("sinkDatabaseUrl");
        this.sinkUsername = sinkProperties.getProperty("sinkUsername");
        this.sinkPassword = sinkProperties.getProperty("sinkPassword");
        this.sinkTableName = sinkProperties.getProperty("sinkTableName");
        this.sinkPriKey = sinkProperties.getProperty("sinkPriKey");
        this.cols = sinkProperties.getProperty("cols");
        this.colsType = sinkProperties.getProperty("colsType");
        this.enableLog = sinkProperties.getProperty("enableLog");
        if ("1".equals(enableLog)) {
            this.logDatabaseUrl = sinkProperties.getProperty("logDatabaseUrl");
            this.logUsername = sinkProperties.getProperty("logUsername");
            this.logPassword = sinkProperties.getProperty("logPassword");
            this.logTablename = sinkProperties.getProperty("logTablename");
            this.insertPolicy = sinkProperties.getProperty("insertPolicy");
            this.logExtCols = sinkProperties.getProperty("logExtCols");
            this.logExtColTypes = sinkProperties.getProperty("logExtColTypes");
            this.logType = sinkProperties.getProperty("logType");
            this.logExtColsPolicy = sinkProperties.getProperty("logExtColsPolicy");
        }
        this.insertPolicy = sinkProperties.getProperty("insertPolicy");

    }

    // 1,初始化
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("oracle.jdbc.OracleDriver");
        connection = DriverManager.getConnection(this.sinkDatabaseUrl, this.sinkUsername, this.sinkPassword);
        if ("1".equals(this.enableLog)) {
            logConnection = DriverManager.getConnection(this.logDatabaseUrl, this.logUsername, this.logPassword);
        }

    }

    // 2,执行
    @Override
    public void invoke(String value, Context context) throws Exception {

        //System.out.println("value.toString()-------" + value.toString());
        tableName = this.sinkTableName;
        priKey = this.sinkPriKey;
        JSONObject jsonObject = (JSONObject) JSONObject.parse(value);
        String op = jsonObject.getString("op");
        JSONObject data = jsonObject.getJSONObject("data");
        Set<String> colSet = data.keySet();
        if (op!=null){
        	if ("INSERT".equals(op)) {
                if (this.insertPolicy==null||"insert".equals(this.insertPolicy)){
                    sql = SqlBuildUtil.buildInsertSql(tableName, this.cols);
                    //System.out.println("生成的SQL:" + sql);
                    SqlExecuteUtil.executeInsert(sql, connection, statement, data, this.cols, this.colsType);
                }else if ("merge".equals(this.insertPolicy)){
                    sql = SqlBuildUtil.buildMergeSql(tableName, this.cols,priKey);
                    //System.out.println("生成的SQL:" + sql);
                    SqlExecuteUtil.executeMerge(sql, connection, statement, data, this.cols, this.colsType,priKey);
                }
               

            } else if ("UPDATE_AFTER".equals(op)) {
                sql = SqlBuildUtil.buildUpdateSql(tableName, this.cols, priKey);
                //System.out.println("生成的SQL:" + sql);
                SqlExecuteUtil.executeUpate(sql, connection, statement, data, this.cols, this.colsType, priKey);
            } else if ("DELETE".equals(op)) {
                sql = SqlBuildUtil.buildDelteSql(tableName, priKey);
                //System.out.println("生成的SQL:" + sql);
                SqlExecuteUtil.executeDelete(sql, connection, statement, data, colSet, priKey);
            }
        	 if (this.enableLog.equals("1")) {
                 sql = SqlBuildUtil.buildInsertLogSql(this.logTablename, this.cols, this.logExtCols);
                // System.out.println("生成的LOG SQL:" + sql);
                 SqlExecuteUtil.executeInsertLog(sql, logConnection, logStatement, data, this.cols, this.colsType, this.logExtCols, this.logExtColTypes, this.logExtColsPolicy, op);
             }
            
        }
        

    }

    // 3,关闭
    @Override
    public void close() throws Exception {
        super.close();
        if (statement != null)
            statement.close();
        if (connection != null)
            connection.close();
        if (logStatement != null)
        	logStatement.close();
        if (logConnection != null)
        	logConnection.close();
    }
}