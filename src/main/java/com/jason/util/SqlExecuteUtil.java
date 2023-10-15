package com.jason.util;

import com.alibaba.fastjson.JSONObject;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.UUID;

public class SqlExecuteUtil {
    public static void executeInsert(String sql, Connection connection, PreparedStatement statement, JSONObject data, String cols, String colsType) throws SQLException, ParseException {
        statement = connection.prepareStatement(sql);
        int i = 1;
        String[] splitCol = cols.split("\\|");
        String[] splitColsType = colsType.split("\\|");

        for (String col : splitCol) {
            // statement.setString(i,data.getString(col));
            setStatement(i, statement, splitColsType[i - 1], data.getString(col));
            i++;
        }
        statement.execute();
        statement.close();
    }
    public static void executeMerge(String sql, Connection connection, PreparedStatement statement, JSONObject data, String cols, String colsType, String priKey) throws SQLException, ParseException {
        
    	statement = connection.prepareStatement(sql);
        int i = 1;
        String[] splitCol = cols.split("\\|");
        String[] splitColsType = colsType.split("\\|");
        String priKeyType = "VARCHAR2";
        for (int j=0;j<splitCol.length;j++){
            if (priKey.equals(splitCol[j])){
                priKeyType = splitColsType[j];
                break;
            }
        }
        setStatement(i,statement,priKeyType,data.getString(priKey));
        i = i+1;
        int j=0;
        for (String col : splitCol) {
            //statement.setString(i,data.getString(col));
            if (!col.equals(priKey)){
                setStatement(i, statement, splitColsType[j], data.getString(col));
                i++;
            }
            j++;

        }
        int x = 0;
        for (String col : splitCol) {
            // statement.setString(i,data.getString(col));
            setStatement(i, statement, splitColsType[x], data.getString(col));
            i++;
            x++;
        }
        try{
        	 statement.execute();
        }catch(Exception e){
        	System.out.println("Exception data:"+data);
        	System.out.println("Exception Sql:"+sql);
        	e.printStackTrace();
        }finally{
        	statement.close();
        }
        

    }


    public static void executeUpate(String sql, Connection connection, PreparedStatement statement, JSONObject data, String cols, String colsType, String priKey) throws SQLException, ParseException {
        statement = connection.prepareStatement(sql);
        int i = 1;
        String[] splitCol = cols.split("\\|");
        String[] splitColsType = colsType.split("\\|");
        for (String col : splitCol) {
            //statement.setString(i,data.getString(col));
            setStatement(i, statement, splitColsType[i - 1], data.getString(col));
            i++;
        }
        //System.out.println(i);
        //System.out.println(splitCol.length+1);
        statement.setString(splitCol.length + 1, data.getString(priKey));
        statement.execute();
        statement.close();
    }

    public static void executeDelete(String sql, Connection connection, PreparedStatement statement, JSONObject data, Set<String> colSet, String priKey) throws SQLException {
        statement = connection.prepareStatement(sql);
        statement.setString(1, data.getString(priKey));
        statement.execute();
        statement.close();
    }

    public static void executeInsertLog(String sql, Connection connection, PreparedStatement logStatement, JSONObject data, String cols, String colsType, String logExtCols, String logExtColTypes, String logExtColsPolicy, String op) throws SQLException, ParseException {
        logStatement = connection.prepareStatement(sql);
        String[] splitCol = cols.split("\\|");
        String[] splitColsType = colsType.split("\\|");
        int i = 1;
        for (String col : splitCol) {
            setStatement(i, logStatement, splitColsType[i - 1], data.getString(col));
            i++;
        }
        String[] splitExtCols = logExtCols.split("\\|");
        String[] splitExtColsType = logExtColTypes.split("\\|");
        String[] splitExtColPolicy = logExtColsPolicy.split("\\|");
        int j = 1;
        for (String col : splitExtCols) {
            setExtStatement(splitCol.length + j, logStatement, splitExtColPolicy[j-1], splitExtColsType[j-1], op);
            j++;
        }
        logStatement.execute();
        logStatement.close();

    }


    private static void setStatement(int i, PreparedStatement statement, String colType, String colValue) throws SQLException, ParseException {
        if ("FLOAT".equals(colType)) {
            statement.setFloat(i, Float.valueOf(colValue));
        } else if ("BINARY_FLOAT".equals(colType)) {
            statement.setFloat(i, Float.valueOf(colValue));
        } else if ("DATE".equals(colType)) {
            // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
            if (colValue != null && !"".equals(colType)) {
                //statement.setDate(i, new java.sql.Date(sdf.parse(colValue).getTime()) );
//            	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            	String date_time = formatter.format(new Date(Long.valueOf(colValue)));
//            	java.util.Date times=formatter.parse(date_time);
//            	
//            	Instant instant = times.toInstant();
//            	ZoneId zoneId = ZoneId.of("Asia/Shanghai");
//            	Instant instant1 = instant.atZone(zoneId).toInstant();
//            	//Instant instant1 = localDateTime.toInstant();
//            	long timeFromLocal1 = instant1.toEpochMilli();
//
//                statement.setDate(i, new java.sql.Date(timeFromLocal1));
            	//System.out.println("colValue:"+colValue);
            	colValue = colValue.replaceAll("T"," ");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm");
                if (colValue.length()==16){
                	sdf =  new SimpleDateFormat("yyyy-MM-dd hh:mm");
                } else if (colValue.length()==19){
                	sdf =  new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                } else if (colValue.length()>19){
                	sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
                }
                java.util.Date d = sdf.parse(colValue);
                statement.setDate(i, new java.sql.Date(d.getTime()));
                
            } else {
                statement.setDate(i, null);
            }
        } else if ("TIMESTAMP".equals(colType)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
            if (colValue != null && !"".equals(colValue)) {
                statement.setTimestamp(i, new Timestamp(sdf.parse(colValue).getTime()));
            } else {
                statement.setTimestamp(i, null);
            }
        } else if ("CHAR".equals(colType)) {
            statement.setString(i, colValue);
        } else if ("NCHAR".equals(colType)) {
            statement.setString(i, colValue);
        } else if ("NVARCHAR2".equals(colType)) {
            statement.setString(i, colValue);
        } else if ("VARCHAR".equals(colType)) {
            statement.setString(i, colValue);
        } else if ("VARCHAR2".equals(colType)) {
            statement.setString(i, colValue);
        } else if ("CLOB".equals(colType)) {
            
            statement.setString(i, colValue);


        } else if ("NCLOB".equals(colType)) {
            statement.setString(i, colValue);
        } else if ("XMLType".equals(colType)) {
            statement.setString(i, colValue);
        } else if ("BLOB".equals(colType)) {
            //statement.setString(i,colValue);
            if (colValue != null && !"".equals(colValue)) {
                statement.setBytes(i, colValue.getBytes());
            } else {
                statement.setBytes(i, null);
            }

        } else if ("ROWID".equals(colType)) {
            //statement.setString(i,colValue);
            if (colValue != null && !"".equals(colValue)) {
                statement.setBytes(i, colValue.getBytes());
            } else {
                statement.setBytes(i, null);
            }
        } else if (colType.contains("NUMBER")) {
            if (colValue != null && !"".equals(colValue)) {
                if ("NUMBER".equals(colType)) {
                    statement.setInt(i, Integer.valueOf(colValue));
                } else {
                    String str = colType.replace("NUMBER", "").replace("(", "").replace(")", "");
                    String[] split = str.split(",");
                    int p = Integer.valueOf(split[0]);
                    int s = Integer.valueOf(split[1]);
                    if (p - s < 3 && s <= 0) {
                        statement.setInt(i, Integer.valueOf(colValue));
                    } else if (p - s < 5 && s <= 0) {
                        statement.setInt(i, Integer.valueOf(colValue));
                    } else if (p - s < 10 && s <= 0) {
                        statement.setInt(i, Integer.valueOf(colValue));
                    } else if (p - s < 19 && s <= 0) {
                        statement.setInt(i, Integer.valueOf(colValue));
                    } else if (p - s <= 38 && p - s >= 19 && s <= 0) {
                        statement.setDouble(i, Double.valueOf(colValue));
                    } else if (s > 0) {
                        statement.setDouble(i, Double.valueOf(colValue));
                    } else if (p - s > 38 && s <= 0) {
                        statement.setDouble(i, Double.valueOf(colValue));
                    }
                }
            } else {
                statement.setInt(i, 0);
            }
        }
    }

    private static void setExtStatement(int i, PreparedStatement logStatement, String colPolicy, String colType, String op) throws SQLException {
        if ("UUID".equals(colPolicy)) {
            logStatement.setString(i, UUID.randomUUID().toString());
        } else if ("SYSDATE".equals(colPolicy)) {
            logStatement.setDate(i, new java.sql.Date(new java.util.Date().getTime()));
        } else if ("OP".equals(colPolicy)) {
            logStatement.setString(i, op);
        }
    }



}
