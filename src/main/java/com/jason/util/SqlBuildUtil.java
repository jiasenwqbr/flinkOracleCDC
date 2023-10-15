package com.jason.util;

import java.util.Set;

public class SqlBuildUtil {

    public static String buildInsertSql(String tableName, String  cols) {
        String sql = "insert into "+tableName+" (";
        String[] splitCol = cols.split("\\|");
        for (String col : splitCol){
            sql += col+",";
        }
        sql = sql.substring(0,sql.length()-1);

        sql = sql + " ) values (";
        for (String col : splitCol){
            sql+="?,";
        }
        sql = sql.substring(0,sql.length()-1);
        sql = sql + " )";
        return sql;
    }

    public static String buildUpdateSql(String tableName, String cols,String priKey) {
        String[] splitCol = cols.split("\\|");
        String sql = "update " + tableName + " set ";
        for (String col : splitCol){
            sql+=col+"=?,";
        }
        sql = sql.substring(0,sql.length()-1);
        sql = sql+" where "+priKey + "=?";
        return sql;
    }

    public static String buildDelteSql(String tableName, String priKey) {
        String sql = "delete "+ tableName + " where "+priKey + "=?";
        return sql;
    }

    public static String buildInsertLogSql(String logTablename, String cols, String logExtCols) {
        String[] splitCol = cols.split("\\|");
        String[] split = logExtCols.split("\\|");
        String sql = "insert into "+logTablename+" (";
        for (String col : splitCol){
            sql += col+",";
        }
        for (String col : split){
            sql += col+",";
        }
        sql = sql.substring(0,sql.length()-1);

        sql = sql + " ) values (";
        for (String col : splitCol){
            sql+="?,";
        }

        for (String col : split){
            sql+="?,";
        }
        sql = sql.substring(0,sql.length()-1);
        sql = sql + " )";
        return sql;
    }

    public static String buildMergeSql(String tableName, String cols, String priKey) {
        String[] splitCol = cols.split("\\|");
        String sql = "MERGE INTO "+tableName+" \n";
        sql+="USING dual \n";
        sql+= "ON ("+priKey+" = ?) \n";
        sql+=" WHEN MATCHED THEN \n";
        sql+=" UPDATE SET ";
        for (String col : splitCol){
            if (!col.equals(priKey)){
                sql+=col+"=?,";
            }
        }
        sql = sql.substring(0,sql.length()-1);
        sql+="  \n";
        sql+=" WHEN NOT MATCHED THEN \n";
        sql+="INSERT \n" +
                "    ( \n";
        for (String col : splitCol){
            sql+=col+",";
        }
        sql = sql.substring(0,sql.length()-1);
        sql+=") \n";
        sql+=" VALUES(";
        for (String col : splitCol){
            sql+="?,";
        }
        sql = sql.substring(0,sql.length()-1);
        sql+=")";
        return sql;
    }
}
