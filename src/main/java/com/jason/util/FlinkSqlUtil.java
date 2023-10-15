package com.jason.util;

public class FlinkSqlUtil {
    public static String replaceKeyWordInCol(String col){
        if (col.toUpperCase().equals("TIME")){
            return "`"+col+"`";
        } else if (col.toUpperCase().equals("RESULT")){
            return "`"+col+"`";
        }
        //RESULT
        return col;
    }
}
