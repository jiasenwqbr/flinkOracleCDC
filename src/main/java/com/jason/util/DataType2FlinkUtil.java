package com.jason.util;

public class DataType2FlinkUtil {
    public static String oracleDataType2Flink(String type){
        String flinkType = null;
        if (type!=null){
            if("FLOAT".equals(type)){
                flinkType = "FLOAT";
            } else if ("BINARY_FLOAT".equals(type)){
                flinkType = "FLOAT";
            } else if ("BINARY_DOUBLE".equals(type)){
                flinkType = "DOUBLE";
            } else if ("NUMBER(1)".equals(type)){
                flinkType = "BOOLEAN";
            } else if ("DATE".equals(type)){
                flinkType = "TIMESTAMP(3)";
            } else if ("TIMESTAMP".equals(type)){
                flinkType="TIMESTAMP(3)";
            } else if("CHAR".equals(type)){
                flinkType="STRING";
            } else if("NCHAR".equals(type)){
                flinkType="STRING";
            } else if("NVARCHAR2".equals(type)){
                flinkType="STRING";
            } else if("VARCHAR".equals(type)){
                flinkType="STRING";
            } else if("VARCHAR2".equals(type)){
                flinkType="STRING";
            } else if("CLOB".equals(type)){
                flinkType="STRING";
            } else if("NCLOB".equals(type)){
                flinkType="STRING";
            } else if("XMLType".equals(type)){
                flinkType="STRING";
            } else if("BLOB".equals(type)){
                flinkType="BYTES";
            } else if("ROWID".equals(type)){
                flinkType="BYTES";
            } else if(type.contains("NUMBER")){
                if ("NUMBER".equals(type)){
                    flinkType = "DECIMAL(5,0)";
                }else{
                    String str  = type.replace("NUMBER", "").replace("(", "").replace(")", "");
                    String[] split = str.split(",");
                    int p = Integer.valueOf(split[0]);
                    int s = Integer.valueOf(split[1]);
                    if (p-s<3&&s<=0){
                        flinkType = "TINYINT";
                    } else if (p-s<5&&s<=0){
                        flinkType = "SMALLINT";
                    } else if (p - s < 10&&s <= 0){
                        flinkType = "INT";
                    } else if (p - s < 19 && s <= 0){
                        flinkType = "BIGINT";
                    } else if ( p - s <= 38&&p-s>=19&&s<=0 ){
                        flinkType = "DECIMAL("+(p - s)+", 0)";
                    } else if (s>0){
                        flinkType = "DECIMAL("+p +","+s+")";
                    } else if (p - s > 38&&s<=0){
                        flinkType = "STRING";
                    }
                }

            } else if (type.equals("INTERVAL_DAY_TO_SECOND")||type.equals("INTERVAL_YEAR_TO_MONTH")){
                flinkType = "BIGINT";
            }
        }

        return flinkType;
    }

}
