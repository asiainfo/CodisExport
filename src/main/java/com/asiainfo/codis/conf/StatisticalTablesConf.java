package com.asiainfo.codis.conf;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class StatisticalTablesConf {
    private static Logger logger = Logger.getLogger(StatisticalTablesConf.class);
    private static String TABLES_CONFIG = "tables.properties";
    private static Properties properties = new Properties();
    private static Map<String, String> allTables = new HashMap<String, String>();

    public final static String TABLE_COLUMN_SEPARATOR = ",";

    public final static String TABLE_IGNORE_HEADER_FLAG = "#";
    public final static String CODIS_KEY_PREFIX = "siteposition";
    public final static String TABLE_FILE_TYPE = ".txt";
    public final static String EMPTY_VALUE = "NA";

    static {
        try {
            properties.load(new FileInputStream("conf" + File.separator + TABLES_CONFIG));

            Iterator it = properties.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry entry=(Map.Entry)it.next();
                allTables.put((String)entry.getKey(), (String)entry.getValue());
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }


    public static String getColumnslHeader(String tableName){
        return properties.getProperty(tableName, "");
    }

    public static Map<String, String> getTables(){
        return allTables;
    }
}
