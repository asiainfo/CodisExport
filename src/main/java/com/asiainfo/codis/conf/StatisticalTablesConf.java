package com.asiainfo.codis.conf;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class StatisticalTablesConf {
    private static Logger logger = Logger.getLogger(StatisticalTablesConf.class);
    private static String TABLES_CONFIG = "tables.json";
    private static Properties properties = new Properties();
    private static Map<String, String> allTables = new HashMap<String, String>();

    private static Map<String, CodisTable> allTablesSchema;

    public final static String TABLE_COLUMN_SEPARATOR = ",";

    public final static String TABLE_IGNORE_HEADER_FLAG = "#";
    public final static String CODIS_KEY_PREFIX = "siteposition";
    public final static String TABLE_FILE_TYPE = ".txt";
    public final static String EMPTY_VALUE = "#NA#";

    public static String getColumnslHeader(String tableName){
        return properties.getProperty(tableName, "");
    }


    public static Map<String, CodisTable> getAllTablesSchema() {
        return allTablesSchema;
    }

    public static Map<String, String> getTables(){
        return allTables;
    }

    public static void init(){
        try {
            Gson gson = new GsonBuilder().create();

            allTablesSchema = gson.fromJson(FileUtils.readFileToString(new File("conf" + File.separator + TABLES_CONFIG)), new TypeToken<Map<String, CodisTable>>() {
            }.getType());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }
}
