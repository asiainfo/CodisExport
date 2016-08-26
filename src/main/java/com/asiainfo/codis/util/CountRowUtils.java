package com.asiainfo.codis.util;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by peng on 16/8/25.
 */
public class CountRowUtils {

    private static Logger logger = Logger.getLogger(CountRowUtils.class);

    public static Map<String, Map<String, Long>> mergeData(Map<String, Map<String, Long>> leftResult, Map<String, Map<String, Long>> rightResult) throws ExecutionException, InterruptedException {

        HashMap<String, Map<String, Long>> finalResult = new HashMap();


        for (Map.Entry entry : leftResult.entrySet()) {
            String tableName = (String) entry.getKey();
            Map<String, Long> values = (Map<String, Long>) entry.getValue();
            Map<String, Long> map = finalResult.containsKey(tableName) ? finalResult.get(tableName) : new HashMap();

            for (Map.Entry<String, Long> _entry : values.entrySet()) {
                String key = _entry.getKey();
                Long num = _entry.getValue();

                if (map.containsKey(key)) {
                    map.put(key, map.get(key) + num);
                } else {
                    map.put(key, num);
                }

            }


            finalResult.put(tableName, map);
        }

        for (Map.Entry entry : rightResult.entrySet()) {
            String tableName = (String) entry.getKey();
            Map<String, Long> values = (Map<String, Long>) entry.getValue();
            Map<String, Long> map = finalResult.containsKey(tableName) ? finalResult.get(tableName) : new HashMap();

            for (Map.Entry<String, Long> _entry : values.entrySet()) {
                String key = _entry.getKey();
                Long num = _entry.getValue();

                if (map.containsKey(key)) {
                    map.put(key, map.get(key) + num);
                } else {
                    map.put(key, num);
                }

            }


            finalResult.put(tableName, map);
        }


        //logger.info("Reult : " + finalResult);

        return finalResult;
    }
}
