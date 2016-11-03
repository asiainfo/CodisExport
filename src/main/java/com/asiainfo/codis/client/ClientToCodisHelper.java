package com.asiainfo.codis.client;

import codis.Conf;
import com.asiainfo.codis.conf.*;
import com.asiainfo.codis.event.EventQueue;
import com.asiainfo.codis.util.CountRowUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RecursiveTask;

public class ClientToCodisHelper extends RecursiveTask<Map<String, Map<String, Long>>> {
    private Logger logger = Logger.getLogger(ClientToCodisHelper.class);
    private Object[] keys;
    private JedisPool jedisPool;

    private String codisAddress;

    private int start;
    private int end;

    private EventQueue eventQueue;

    public ClientToCodisHelper(Object[] keys, JedisPool jedisPool, int start, int end, EventQueue eventQueue) {
        this.keys = keys;
        this.jedisPool = jedisPool;
        this.start = start;
        this.end = end;

        this.eventQueue = eventQueue;
    }

    private void count(Map<String, Long> map, String key) {
        if (map.containsKey(key)) {
            long num = map.get(key) + 1;
            map.put(key, num);
        } else {
            map.put(key, 1l);
        }
    }

    @Override
    protected Map<String, Map<String, Long>> compute() {
        Map<String, Map<String, Long>> result = new HashMap();

        Map<String, CodisTable> allTablesSchema = StatisticalTablesConf.getAllTablesSchema();

        if (end - start > Conf.getInt(Conf.CODIS_EXPORT_MAX_ROW, Conf.DEFAULT_CODIS_EXPORT_MAX_ROW)) {
            int mid = (end + start) / 2;

            ClientToCodisHelper left = new ClientToCodisHelper(keys, jedisPool, start, mid, eventQueue);
            left.setCodisAddress(codisAddress);

            ClientToCodisHelper right = new ClientToCodisHelper(keys, jedisPool, mid + 1, end, eventQueue);
            right.setCodisAddress(codisAddress);

            this.invokeAll(left, right);

            try {
                return CountRowUtils.mergeData(left.join(), right.join());
            } catch (Exception e) {
                logger.error("Merge data failed.", e);
                return result;
            }

        }
        else {
            Jedis jedis = null;
            try {
                long startTime=System.currentTimeMillis();
                jedis = jedisPool.getResource();
                Pipeline pipeline = jedis.pipelined();


                for (int i = start; i <= end; i++) {
                    String index = (String) keys[i];
                    pipeline.hgetAll(index);
                }

                List<Object> kvList = pipeline.syncAndReturnAll();

                long endFromCodisTime = System.currentTimeMillis();
                logger.info("Get '" + kvList.size() + "' data from codis<" + codisAddress + "> taking " + (endFromCodisTime - startTime) + "ms.");
                if (kvList == null) {
                    return result;
                }

                List<String> rows = new ArrayList();

                for (Object m : kvList) { // go through every row
                    Map<String, String> allColumnDataMap = (Map<String, String>) m; // a row in codis, the key is col name

                    //rows.add(StringUtils.remove(StringUtils.remove(allColumnDataMap.values().toString(), "["), "]")); //value to a row list
                    rows.add(handleRow(allColumnDataMap));

                    for (Map.Entry<String, CodisTable> entry : allTablesSchema.entrySet()) {// go through every table

                        StringBuilder targetRowKey = new StringBuilder();

                        String tableName = entry.getKey();
                        CodisTable codisTable = entry.getValue();
                        String header = codisTable.getHeader();

                        Map<String, Long> tableCount = result.containsKey(tableName) ? result.get(tableName) : new HashMap();

                        String[] headers = header.split(",");//all table columns

                        Map<String, Boolean> eachAvailability = new HashMap<>();

                        Map<String, Condition> conditions = codisTable.getConditions();

                        for (String _header : headers) {
                            String colValue = allColumnDataMap.get(_header);

                            if (colValue == null) {
                                colValue = StatisticalTablesConf.EMPTY_VALUE;
                            }

                            Condition condition;
                            if (conditions != null && conditions.containsKey(_header)){
                                condition = conditions.get(_header);
                                condition.setState(true);

                                Context context = new Context(condition);
                                eachAvailability.put(_header, context.matches(colValue));//判断每列条件是否满足
                            }

                            this.handleOtherConditionAvailable(conditions, allColumnDataMap, eachAvailability);

                            targetRowKey.append(colValue).append(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR);
                        }


                        if (eachAvailability.isEmpty() || this.isAvailableRow(codisTable.getWhere(), eachAvailability)){
                            count(tableCount, targetRowKey.toString());
                        }

                        result.put(tableName, tableCount);
                    }// end of every table

                }//end of a row


                pipeline.close();

                if (Conf.getBoolean(Conf.EXPORT_FILE_ENABLE, Conf.DEFAULT_EXPORT_FILE_ENABLE)){
                    logger.info("Ready to write " + rows.size() + " rows.");
                    eventQueue.produceEvent(rows);
                }


                long endTime = System.currentTimeMillis();

                logger.info("Compute data taking " + (endTime - endFromCodisTime) + "ms.");
            } catch (Exception e) {
                logger.error("Unknown error.", e);
            } finally {
                if (jedis != null && jedisPool != null){
                    jedisPool.returnResource(jedis);
                }
            }

            logger.debug("Result size : " + result.size());
            return result;
        }
    }

    private boolean isAvailableRow(String where, Map<String, Boolean> eachAvailability){
        String[] orList = StringUtils.splitByWholeSeparator(where, Condition.OR);
        boolean result = false;

        boolean allAndResult = true;
        if (orList.length == 0){
            for(String key : eachAvailability.keySet()){
                if (!eachAvailability.get(key)){
                    allAndResult = false;
                    break;
                }
            }

            result = allAndResult;
        }
        else {
            for (String orCon : orList){
                if (orCon.contains(Condition.AND)){
                    String[] andList = StringUtils.splitByWholeSeparator(orCon, Condition.AND);
                    boolean isAllTrue = true;
                    for (String andCon : andList){
                        if (eachAvailability.get(andCon) != null && !eachAvailability.get(andCon)){
                            isAllTrue = false;
                            break;
                        }
                    }

                    if (isAllTrue){
                        result = true;
                    }

                }else {
                    if (eachAvailability.get(orCon) != null && eachAvailability.get(orCon)){
                        result = true;
                    }

                }
            }
        }


        return result;
    }

    private void handleOtherConditionAvailable(Map<String, Condition> conditions, Map<String, String> allColumnDataMap, Map<String, Boolean> eachAvailability){
        if (conditions != null){
            for(String key : conditions.keySet()){
                Condition condition = conditions.get(key);
                if (!condition.getState()){
                    logger.debug("Deal with " + condition);
                    String colValue = allColumnDataMap.get(key);

                    if (colValue == null) {
                        colValue = StatisticalTablesConf.EMPTY_VALUE;
                    }
                    Context context = new Context(condition);
                    eachAvailability.put(key, context.matches(colValue));//判断每列条件是否满足
                }
            }
        }
    }

    public void setCodisAddress(String codisAddress) {
        this.codisAddress = codisAddress;
    }

    private String handleRow(Map<String, String> allColumnDataMap){
        String hdfsOutputSchema = Conf.getProp().getProperty(Conf.HDFS_OUTPUT_SCHEMA);
        if (StringUtils.isEmpty(hdfsOutputSchema)){
            logger.error("Invalid hdfs.output.schema value");
            return StringUtils.remove(StringUtils.remove(allColumnDataMap.values().toString(), "["), "]");
        }
        else {
            String[] headers = hdfsOutputSchema.split(",");
            StringBuilder result = new StringBuilder();
            for (String header : headers){
                result.append(allColumnDataMap.get(header.trim()));
                result.append(",");
            }

            if(result.length() > 0){
                return StringUtils.removeEnd(result.toString(), ",");
            }else {
                logger.error("No match hdfs.output.schema data");
                return StringUtils.remove(StringUtils.remove(allColumnDataMap.values().toString(), "["), "]");
            }

        }
    }
}
