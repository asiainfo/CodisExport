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

            ClientToCodisHelper right = new ClientToCodisHelper(keys, jedisPool, mid + 1, end, eventQueue);

            this.invokeAll(left, right);

            try {
                return CountRowUtils.mergeData(left.join(), right.join());
            } catch (ExecutionException | InterruptedException e) {
                logger.error(e.getMessage());
                return result;
            }

        }
        else {

            Jedis jedis = jedisPool.getResource();
            Pipeline pipeline = jedis.pipelined();


            for (int i = start; i <= end; i++) {
                String index = (String) keys[i];
                pipeline.hgetAll(index);
            }

            List<Object> kvList = pipeline.syncAndReturnAll();

            if (kvList == null) {
                return result;
            }

            List<String> rows = new ArrayList();

            for (Object m : kvList) { // go through every row
                Map<String, String> allColumnDataMap = (Map<String, String>) m; // a row in codis, the key is col name

                rows.add(StringUtils.remove(StringUtils.remove(allColumnDataMap.values().toString(), "["), "]")); //value to a row list

                for (Map.Entry<String, CodisTable> entry : allTablesSchema.entrySet()) {// go through every table

                    StringBuilder targetRowKey = new StringBuilder();

                    String tableName = entry.getKey();
                    CodisTable codisTable = entry.getValue();
                    String header = codisTable.getHeader();

                    Map<String, Long> tableCount = result.containsKey(tableName) ? result.get(tableName) : new HashMap();

                    String[] headers = header.split(",");//all table columns

                    Map<String, Boolean> eachAvailability = new HashMap<>();

                    for (String _header : headers) {
                        String colValue = allColumnDataMap.get(_header);

                        if (colValue == null) {
                            colValue = StatisticalTablesConf.EMPTY_VALUE;
                        }

                        Map<String, Condition> conditions = codisTable.getConditions();

                        Condition condition;
                        if (conditions.containsKey(_header)){
                            condition = conditions.get(_header);

                            Context context = new Context(condition);
                            eachAvailability.put(_header, context.matches(colValue));

                        }

                        targetRowKey.append(colValue).append(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR);
                    }

                    if (eachAvailability.isEmpty() || this.isAvailableRow(codisTable.getWhere(), eachAvailability)){
                        count(tableCount, targetRowKey.toString());
                    }

                    result.put(tableName, tableCount);
                }// end of every table

            }//end of a row

            try {
                pipeline.close();

                if (Conf.getBoolean(Conf.EXPORT_FILE_ENABLE, Conf.DEFAULT_EXPORT_FILE_ENABLE)){
                    eventQueue.produceEvent(rows);
                }

            } catch (Exception e) {
                logger.error(e.getMessage());
            } finally {
                jedisPool.returnResource(jedis);
            }

            logger.debug("Result size : " + result.size());
            return result;
        }
    }

    private boolean isAvailableRow(String where, Map<String, Boolean> eachAvailability){
        String[] orList = StringUtils.splitByWholeSeparator(where, Condition.OR);
        boolean result = false;

        for (String orCon : orList){
            if (orCon.contains(Condition.AND)){
                String[] andList = StringUtils.splitByWholeSeparator(orCon, Condition.AND);
                boolean isAllTrue = true;
                for (String andCon : andList){
                    if (eachAvailability.get(orCon) != null && !eachAvailability.get(andCon)){
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

        return result;
    }
}
