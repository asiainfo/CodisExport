package com.asiainfo.codis.client;

import codis.Conf;
import com.asiainfo.codis.conf.StatisticalTablesConf;
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
        Map<String, String> allTables = StatisticalTablesConf.getTables();

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

            for (Object m : kvList) {

                Map<String, String> allColumnDataMap = (Map<String, String>) m;

                rows.add(StringUtils.remove(StringUtils.remove(allColumnDataMap.values().toString(), "["), "]"));

                for (Map.Entry<String, String> entry : allTables.entrySet()) {

                    StringBuilder bs = new StringBuilder();

                    String tableName = entry.getKey();
                    String header = entry.getValue();

                    Map<String, Long> tableCount = result.containsKey(tableName) ? result.get(tableName) : new HashMap();

                    String[] headers = header.split(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR);//all table columns

                    boolean isIgnoreRow = false;

                    for (String _header : headers) {
                        String colValue = StringUtils.trimToEmpty(allColumnDataMap.get(_header));
                        if (StringUtils.isEmpty(colValue)) {
                            colValue = StatisticalTablesConf.EMPTY_VALUE;
                        }
//                        if (StringUtils.startsWith(_header, StatisticalTablesConf.TABLE_IGNORE_HEADER_FLAG) && BooleanUtils.toBoolean(colValue)){//TODO
//                            isIgnoreRow = true;
//                            break;
//                        }

                        bs.append(colValue).append(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR);
                    }

                    if (!isIgnoreRow){
                        count(tableCount, bs.toString());
                        result.put(tableName, tableCount);
                    }
                }

            }

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

            return result;
        }
    }
}
