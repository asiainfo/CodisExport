package com.asiainfo.codis.client;

import com.asiainfo.codis.conf.StatisticalTablesConf;
import com.asiainfo.codis.util.CountRowUtils;
import com.asiainfo.codis.util.OutputFileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RecursiveTask;

public class ClientToCodisHelper extends RecursiveTask<Map<String, Map<String, Long>>> {
    private Logger logger = Logger.getLogger(ClientToCodisHelper.class);
    private Object[] keys;
    private JedisPool jedisPool;

    private int start;
    private int end;

    public ClientToCodisHelper(Object[] keys, JedisPool jedisPool, int start, int end) {
        this.keys = keys;
        this.jedisPool = jedisPool;
        this.start = start;
        this.end = end;
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

        if (end - start > StatisticalTablesConf.MAX_ROW_NUM) {
            int mid = (end + start) / 2;

            ClientToCodisHelper left = new ClientToCodisHelper(keys, jedisPool, start, mid);

            ClientToCodisHelper right = new ClientToCodisHelper(keys, jedisPool, mid + 1, end);

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

                rows.addAll(allColumnDataMap.values());

                for (Map.Entry<String, String> entry : allTables.entrySet()) {

                    StringBuilder bs = new StringBuilder();

                    String tableName = entry.getKey();
                    String header = entry.getValue();

                    Map<String, Long> tableCount = result.containsKey(tableName) ? result.get(tableName) : new HashMap();

                    String[] headers = header.split(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR);//all table columns

                    for (String _header : headers) {
                        bs.append(allColumnDataMap.get(_header)).append(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR);
                    }

                    count(tableCount, bs.toString());

                    result.put(tableName, tableCount);
                }

            }

            try {
                pipeline.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            } finally {
                jedisPool.returnResource(jedis);
            }


            String fileName = "codis" + String.valueOf(System.currentTimeMillis()) + "-" + Thread.currentThread().getId() + StatisticalTablesConf.TABLE_FILE_TYPE;

            OutputFileUtils.exportToLocal(fileName, rows);
            OutputFileUtils.exportToHDFS(fileName);

            return result;
        }
    }
}
