package com.asiainfo.codis.client;

import codis.Conf;
import com.asiainfo.codis.conf.StatisticalTablesConf;
import com.asiainfo.codis.event.EventQueue;
import com.asiainfo.codis.util.CountRowUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;
import java.util.concurrent.*;

import org.apache.log4j.Logger;


public class ClientToCodis extends RecursiveTask<Map<String, Map<String, Long>>> {
    private Logger logger = Logger.getLogger(ClientToCodis.class);

    private String[] codisHostsInfo;
    private JedisPoolConfig config;

    private int start;
    private int end;

    private ForkJoinPool pool;
    private EventQueue eventQueue;


    public ClientToCodis(String[] codisHostsInfo, ForkJoinPool pool, EventQueue eventQueue) {
        this.codisHostsInfo = codisHostsInfo;
        init();

        start = 0;
        end = codisHostsInfo.length - 1;

        this.pool = pool;
        this.eventQueue = eventQueue;
    }

    public ClientToCodis(String[] codisHostsInfo, int start, int end, ForkJoinPool pool, EventQueue eventQueue) {
        this.codisHostsInfo = codisHostsInfo;
        this.start = start;
        this.end = end;

        this.pool = pool;
        this.eventQueue = eventQueue;
    }


    @Override
    protected Map<String, Map<String, Long>> compute() {
        Map<String, Map<String, Long>> result = new HashMap();

        if (end != start){
            int mid = (end + start) / 2;

            ClientToCodis left = new ClientToCodis(codisHostsInfo, start, mid, pool, eventQueue);

            ClientToCodis right = new ClientToCodis(codisHostsInfo, mid + 1, end, pool, eventQueue);

            this.invokeAll(left, right);

            try {
                return CountRowUtils.mergeData(left.join(), right.join());
            } catch (Exception e) {
                logger.error("Merge data failed.", e);
                return result;
            }
        }
        else {
            init();

            String[] ip_port = codisHostsInfo[start].split(":");
            if (ip_port.length != 2){
                logger.error("Codis info is invalid.");
                return result;
            }

            try {

                JedisPool jedisPool = new JedisPool(config, ip_port[0].trim(), Integer.valueOf(ip_port[1].trim()),
                        Conf.getInt(Conf.JEDIS_TIMEOUT_MS, Conf.DEFAULT_JEDIS_TIMEOUT_MS));

                Jedis jedis = jedisPool.getResource();

                long startTime=System.currentTimeMillis();
                Set<String> set = jedis.keys(Conf.getProp().getProperty(StatisticalTablesConf.CODIS_KEY_PREFIX, StatisticalTablesConf.DEFAULT_CODIS_KEY_PREFIX) + ":*");
                Object[] keys = set.toArray();

                int keyNum = keys.length;

                long endTime = System.currentTimeMillis();
                logger.info("Get '" + keyNum + "' keys taking " + (endTime - startTime) + "ms from " + codisHostsInfo[start] + ".");

                ClientToCodisHelper clientToCodisHelper = new ClientToCodisHelper(keys, jedisPool, 0, keyNum-1, eventQueue);
                clientToCodisHelper.setCodisAddress(codisHostsInfo[start]);

                //ForkJoinPool pool = new ForkJoinPool(Conf.getInt(Conf.CODIS_CLIENT_THREAD_COUNT, Conf.DEFAULT_CODIS_CLIENT_THREAD_COUNT));
                ForkJoinTask<Map<String, Map<String, Long>>> finalResult = pool.submit(clientToCodisHelper);

                result = finalResult.join();


                if (finalResult.getException() != null){
                    logger.error(finalResult.getException());
                }


                jedisPool.close();
            }
            catch (Exception e){
                logger.error("Unknown error.", e);
            }

            return result;

        }

    }

    private void init(){
        config = new JedisPoolConfig();
        //连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
        config.setBlockWhenExhausted(true);
        //设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
        config.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy");
        //是否启用pool的jmx管理功能, 默认true
        config.setJmxEnabled(true);
        //MBean ObjectName = new ObjectName("org.apache.commons.pool2:type=GenericObjectPool,name=" + "pool" + i); 默 认为"pool", JMX不熟,具体不知道是干啥的...默认就好.
        config.setJmxNamePrefix("pool");
        //是否启用后进先出, 默认true
        config.setLifo(true);
        //最大空闲连接数, 默认8个
        config.setMaxIdle(100);
        //最大连接数, 默认8个
        config.setMaxTotal(100);
        //获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
        config.setMaxWaitMillis(10000);
        //逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
        config.setMinEvictableIdleTimeMillis(1800000);
        //最小空闲连接数, 默认0
        config.setMinIdle(0);
        //每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
        config.setNumTestsPerEvictionRun(3);
        //对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断  (默认逐出策略)
        config.setSoftMinEvictableIdleTimeMillis(1800000);
        //在获取连接的时候检查有效性, 默认false
        config.setTestOnBorrow(true);
        //在空闲时检查有效性, 默认false
        config.setTestWhileIdle(true);
        //逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
        config.setTimeBetweenEvictionRunsMillis(-1);


    }
}
