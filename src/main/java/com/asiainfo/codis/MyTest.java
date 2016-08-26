//package com.asiainfo.codis;
//
//import com.fasterxml.jackson.annotation.JsonProperty;
//import io.codis.jodis.JedisResourcePool;
//import io.codis.jodis.RoundRobinJedisPool;
//import org.apache.log4j.Logger;
//import org.apache.log4j.xml.DOMConfigurator;
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.JedisPoolConfig;
//import redis.clients.jedis.Pipeline;
//
//import java.io.File;
//import java.util.Set;
//
///**
// * Created by peng on 16/8/25.
// */
//public class MyTest {
//    public static Logger logger = Logger.getLogger(MyTest.class);
//    public static int MAX_ACTIVE = 1024;
//
//    //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
//    public static int MAX_IDLE = 200;
//
//    //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
//    public static int MAX_WAIT = 10000;
//
//    public static int TIMEOUT = 10000;
//
//    public static int RETRY_NUM = 5;
//
//    @JsonProperty("proxy_addr")
//    private String addr;
//
//    public static void main(String[] args) {
//        String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
//
//        DOMConfigurator.configure(userdir + "log4j.xml");//加载.xml文件
//        JedisPoolConfig config = new JedisPoolConfig();
//        //连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
//        config.setBlockWhenExhausted(true);
//        //设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
//        //config.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy");
//        //是否启用pool的jmx管理功能, 默认true
//        config.setJmxEnabled(true);
//        //MBean ObjectName = new ObjectName("org.apache.commons.pool2:type=GenericObjectPool,name=" + "pool" + i); 默 认为"pool", JMX不熟,具体不知道是干啥的...默认就好.
//        config.setJmxNamePrefix("pool");
//        //是否启用后进先出, 默认true
//        config.setLifo(true);
//        //最大空闲连接数, 默认8个
//        config.setMaxIdle(200);
//        //最大连接数, 默认8个
//        config.setMaxTotal(100);
//        //获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
//        //config.setMaxWaitMillis(-1);
//        config.setMaxWaitMillis(10000);
//        //逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
//        config.setMinEvictableIdleTimeMillis(1800000);
//        //最小空闲连接数, 默认0
//        config.setMinIdle(10);
//        //每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
//        config.setNumTestsPerEvictionRun(3);
//        //对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断  (默认逐出策略)
//        config.setSoftMinEvictableIdleTimeMillis(1800000);
//        //在获取连接的时候检查有效性, 默认false
//        config.setTestOnBorrow(false);
//        //在空闲时检查有效性, 默认false
//        config.setTestWhileIdle(false);
//        //逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
//        config.setTimeBetweenEvictionRunsMillis(-1);
//
//
//        //JedisPoolConfig config = new JedisPoolConfig();
////        config.setMaxActive(MAX_ACTIVE);
////        config.setMaxIdle(MAX_IDLE);
////        config.setMaxWait(MAX_WAIT);
//        config.setTestOnBorrow(true);
//        config.setTestOnReturn(true);
//
//        JedisResourcePool jedisPool = RoundRobinJedisPool.create().poolConfig(config)
//                .curatorClient("ochadoop19:2181", 300000).zkProxyDir("/zk/codis/db_test/proxy/proxy_1").build();
//
//        Jedis jedis = jedisPool.getResource();
//
//            jedis.set("foo", "bar");
//            String value = jedis.get("foo");
//            System.out.println(value);
//
//
//
//        //JedisPool jedisPool = new JedisPool(config,"ochadoop19", 19000);
//        //Jedis jedis = jedisPool.getResource();
//        Pipeline pipeline = jedis.pipelined();
//        Set<String> setKeys = jedis.keys("siteposition:*");
//        //Object[] keysIter = setKeys.toArray();
//
//        logger.info(setKeys);
//    }
//
//}
