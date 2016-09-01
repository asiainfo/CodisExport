package com.asiainfo.codis;

import codis.Conf;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by peng on 16/8/24.
 */
public class GenerateTestData implements Runnable {
    static Logger logger = Logger.getLogger(GenerateTestData.class);

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    private CountDownLatch countDownLatch;//定义一个静态的CountDownLatch
    JedisPoolConfig config = new JedisPoolConfig();
    //ExecutorService exe =  Executors.newCachedThreadPool();

    private String ip;

    public GenerateTestData(String ip, int port, CountDownLatch countDownLatch) {
        this.ip = ip;
        this.port = port;
        this.countDownLatch = countDownLatch;

        init();

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
        config.setMaxWaitMillis(-1);
        //逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
        config.setMinEvictableIdleTimeMillis(1800000);
        //最小空闲连接数, 默认0
        config.setMinIdle(0);
        //每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
        config.setNumTestsPerEvictionRun(3);
        //对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断  (默认逐出策略)
        config.setSoftMinEvictableIdleTimeMillis(1800000);
        //在获取连接的时候检查有效性, 默认false
        config.setTestOnBorrow(false);
        //在空闲时检查有效性, 默认false
        config.setTestWhileIdle(false);
        //逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
        config.setTimeBetweenEvictionRunsMillis(-1);
    }

    private int port;

    public GenerateTestData() {
    }

    @Override
    public void run() {

        JedisPool jedisPool = new JedisPool(config, ip, port);
        Jedis jedis = jedisPool.getResource();

        int max=9999;
        int min=99;
        Random random = new Random();

        //int s = random.nextInt(max)%(max-min+1) + min;
        //System.out.println(s + "##");
        //long i = 0;
        //long i = 4600092611011800L + port;
        Set<Integer> d = new HashSet<>();

        long iMax = 30;

        SimpleDateFormat dateFm = new SimpleDateFormat("yyyyMMdd"); //格式化当前系统日期
        String dateTime = dateFm.format(new java.util.Date());
        String  timeStamp = String.valueOf(System.currentTimeMillis());

        //System.out.println("The key is " + String.valueOf(i));
        int j = 1;

        while (j <= iMax){

            int s = random.nextInt(max)%(max-min+1) + min;

            if (d.contains(s)){
                System.out.println("==========");
                s = random.nextInt(max)%(max-min+1) + min;
                d.add(s);
            }

            long k = 4600092611000000L + s;

            String key = "siteposition:" + String.valueOf(k);
            HashMap<String, String> values = new HashMap<String, String>();
            values.put("imsi", String.valueOf(k));
            values.put("date",dateTime);
            values.put("TIMESTAMP",timeStamp);
            values.put("area_id","111");
            values.put("province_id","111");
            values.put("cre_id","111");

            jedis.hmset(key, values);

            j++;
        }

        System.out.println("Sum is " + j);

        countDownLatch.countDown();

    }


    public static void main(String[] args) {

        //获取所有的codis集群的slave的ip和port信息，配置到了配置文件里
        String[] codisHostsInfo = Conf.getProp("codisHostsInfo").split(",");

        CountDownLatch countDownLatch;//定义一个静态的CountDownLatch

        //System.out.println("Start time : " + new Date());
        long startTime=System.currentTimeMillis();

        countDownLatch = new CountDownLatch(2);

        //System.out.println("Start to send message...");
        for(String v : codisHostsInfo){
            String[] ipPort = v.split(":");
            new Thread(new GenerateTestData(ipPort[0], Integer.valueOf(ipPort[1]), countDownLatch)).start();
        }

        long firstEndTime=System.currentTimeMillis();
        System.out.println("Finish all messages." + (firstEndTime-startTime)+"ms");


        try {
            countDownLatch.await();   //等待子线程全部执行结束（等待CountDownLatch计数变为0）
            long endTime = System.currentTimeMillis(); //获取结束时间
            System.out.println("End time : " + new Date());
            System.out.println("程序运行时间 " + (endTime-startTime)+"ms");

        } catch (InterruptedException e) {
            System.exit(1);
            e.printStackTrace();
        }

    }

}
