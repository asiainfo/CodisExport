package codis;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

public class CodisMain {
	static Logger logger = Logger.getLogger(CodisMain.class);
	public static void main(String[] args) {

		long startTime = System.currentTimeMillis();
		JedisPoolConfig config = new JedisPoolConfig();
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





		JedisPool jedisPool = new JedisPool(config,"ochadoop19", 6379);
		Jedis jedis = jedisPool.getResource();
		Pipeline pipeline = jedis.pipelined();
		Set<String> setKeys = jedis.keys("siteposition:*");
		Object[] keysIter = setKeys.toArray();

		ExecutorService exe =  Executors.newCachedThreadPool();
		executeTask(0,15,jedisPool,setKeys,pipeline,jedis,keysIter,startTime,exe);
	}
	//分段执行任务
	public static void executeTask(Integer start,Integer end,JedisPool jedisPool,Set<String> setKeys,
			Pipeline pipeline,Jedis jedis,Object[] keysIter,long startTime,ExecutorService exe){
		Map<Integer,Integer> map = new HashMap<Integer,Integer>();
		CodisExportDataToFile cet = new CodisExportDataToFile("ochadoop19","codis"+Math.random()+".txt",6379,startTime,start,end,jedisPool,
				setKeys,pipeline,jedis,keysIter);
		Thread t = new Thread(cet);
		t.start();
//		exe.execute(cet);
		start = end;
		end+=15;
		if(end<=setKeys.size()&&start<setKeys.size()){
//			JedisPool jedisPoolNew = new JedisPool("YSZQDJ3", 6380);
//			Jedis jedisNew = jedisPoolNew.getResource();
//			Pipeline pipelineNew = jedis.pipelined();
			executeTask(start, end, jedisPool,setKeys,
					pipeline, jedis,keysIter, startTime, exe);
		}else if(end>setKeys.size()&&start<setKeys.size()){
//			JedisPool jedisPoolNew1 = new JedisPool("YSZQDJ3", 6380);
//			Jedis jedisNew1 = jedisPoolNew1.getResource();
//			Pipeline pipelineNew1 = jedis.pipelined();
			executeTask(start, setKeys.size(), jedisPool,setKeys,
					pipeline, jedis,keysIter, startTime, exe);
		}



	}

}
