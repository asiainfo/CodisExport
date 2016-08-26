//package codis;
//
//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.ExecutorService;
//
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.JedisPoolConfig;
//import redis.clients.jedis.Pipeline;
//import redis.clients.jedis.Response;
//
//public class Test {
//
//	public static void main(String[] args) {
//		JedisPoolConfig config = new JedisPoolConfig();
//		//连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
//		config.setBlockWhenExhausted(true);
//		//设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
//		config.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy");
//		//是否启用pool的jmx管理功能, 默认true
//		config.setJmxEnabled(true);
//		//MBean ObjectName = new ObjectName("org.apache.commons.pool2:type=GenericObjectPool,name=" + "pool" + i); 默 认为"pool", JMX不熟,具体不知道是干啥的...默认就好.
//		config.setJmxNamePrefix("pool");
//		//是否启用后进先出, 默认true
//		config.setLifo(true);
//		//最大空闲连接数, 默认8个
//		config.setMaxIdle(100);
//		//最大连接数, 默认8个
//		config.setMaxTotal(100);
//		//获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
//		config.setMaxWaitMillis(-1);
//		//逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
//		config.setMinEvictableIdleTimeMillis(1800000);
//		//最小空闲连接数, 默认0
//		config.setMinIdle(0);
//		//每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
//		config.setNumTestsPerEvictionRun(3);
//		//对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断  (默认逐出策略)
//		config.setSoftMinEvictableIdleTimeMillis(1800000);
//		//在获取连接的时候检查有效性, 默认false
//		config.setTestOnBorrow(false);
//		//在空闲时检查有效性, 默认false
//		config.setTestWhileIdle(false);
//		//逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
//		config.setTimeBetweenEvictionRunsMillis(-1);
//
//		long start = System.currentTimeMillis();
//		JedisPool jedisPool = new JedisPool(config,"YSZQDJ3", 6380);
//		Jedis jedis = jedisPool.getResource();
//		Set<String> set = jedis.keys("site_position:*");
//		Object[] keys = set.toArray();
//		Pipeline pipeline = jedis.pipelined();
//		int len = keys.length;
//		executeTask(0,50000,keys,keys.length,pipeline,"codis"+Math.random()+".txt");
//		try {
//			pipeline.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		jedisPool.returnBrokenResource(jedis);
////		jedis.close();
//		jedisPool.close();
//		long end = System.currentTimeMillis();
//		System.out.println("Pipelined hgetall: " + ((end - start)/1000.0) + " seconds");
//
//	}
//
//	public static void executeTask(Integer start,Integer end,Object[] keys,int len,Pipeline pipeline,String filename){
//		FileOutputStream fop = null;
//		File f = new File(Conf.getProp("file_path") + filename);
//		if (!f.exists()) {
//			try {
//				f.createNewFile();
//				fop = new FileOutputStream(f);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		for(int i = start;i<end-1;i++){
//			String index = (String)keys[i];
//			pipeline.hgetAll(index);
//		}
//		List<Object> list = null;
//		try{
//			list = pipeline.syncAndReturnAll();
//		}catch(Exception e){
//			e.printStackTrace();
//		}
//
//
//		for (Object m : list) {
//			Map<String, String> map = (Map<String, String>) m;
//			String dataType = map.get("dataType");
//			String lac_cell = map.get("lac_cell");
//			String islocal = map.get("islocal");
//			String activetime = map.get("activetime");
//			String imsi = map.get("imsi");
//			String intime = map.get("intime");
//			StringBuffer bs = new StringBuffer();
//			bs.append(dataType).append(",").append(lac_cell).append(",")
//					.append(islocal).append(",").append(activetime).append(",")
//					.append(imsi).append(",").append(intime)
//					.append(System.getProperty("line.separator"));
//			byte[] contentInBytes = bs.toString().getBytes();
//			try {
//				if (fop == null) {
//					try {
//						f = new File(Conf.getProp("file_path") + filename);
//						if (!f.exists()) {
//							try {
//								f.createNewFile();
//							} catch (IOException e) {
//								e.printStackTrace();
//							}
//						}
//						fop = new FileOutputStream(f);
//
//					} catch (Exception e) {
//
//					}
//				}
//				fop.write(contentInBytes);
//				fop.flush();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//
//		try {
//			fop.close();
//			fop = null;
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		start = end;
//		end+=50000;
//		 System.out.println("start="+start+",end="+end+",size="+list.size());
//		if(end<=len&&start<len){
//			executeTask(start, end, keys,len,pipeline,"codis"+Math.random()+".txt");
//		}else if(end>len&&start<len){
//			executeTask(start, len, keys,len,pipeline,"codis"+Math.random()+".txt");
//		}
//
//	}
//
//}
