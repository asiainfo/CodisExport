package codis;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class RedisExportDataToFile implements Runnable {
	private static JedisPoolConfig config = new JedisPoolConfig();
	static Logger logger = Logger.getLogger(RedisExportDataToFile.class);
	private String hostname;
	private int port;
	private long start;
	private int startIndex;
	private int endIndex;
	private String filePath;
	private FileSystem fs = null;
	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	private Set<String> set = new HashSet<String>();

	public RedisExportDataToFile(String hostname, int port,
			long start,int startIndex,int endIndex,String filePath,FileSystem fs) {
		this.hostname = hostname;
		this.port = port;
		this.start = start;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.filePath = filePath;
		this.fs = fs;
	}

	// 递归生成key的集合
	public Set<String> getKeys(String startCursor, String pattern, Jedis jedis,
			int count) {
		ScanResult<String> scanResult = jedis.scan(startCursor,
				new ScanParams().match(pattern).count(count));
		String cursor = scanResult.getStringCursor();
		if (!cursor.equals("0")) {
			List<String> keys = scanResult.getResult();
			for (String key : keys) {
				set.add(key);
			}
			getKeys(cursor, pattern, jedis, count);
		}
		return set;
	}

	public void run() {
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
		JedisPool jedisPool = null;
		Jedis jedis = null;
		StringBuffer bs = new StringBuffer();
		//生成文件到hdfs上


    	FSDataOutputStream fsout = null;
    	Pipeline pipeline = null;
		try {
			synchronized(this){
				//先创建文件
				logger.info("传入的filepath="+filePath);
				if(!fs.exists(new Path(filePath))){//先判断该路径是否存在，不存在则创建
			        fs.mkdirs(new Path(filePath));
					logger.info("创建了filepath="+filePath);
				}
				filePath = filePath+"/"+hostname+port+new Random().nextLong()+".csv";
				fsout = fs.create(new Path(filePath));
			}

			jedisPool = new JedisPool(config,hostname, port);
			jedis = jedisPool.getResource();
			Set<String> set = jedis.keys("area_info_:*");
			Object[] keys = set.toArray();
			pipeline = jedis.pipelined();

			//组装数据返回
			executeTask(0,50000,keys,keys.length,pipeline,bs);
			//将所有数据写入文件
			fsout.write(bs.toString().getBytes());
			pipeline.close();
		} catch (Exception e) {
//			logger.info("hostname="+hostname+",port="+port);
			logger.error("ppppp",e);
			e.printStackTrace();
		}finally{
    		try {
    			if(fsout!=null){
    				fsout.close();
    			}

				jedis.close();
				jedisPool.returnBrokenResource(jedis);
				jedisPool.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		long end = System.currentTimeMillis();
		logger.info("--------time=" + (end - start) / 1000 + "s");
	}

	public static StringBuffer executeTask(Integer start,Integer end,Object[] keys,int len,Pipeline pipeline,StringBuffer bs){
		for(int i = start;i<end-1;i++){
			String index = (String)keys[i];
			pipeline.hgetAll(index);
		}
		List<Object> list = null;
		try{
			list = pipeline.syncAndReturnAll();
		}catch(Exception e){
			e.printStackTrace();
		}


		for (Object m : list) {
			Map<String, String> map = (Map<String, String>) m;
			String dataType = map.get("dataType");
			String lac_cell = map.get("lac_cell");
			String islocal = map.get("islocal");
			String activetime = map.get("activetime");
			String imsi = map.get("imsi");
			String intime = map.get("intime");

			bs.append(dataType).append(",").append(lac_cell).append(",")
					.append(islocal).append(",").append(activetime).append(",")
					.append(imsi).append(",").append(intime)
					.append(System.getProperty("line.separator"));

		}

		start = end;
		end+=50000;
		if(end<=len&&start<len){
			executeTask(start, end, keys,len,pipeline,bs);
		}else if(end>len&&start<len){
			executeTask(start, len, keys,len,pipeline,bs);
		}
		return bs;
	}
//	public static void executeTask(Integer start,Integer end,Object[] keys,int len,Pipeline pipeline,String filename,String hostname,int port){
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
//			.append(islocal).append(",").append(activetime).append(",")
//			.append(imsi).append(",").append(intime)
//			.append(System.getProperty("line.separator"));
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
//		System.out.println("start="+start+",end="+end+",size="+list.size());
//		if(end<=len&&start<len){
//			executeTask(start, end, keys,len,pipeline,hostname+"_"+port+Math.random()+".txt",hostname,port);
//		}else if(end>len&&start<len){
//			executeTask(start, len, keys,len,pipeline,hostname+"_"+port+Math.random()+".txt",hostname,port);
//		}
//
//	}




	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}


	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public int getStartIndex() {
		return startIndex;
	}

	public void setStartIndex(int startIndex) {
		this.startIndex = startIndex;
	}

	public int getEndIndex() {
		return endIndex;
	}

	public void setEndIndex(int endIndex) {
		this.endIndex = endIndex;
	}

}
