package codis;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class CodisExportDataToFile implements Runnable {
	static Logger logger = Logger.getLogger(CodisExportDataToFile.class);
	private String hostname;
	private String filename;
	private int port;
	private long start;
	private int startIndex;
	private int endIndex;
	private JedisPool jedisPool;
	private Set<String> set = new HashSet<String>();
	private Set<String> setKeys = new HashSet<String>();
	private Pipeline pipeline = null;
	private Jedis jedis = null;
	private Object[] keysIter=null;

	public CodisExportDataToFile(String hostname, String filename, int port,
			long start,int startIndex,int endIndex,JedisPool jedisPool,Set<String> setKeys,
			Pipeline pipeline,Jedis jedis,Object[] keysIter) {
		this.hostname = hostname;
		this.filename = filename;
		this.port = port;
		this.start = start;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.jedisPool = jedisPool;
		this.setKeys = setKeys;
		this.pipeline = pipeline;
		this.jedis = jedis;
		this.keysIter = keysIter;
	}

	// 递归生成key的集合
//	public Set<String> getKeys(String startCursor, String pattern, Jedis jedis,
//			int count) {
//		ScanResult<String> scanResult = jedis.scan(startCursor,
//				new ScanParams().match(pattern).count(count));
//		String cursor = scanResult.getStringCursor();
//		if (!cursor.equals("0")) {
//			List<String> keys = scanResult.getResult();
//			for (String key : keys) {
//				set.add(key);
//			}
//			getKeys(cursor, pattern, jedis, count);
//		}
//		return set;
//	}

	public void run() {
		FileOutputStream fop = null;
//		File f = new File(Conf.getProp("file_path") + filename);
//		if (!f.exists()) {
//			try {
//				f.createNewFile();
//				fop = new FileOutputStream(f);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
		logger.info("startIndex="+startIndex+",endIndex="+endIndex+","+pipeline);
		for(int k=startIndex;k<endIndex-1;k++){
			String index = null;
			try{
				index = (String) keysIter[k];
				pipeline.hgetAll(index);

			}catch(Exception e){
				logger.info("-----------index="+index);
				e.printStackTrace();
				break;
			}

		}

		List<Object> list = null;
		try{
			list = pipeline.syncAndReturnAll();
		}catch(Exception e){
			e.printStackTrace();
		}


//		logger.info(list.size());
		for (Object m : list) {
			Map<String, String> map = (Map<String, String>) m;
			String dataType = map.get("dataType");
			String lac_cell = map.get("lac_cell");
			String islocal = map.get("islocal");
			String activetime = map.get("activetime");
			String imsi = map.get("imsi");
			String intime = map.get("intime");
			StringBuffer bs = new StringBuffer();
			bs.append(dataType).append(",").append(lac_cell).append(",")
					.append(islocal).append(",").append(activetime).append(",")
					.append(imsi).append(",").append(intime)
					.append(System.getProperty("line.separator"));
			byte[] contentInBytes = bs.toString().getBytes();
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
		}
		//
		try {
			fop.close();
			fop = null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			pipeline.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		jedisPool.returnBrokenResource(jedis);
		jedisPool.close();
		long end = System.currentTimeMillis();
		logger.info("--------time=" + (end - start) / 1000 + "s");
	}

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



	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
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
