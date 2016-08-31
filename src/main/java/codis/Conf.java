package codis;

import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.Properties;

public class Conf {
	
	private static String CONFIG_FILE = "ocdc.properties";
	private static String TABLES_CONFIG = "tables.properties";
	
	private static String MSN_CN_FILE = "msn_cn.properties";
	private static String MSN_EN_FILE = "msn_en.properties";
	
	private static String DB_TYPE = "db.type";
	private static String DB_USERNAME = "db.username";
	private static String DB_PASSWORD = "db.password";
	private static String DB_URL = "db.url";
	private static String DB_POOL_MAX_SIZE = "db.pool.max_size";
	private static String DB_POOL_MIN_SIZE = "db.pool.min_size";
	
	private static String HIVE2 = "hive2";
	private static String HIVE_METASTORE = "hivemetastore";
	
	private static String HDFS_URI = "hdfs_uri";
	
	private static String WEB_NAME = "";
	private static Properties properties = new Properties();
	private static Properties msn_cn= new Properties();
	private static Properties msn_en= new Properties();

	public final static String CODIS_CLIENT_THREAD_COUNT = "codis.client.thread-count";
	public final static int DEFAULT_CODIS_CLIENT_THREAD_COUNT = 8;
//	public static void init(){
	static {
		try {
			InputStream inputStream = Conf.class.getClassLoader().getResourceAsStream(CONFIG_FILE);

			//InputStream tablesInputStream = Conf.class.getClassLoader().getResourceAsStream(TABLES_CONFIG);

			System.out.println(new File("conf" + File.separator + CONFIG_FILE).getAbsolutePath());
		
			if (inputStream == null){
			      //throw new RuntimeException(CONFIG_FILE + " not found in classpath");
			}else{
				  properties.load(new FileInputStream("conf" + File.separator + CONFIG_FILE));
				  inputStream.close();
			}
		} catch (FileNotFoundException fnf) {
	    	//throw new RuntimeException("No configuration file " + CONFIG_FILE + " found in classpath.", fnf);
	    } catch (IOException ie) {
	    	//throw new IllegalArgumentException("Can't read configuration file " + CONFIG_FILE, ie);
	    }
		
		try {
			InputStream inputStream = Conf.class.getClassLoader().getResourceAsStream(MSN_CN_FILE);
			if (inputStream == null){
			}else{
				msn_cn.load(inputStream);
				inputStream.close();
			}
	    } catch (IOException ie) {
	    	ie.printStackTrace();
	    }
		
		try {
			InputStream inputStream = Conf.class.getClassLoader().getResourceAsStream(MSN_EN_FILE);
			if (inputStream == null){
			}else{
				msn_en.load(inputStream);
				inputStream.close();
			}
	    } catch (IOException ie) {
	    	ie.printStackTrace();
	    }
	}

	public static int getInt(String name, int defaultValue) {
		String valueString = StringUtils.trim(properties.getProperty(name));
		if (StringUtils.isEmpty(valueString))
			return defaultValue;

		return Integer.parseInt(valueString);
	}
	

	public static String getDbType(){
		return properties.getProperty(DB_TYPE, "derby");
	}
	public static String getDbUsername(){
		return properties.getProperty(DB_USERNAME);
	}
	public static String getDbPassword(){
		return properties.getProperty(DB_PASSWORD);
	}
	public static String getDbUrl(){
		return properties.getProperty(DB_URL);
	}
	public static String getDbPoolMaxSize(){
		return properties.getProperty(DB_POOL_MAX_SIZE, "10");
	}
	public static String getDbPoolMinSize(){
		return properties.getProperty(DB_POOL_MIN_SIZE, "2");
	}
	public static String getProp(String name){
		return properties.getProperty(name, "");
	}
	public static String getWebName(){
		return WEB_NAME;
	}
	public static String getHIVE2(){
		return properties.getProperty(HIVE2);
	}
	public static String getHIVE_METASTORE(){
		return properties.getProperty(HIVE_METASTORE);
	}
	
	public static String getHDFS_URI() {
		return properties.getProperty(HDFS_URI);
	}

	public static String getTitle(String name, String local){
		String title = null;
		if("en".equals(local))
			title = msn_en.getProperty(name);
		else
			title = msn_cn.getProperty(name);
		if(title==null)
			return name;
		else 
			return title;
	}
	
	static void setWebName(String webName){
		WEB_NAME =  webName;
	}
	
	
	public static void main(String[] args) {
		System.out.println(Conf.getProp("thriftserver.url"));
	}
}