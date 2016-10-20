package codis;

import com.asiainfo.codis.ExportData;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Paths;
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
	public final static String CODIS_EXPORT_MAX_ROW = "codis.export.maximum-row-per-thread";
	public final static int DEFAULT_CODIS_EXPORT_MAX_ROW = 1000;

    public final static String EXPORT_FILE_ENABLE = "export.source-file.enable";
    public final static boolean DEFAULT_EXPORT_FILE_ENABLE = false;

	public final static String OUTPUT_FILE_SEPARATOR = "output.file.separator";
	public final static String DEFAULT_OUTPUT_FILE_SEPARATOR = ",";

	public final static String CODIS_EXPORT_INTERVAL_S = "codis.export.interval-s";
	public final static long DEFAULT_CODIS_EXPORT_INTERVAL_S = 600L;

    public final static String HDFS_OUTPUT_SCHEMA = "hdfs.output.schema";

    public final static String HDFS_OUTPUT_COUNTS_PER_FILE = "hdfs.output.counts-per-file";
    public final static int DEFAULT_HDFS_OUTPUT_COUNTS_PER_FILE = 2000000;

    public final static String JEDIS_TIMEOUT_MS= "jedis.timeout-ms";
    public final static int DEFAULT_JEDIS_TIMEOUT_MS= 300000;

	public final static String CODIS_OUTPUT_BASE_PATH = "codis.output.base.path";

	private static Logger logger = Logger.getLogger(Conf.class);



//	public static void init(){
	static {
		try {
			InputStream inputStream = Conf.class.getClassLoader().getResourceAsStream(CONFIG_FILE);
			String confDir = Paths.get(ExportData.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent().getParent() + File.separator + "conf" + File.separator;
			//InputStream tablesInputStream = Conf.class.getClassLoader().getResourceAsStream(TABLES_CONFIG);

			//System.out.println(new File("conf" + File.separator + CONFIG_FILE).getAbsolutePath());
		
			if (inputStream == null){
			      //throw new RuntimeException(CONFIG_FILE + " not found in classpath");
			}else{
				  properties.load(new FileInputStream(confDir + CONFIG_FILE));
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

		int result;
		try
		{
			result = Integer.parseInt(valueString);
		}catch (NumberFormatException e){
			logger.error("Invalid value '" + valueString + "' of property '" + name + "'");
			return defaultValue;
		}

		return result;
	}

	public static long getLong(String name, long defaultValue) {
		String valueString = StringUtils.trim(properties.getProperty(name));
		if (StringUtils.isEmpty(valueString))
			return defaultValue;

		long result;
		try
		{
			result = Long.parseLong(valueString);
		}catch (NumberFormatException e){
			logger.error("Invalid value '" + valueString + "' of property '" + name + "'");
			return defaultValue;
		}

		return result;
	}


    public static boolean getBoolean(String name, boolean defaultValue) {
        String valueString = StringUtils.trim(properties.getProperty(name));
        if (null == valueString || valueString.isEmpty()) {
            return defaultValue;
        }

        if (StringUtils.equalsIgnoreCase("true", valueString))
            return true;
        else if (StringUtils.equalsIgnoreCase("false", valueString))
            return false;
        else return defaultValue;
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
    public static Properties getProp(){
        return properties;
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