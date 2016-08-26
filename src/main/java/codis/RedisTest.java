//package codis;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//
//public class RedisTest {
//
//	public static void main(String[] args) {
//		String filePath = "";//args[0];
////		System.out.println(filePath);
////		if(StringUtils.isBlank(filePath)){
////			try {
////				throw new Exception("没有传入路径参数，请传入参数后重试！！！");
////			} catch (Exception e) {
////				e.printStackTrace();
////			}
////		}
//		long startTime = System.currentTimeMillis();
//		ExecutorService exe = java.util.concurrent.Executors.newFixedThreadPool(10);
//		Configuration conf = new Configuration();
//
//		//add by wp
//		String userdir = System.getProperty("user.dir") + File.separator
//				+ "conf" + File.separator;
//
//		//System.out.println(f.getAbsolutePath());
//		conf.addResource(new Path(userdir + "yarn-site.xml"));
//
//		System.out.println(conf.get("yarn.resourcemanager.connect.retry-interval.ms"));
//
//		FileSystem fs = null;
//		//获取所有的codis集群的slave的ip和port信息，配置到了配置文件里
//		String[] codisHostsInfo = Conf.getProp("codisHostsInfo").split(",");
//		FSDataOutputStream outputStream = null;
//		try {
//			 fs = FileSystem.get(conf);
//			 for(int i=0;i<codisHostsInfo.length;i++){
//					String[] info = codisHostsInfo[i].split(":");
//					RedisExportDataToFile rf = new RedisExportDataToFile(info[0],
//							Integer.valueOf(info[1]).intValue(),startTime,0,50000,filePath,fs);
//					exe.submit(rf);
//			}
//			exe.shutdown();
//			while(!exe.isTerminated()){
//				Thread.sleep(1000);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}finally{
//			try {
//				fs.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//
//
//
//
//
//	}
//
//}
